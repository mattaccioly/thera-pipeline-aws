import json
import boto3
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import math

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
athena_client = boto3.client('athena')
bedrock_client = boto3.client('bedrock-runtime')
s3_client = boto3.client('s3')
cloudwatch = boto3.client('cloudwatch')

# Environment variables
ATHENA_DATABASE = os.environ['ATHENA_DATABASE']
ATHENA_WORKGROUP = os.environ.get('ATHENA_WORKGROUP', 'primary')
EMBEDDINGS_BUCKET = os.environ['EMBEDDINGS_BUCKET']
BEDROCK_MODEL_ID = os.environ.get('BEDROCK_MODEL_ID', 'amazon.titan-embed-text-v1')
MAX_CANDIDATES = int(os.environ.get('MAX_CANDIDATES', '5000'))
TOP_RESULTS = int(os.environ.get('TOP_RESULTS', '20'))
MODEL_BUCKET = os.environ.get('MODEL_BUCKET', 'thera-models')
MODEL_KEY = os.environ.get('MODEL_KEY', 'match_lr/model.json')

class EmbeddingsMatcher:
    """Similarity matching using embeddings and ML model"""
    
    def __init__(self, athena_database: str, athena_workgroup: str, embeddings_bucket: str):
        self.athena_database = athena_database
        self.athena_workgroup = athena_workgroup
        self.embeddings_bucket = embeddings_bucket
        self.ml_model = None
        self.model_loaded = False
    
    def get_embedding(self, text: str) -> List[float]:
        """Get embedding for input text using Bedrock"""
        try:
            request_body = {
                "inputText": text
            }
            
            response = bedrock_client.invoke_model(
                modelId=BEDROCK_MODEL_ID,
                body=json.dumps(request_body)
            )
            
            response_body = json.loads(response['body'].read())
            
            if 'embedding' in response_body:
                return response_body['embedding']
            else:
                raise Exception("No embedding found in response")
                
        except Exception as e:
            logger.error(f"Error getting embedding: {e}")
            raise
    
    def load_ml_model(self) -> Dict:
        """Load ML model from S3"""
        if self.model_loaded and self.ml_model:
            return self.ml_model
        
        try:
            response = s3_client.get_object(Bucket=MODEL_BUCKET, Key=MODEL_KEY)
            model_data = json.loads(response['Body'].read().decode('utf-8'))
            
            self.ml_model = model_data
            self.model_loaded = True
            
            logger.info("ML model loaded successfully")
            return model_data
            
        except Exception as e:
            logger.warning(f"Could not load ML model: {e}")
            # Return default model if loading fails
            return {
                'coefficients': [0.5, 0.3, 0.2],  # [embedding_sim, industry_match, geo_match]
                'intercept': 0.0,
                'feature_order': ['embedding_similarity', 'industry_match', 'geo_match']
            }
    
    def get_candidates_from_athena(self, industry: str = None, country: str = None, 
                                 limit: int = MAX_CANDIDATES) -> List[Dict]:
        """Get candidate companies from Athena with optional filters"""
        try:
            # Build WHERE clause
            where_conditions = []
            if industry:
                where_conditions.append(f"industry = '{industry}'")
            if country:
                where_conditions.append(f"country = '{country}'")
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            query = f"""
            SELECT 
                company_key,
                company_name,
                industry,
                country,
                embedding_vector,
                updated_at
            FROM {self.athena_database}.embeddings
            WHERE {where_clause}
            AND embedding_vector IS NOT NULL
            ORDER BY updated_at DESC
            LIMIT {limit}
            """
            
            # Execute query
            response = athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.athena_database},
                WorkGroup=self.athena_workgroup
            )
            
            query_execution_id = response['QueryExecutionId']
            
            # Wait for completion
            while True:
                response = athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                
                status = response['QueryExecution']['Status']['State']
                
                if status in ['SUCCEEDED']:
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    raise Exception(f"Query failed: {response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')}")
                
                time.sleep(2)
            
            # Get results
            response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
            
            candidates = []
            for row in response['ResultSet']['Rows'][1:]:  # Skip header
                if row['Data']:
                    data = {}
                    for i, field in enumerate(['company_key', 'company_name', 'industry', 
                                             'country', 'embedding_vector', 'updated_at']):
                        if i < len(row['Data']):
                            value = row['Data'][i].get('VarCharValue', '')
                            if field == 'embedding_vector':
                                # Parse embedding vector from string
                                try:
                                    data[field] = json.loads(value) if value else []
                                except:
                                    data[field] = []
                            else:
                                data[field] = value
                    
                    if data.get('company_key') and data.get('embedding_vector'):
                        candidates.append(data)
            
            return candidates
            
        except Exception as e:
            logger.error(f"Error getting candidates from Athena: {e}")
            raise
    
    def calculate_cosine_similarity(self, embedding1: List[float], embedding2: List[float]) -> float:
        """Calculate cosine similarity between two embeddings"""
        try:
            if not embedding1 or not embedding2:
                return 0.0
            
            # Convert to numpy arrays
            vec1 = np.array(embedding1).reshape(1, -1)
            vec2 = np.array(embedding2).reshape(1, -1)
            
            # Calculate cosine similarity
            similarity = cosine_similarity(vec1, vec2)[0][0]
            
            # Handle NaN values
            if math.isnan(similarity):
                return 0.0
            
            return float(similarity)
            
        except Exception as e:
            logger.error(f"Error calculating cosine similarity: {e}")
            return 0.0
    
    def calculate_rule_features(self, challenge_text: str, candidate: Dict) -> Dict:
        """Calculate rule-based features for ML model"""
        try:
            features = {}
            
            # Industry match (binary)
            challenge_industry = self._extract_industry_from_text(challenge_text)
            candidate_industry = candidate.get('industry', '').lower()
            features['industry_match'] = 1.0 if challenge_industry and challenge_industry in candidate_industry else 0.0
            
            # Geographic match (binary)
            challenge_country = self._extract_country_from_text(challenge_text)
            candidate_country = candidate.get('country', '').lower()
            features['geo_match'] = 1.0 if challenge_country and challenge_country in candidate_country else 0.0
            
            # Company name similarity (simple string matching)
            challenge_name = self._extract_company_name_from_text(challenge_text)
            candidate_name = candidate.get('company_name', '').lower()
            if challenge_name and candidate_name:
                # Simple Jaccard similarity on words
                challenge_words = set(challenge_name.split())
                candidate_words = set(candidate_name.split())
                if challenge_words and candidate_words:
                    intersection = challenge_words.intersection(candidate_words)
                    union = challenge_words.union(candidate_words)
                    features['name_similarity'] = len(intersection) / len(union) if union else 0.0
                else:
                    features['name_similarity'] = 0.0
            else:
                features['name_similarity'] = 0.0
            
            return features
            
        except Exception as e:
            logger.error(f"Error calculating rule features: {e}")
            return {
                'industry_match': 0.0,
                'geo_match': 0.0,
                'name_similarity': 0.0
            }
    
    def _extract_industry_from_text(self, text: str) -> str:
        """Extract industry from challenge text (simple keyword matching)"""
        text_lower = text.lower()
        
        industry_keywords = {
            'technology': ['tech', 'software', 'ai', 'artificial intelligence', 'machine learning', 'data', 'cloud', 'saas'],
            'healthcare': ['health', 'medical', 'pharma', 'biotech', 'healthcare', 'medicine'],
            'finance': ['fintech', 'finance', 'banking', 'payments', 'crypto', 'blockchain'],
            'ecommerce': ['ecommerce', 'retail', 'marketplace', 'shopping', 'online store'],
            'education': ['education', 'edtech', 'learning', 'school', 'university'],
            'energy': ['energy', 'renewable', 'solar', 'wind', 'clean energy'],
            'transportation': ['transport', 'mobility', 'logistics', 'delivery', 'autonomous'],
            'real_estate': ['real estate', 'property', 'housing', 'construction']
        }
        
        for industry, keywords in industry_keywords.items():
            if any(keyword in text_lower for keyword in keywords):
                return industry
        
        return ''
    
    def _extract_country_from_text(self, text: str) -> str:
        """Extract country from challenge text (simple keyword matching)"""
        text_lower = text.lower()
        
        country_keywords = {
            'united states': ['usa', 'us', 'united states', 'america'],
            'canada': ['canada', 'canadian'],
            'united kingdom': ['uk', 'britain', 'england', 'united kingdom'],
            'germany': ['germany', 'german'],
            'france': ['france', 'french'],
            'australia': ['australia', 'australian'],
            'singapore': ['singapore', 'singaporean'],
            'india': ['india', 'indian'],
            'china': ['china', 'chinese'],
            'japan': ['japan', 'japanese']
        }
        
        for country, keywords in country_keywords.items():
            if any(keyword in text_lower for keyword in keywords):
                return country
        
        return ''
    
    def _extract_company_name_from_text(self, text: str) -> str:
        """Extract potential company name from challenge text"""
        # Simple heuristic: look for capitalized words that might be company names
        words = text.split()
        capitalized_words = [word for word in words if word[0].isupper() and len(word) > 2]
        
        # Return first few capitalized words as potential company name
        return ' '.join(capitalized_words[:3]).lower()
    
    def calculate_ml_score(self, features: Dict) -> float:
        """Calculate ML score using logistic regression model"""
        try:
            model = self.load_ml_model()
            
            # Get feature values in correct order
            feature_values = []
            for feature_name in model['feature_order']:
                if feature_name in features:
                    feature_values.append(features[feature_name])
                else:
                    feature_values.append(0.0)
            
            # Calculate dot product + intercept
            score = sum(coef * val for coef, val in zip(model['coefficients'], feature_values))
            score += model['intercept']
            
            # Apply sigmoid function
            score = 1 / (1 + math.exp(-score))
            
            return score
            
        except Exception as e:
            logger.error(f"Error calculating ML score: {e}")
            return 0.0
    
    def find_matches(self, challenge_text: str, industry: str = None, country: str = None) -> List[Dict]:
        """Find top matches for challenge text"""
        try:
            # Get embedding for challenge text
            challenge_embedding = self.get_embedding(challenge_text)
            
            # Get candidates from Athena
            candidates = self.get_candidates_from_athena(industry, country, MAX_CANDIDATES)
            
            if not candidates:
                return []
            
            logger.info(f"Processing {len(candidates)} candidates")
            
            # Calculate similarities and scores
            results = []
            for candidate in candidates:
                try:
                    # Calculate cosine similarity
                    embedding_sim = self.calculate_cosine_similarity(
                        challenge_embedding, 
                        candidate.get('embedding_vector', [])
                    )
                    
                    # Calculate rule features
                    rule_features = self.calculate_rule_features(challenge_text, candidate)
                    
                    # Calculate ML score
                    ml_score = self.calculate_ml_score({
                        'embedding_similarity': embedding_sim,
                        'industry_match': rule_features['industry_match'],
                        'geo_match': rule_features['geo_match']
                    })
                    
                    # Calculate final score (weighted combination)
                    final_score = (0.7 * embedding_sim) + (0.3 * ml_score)
                    
                    result = {
                        'company_key': candidate.get('company_key'),
                        'company_name': candidate.get('company_name'),
                        'industry': candidate.get('industry'),
                        'country': candidate.get('country'),
                        'embedding_similarity': embedding_sim,
                        'ml_score': ml_score,
                        'final_score': final_score,
                        'rule_features': rule_features,
                        'reason': self._generate_reason(embedding_sim, rule_features, ml_score)
                    }
                    
                    results.append(result)
                    
                except Exception as e:
                    logger.error(f"Error processing candidate {candidate.get('company_key')}: {e}")
                    continue
            
            # Sort by final score and return top results
            results.sort(key=lambda x: x['final_score'], reverse=True)
            return results[:TOP_RESULTS]
            
        except Exception as e:
            logger.error(f"Error finding matches: {e}")
            raise
    
    def _generate_reason(self, embedding_sim: float, rule_features: Dict, ml_score: float) -> str:
        """Generate human-readable reason for the match"""
        reasons = []
        
        if embedding_sim > 0.8:
            reasons.append("Very high content similarity")
        elif embedding_sim > 0.6:
            reasons.append("High content similarity")
        elif embedding_sim > 0.4:
            reasons.append("Moderate content similarity")
        
        if rule_features['industry_match'] > 0:
            reasons.append("Industry match")
        
        if rule_features['geo_match'] > 0:
            reasons.append("Geographic match")
        
        if rule_features['name_similarity'] > 0.5:
            reasons.append("Company name similarity")
        
        if ml_score > 0.7:
            reasons.append("Strong ML prediction")
        elif ml_score > 0.5:
            reasons.append("Moderate ML prediction")
        
        return "; ".join(reasons) if reasons else "General similarity"

def emit_cloudwatch_metrics(candidates_processed: int, matches_found: int, avg_similarity: float) -> None:
    """Emit CloudWatch metrics"""
    try:
        cloudwatch.put_metric_data(
            Namespace='TheraPipeline/Matcher',
            MetricData=[
                {
                    'MetricName': 'CandidatesProcessed',
                    'Value': candidates_processed,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'MatchesFound',
                    'Value': matches_found,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'AverageSimilarity',
                    'Value': avg_similarity,
                    'Unit': 'None',
                    'Timestamp': datetime.utcnow()
                }
            ]
        )
    except Exception as e:
        logger.error(f"Error emitting metrics: {e}")

def lambda_handler(event, context):
    """Main Lambda handler for Matcher"""
    try:
        # Extract challenge text from event
        challenge_text = event.get('challenge_text', '')
        if not challenge_text:
            raise ValueError("challenge_text is required")
        
        # Extract optional filters
        industry = event.get('industry')
        country = event.get('country')
        
        logger.info(f"Finding matches for challenge: {challenge_text[:100]}...")
        
        # Initialize matcher
        matcher = EmbeddingsMatcher(ATHENA_DATABASE, ATHENA_WORKGROUP, EMBEDDINGS_BUCKET)
        
        # Find matches
        matches = matcher.find_matches(challenge_text, industry, country)
        
        # Calculate metrics
        avg_similarity = sum(match['final_score'] for match in matches) / len(matches) if matches else 0.0
        
        # Emit metrics
        emit_cloudwatch_metrics(len(matches), len(matches), avg_similarity)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Matching completed successfully',
                'challenge_text': challenge_text,
                'matches_found': len(matches),
                'average_similarity': avg_similarity,
                'matches': matches
            })
        }
        
    except Exception as e:
        logger.error(f"Error in matcher: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

# For local testing
if __name__ == "__main__":
    # Test with sample challenge
    test_event = {
        'challenge_text': 'Looking for AI startups in healthcare that use machine learning for drug discovery',
        'industry': 'healthcare',
        'country': 'united states'
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))
