import json
import boto3
import os
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')

# Environment variables
MODEL_BUCKET = os.environ['MODEL_BUCKET']
MODEL_KEY = os.environ.get('MODEL_KEY', 'models/match_lr/model.json')

class MatcherScoring:
    """Matcher scoring using trained ML model"""
    
    def __init__(self, model_bucket: str):
        self.model_bucket = model_bucket
        self.model_data = None
        self._load_model()
    
    def _load_model(self) -> None:
        """Load the trained model from S3"""
        try:
            response = s3_client.get_object(Bucket=self.model_bucket, Key=MODEL_KEY)
            self.model_data = json.loads(response['Body'].read().decode('utf-8'))
            logger.info(f"Model loaded successfully: {self.model_data['model_version']}")
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            raise
    
    def prepare_features(self, 
                        embedding_similarity: float,
                        industry_geo_score: float,
                        apollo_score: float,
                        rule_features: Dict,
                        company_features: Dict) -> np.ndarray:
        """Prepare features for scoring"""
        try:
            # Get feature order from model
            feature_order = self.model_data['feature_order']
            features = []
            
            # Create feature vector in the same order as training
            for feature_name in feature_order:
                if feature_name == 'embedding_similarity':
                    features.append(embedding_similarity)
                elif feature_name == 'ml_score':
                    # This will be computed by the model
                    features.append(0.0)  # Placeholder
                elif feature_name == 'industry_match':
                    features.append(rule_features.get('industry_match', 0.0))
                elif feature_name == 'geo_match':
                    features.append(rule_features.get('geo_match', 0.0))
                elif feature_name == 'name_similarity':
                    features.append(rule_features.get('name_similarity', 0.0))
                elif feature_name == 'employee_count':
                    features.append(company_features.get('employee_count', 0.0))
                elif feature_name == 'annual_revenue':
                    features.append(company_features.get('annual_revenue', 0.0))
                elif feature_name == 'total_funding':
                    features.append(company_features.get('total_funding', 0.0))
                elif feature_name == 'domain_health_score':
                    features.append(company_features.get('domain_health_score', 0.0))
                elif feature_name == 'content_richness_score':
                    features.append(company_features.get('content_richness_score', 0.0))
                elif feature_name.startswith('industry_'):
                    industry = feature_name.replace('industry_', '')
                    features.append(1.0 if company_features.get('industry') == industry else 0.0)
                elif feature_name.startswith('country_'):
                    country = feature_name.replace('country_', '')
                    features.append(1.0 if company_features.get('country') == country else 0.0)
                else:
                    features.append(0.0)
            
            return np.array(features)
            
        except Exception as e:
            logger.error(f"Error preparing features: {e}")
            raise
    
    def compute_ml_score(self, features: np.ndarray) -> float:
        """Compute ML prediction score using logistic regression"""
        try:
            # Get model coefficients and intercept
            coefficients = np.array(self.model_data['coefficients'])
            intercept = self.model_data['intercept']
            
            # Compute linear combination
            linear_combination = np.dot(features, coefficients) + intercept
            
            # Apply sigmoid function to get probability
            ml_score = 1 / (1 + np.exp(-linear_combination))
            
            return float(ml_score)
            
        except Exception as e:
            logger.error(f"Error computing ML score: {e}")
            return 0.0
    
    def compute_final_score(self, 
                           embedding_similarity: float,
                           industry_geo_score: float,
                           apollo_score: float,
                           ml_score: float) -> float:
        """Compute final score using weighted combination"""
        try:
            # Weighted combination as specified
            final_score = (
                0.6 * embedding_similarity +
                0.2 * industry_geo_score +
                0.1 * apollo_score +
                0.1 * ml_score
            )
            
            # Ensure score is between 0 and 1
            final_score = max(0.0, min(1.0, final_score))
            
            return final_score
            
        except Exception as e:
            logger.error(f"Error computing final score: {e}")
            return 0.0
    
    def score_match(self, 
                   embedding_similarity: float,
                   industry_geo_score: float,
                   apollo_score: float,
                   rule_features: Dict,
                   company_features: Dict) -> Dict:
        """Score a match using all components"""
        try:
            # Prepare features
            features = self.prepare_features(
                embedding_similarity,
                industry_geo_score,
                apollo_score,
                rule_features,
                company_features
            )
            
            # Compute ML score
            ml_score = self.compute_ml_score(features)
            
            # Compute final score
            final_score = self.compute_final_score(
                embedding_similarity,
                industry_geo_score,
                apollo_score,
                ml_score
            )
            
            return {
                'final_score': final_score,
                'embedding_similarity': embedding_similarity,
                'industry_geo_score': industry_geo_score,
                'apollo_score': apollo_score,
                'ml_score': ml_score,
                'score_breakdown': {
                    'embedding_weighted': 0.6 * embedding_similarity,
                    'industry_geo_weighted': 0.2 * industry_geo_score,
                    'apollo_weighted': 0.1 * apollo_score,
                    'ml_weighted': 0.1 * ml_score
                }
            }
            
        except Exception as e:
            logger.error(f"Error scoring match: {e}")
            return {
                'final_score': 0.0,
                'embedding_similarity': embedding_similarity,
                'industry_geo_score': industry_geo_score,
                'apollo_score': apollo_score,
                'ml_score': 0.0,
                'error': str(e)
            }

def score_match(embedding_similarity: float,
                industry_geo_score: float,
                apollo_score: float,
                rule_features: Dict,
                company_features: Dict) -> Dict:
    """
    Main function to score a match
    
    Args:
        embedding_similarity: Similarity score from embeddings (0-1)
        industry_geo_score: Combined industry and geographic match score (0-1)
        apollo_score: Apollo data quality score (0-1)
        rule_features: Dictionary with rule-based features
        company_features: Dictionary with company features
    
    Returns:
        Dictionary with scoring results
    """
    try:
        # Initialize scorer
        scorer = MatcherScoring(MODEL_BUCKET)
        
        # Score the match
        result = scorer.score_match(
            embedding_similarity,
            industry_geo_score,
            apollo_score,
            rule_features,
            company_features
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error in score_match function: {e}")
        return {
            'final_score': 0.0,
            'embedding_similarity': embedding_similarity,
            'industry_geo_score': industry_geo_score,
            'apollo_score': apollo_score,
            'ml_score': 0.0,
            'error': str(e)
        }

# Example usage
if __name__ == "__main__":
    # Example data
    example_rule_features = {
        'industry_match': 0.8,
        'geo_match': 0.6,
        'name_similarity': 0.7
    }
    
    example_company_features = {
        'employee_count': 50,
        'annual_revenue': 1000000,
        'total_funding': 5000000,
        'domain_health_score': 0.8,
        'content_richness_score': 0.7,
        'industry': 'technology',
        'country': 'US'
    }
    
    # Score a match
    result = score_match(
        embedding_similarity=0.85,
        industry_geo_score=0.7,
        apollo_score=0.9,
        rule_features=example_rule_features,
        company_features=example_company_features
    )
    
    print("Scoring Result:")
    print(json.dumps(result, indent=2))
