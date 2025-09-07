import json
import boto3
import hashlib
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
bedrock_runtime = boto3.client('bedrock-runtime', region_name='us-east-2')
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
s3_client = boto3.client('s3')
athena_client = boto3.client('athena')

# Environment variables
CURATED_BUCKET = 'thera-curated-805595753342-v3'
ATHENA_DATABASE = 'thera_gold'
ATHENA_WORKGROUP = 'primary'
DYNAMODB_TABLE = 'dev-advanced-summaries'
DAILY_BUDGET_USD = 50.0
MAX_BATCH_SIZE = 10
MAX_DAILY_COMPANIES = 1000

class BedrockLLMClient:
    """Cost-efficient Bedrock LLM client with intelligent model selection."""
    
    def __init__(self):
        self.total_cost = 0.0
        self.total_calls = 0
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        
        # Model pricing (per 1M tokens)
        self.pricing = {
            'claude-3-haiku': {'input': 0.25, 'output': 1.25},
            'claude-3-sonnet': {'input': 3.00, 'output': 15.00}
        }
    
    def _count_tokens(self, text: str) -> int:
        """Rough token count estimation."""
        return len(text.split()) * 1.3  # Approximate multiplier
    
    def _calculate_cost(self, model_id: str, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost for a model call."""
        pricing = self.pricing.get(model_id, self.pricing['claude-3-haiku'])
        input_cost = (input_tokens / 1_000_000) * pricing['input']
        output_cost = (output_tokens / 1_000_000) * pricing['output']
        return input_cost + output_cost
    
    def invoke_model(self, prompt: str, model_id: str, max_tokens: int = 2048, temperature: float = 0.1) -> Dict:
        """Invoke a Bedrock LLM with the given prompt."""
        try:
            body = json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": max_tokens,
                "temperature": temperature,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            })
            
            response = bedrock_runtime.invoke_model(
                modelId=model_id,
                contentType="application/json",
                accept="application/json",
                body=body
            )
            
            response_body = json.loads(response['body'].read())
            
            input_tokens = self._count_tokens(prompt)
            output_tokens = self._count_tokens(response_body['content'][0]['text'])
            cost = self._calculate_cost(model_id, input_tokens, output_tokens)
            
            self.total_cost += cost
            self.total_input_tokens += input_tokens
            self.total_output_tokens += output_tokens
            self.total_calls += 1
            
            return {
                "summary": response_body['content'][0]['text'],
                "model_id": model_id,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cost": cost
            }
        except Exception as e:
            logger.error(f"Error invoking Bedrock model {model_id}: {e}")
            raise

class AdvancedSummarizationProcessor:
    """Processes startup profiles and generates advanced summaries."""
    
    def __init__(self):
        self.llm_client = BedrockLLMClient()
        self.cache_table = dynamodb.Table(DYNAMODB_TABLE)
    
    def get_cache_key(self, company_key: str, content_hash: str, summary_type: str) -> str:
        """Generate cache key for summary."""
        return f"{company_key}:{summary_type}:{content_hash}"
    
    def get_cached_summary(self, cache_key: str) -> Optional[Dict]:
        """Retrieve cached summary if available."""
        try:
            response = self.cache_table.get_item(Key={'cache_key': cache_key})
            if 'Item' in response:
                item = response['Item']
                # Check if cache is still valid (24 hours)
                if time.time() - item.get('created_at', 0) < 86400:
                    return item
        except Exception as e:
            logger.warning(f"Error retrieving cache: {e}")
        return None
    
    def cache_summary(self, cache_key: str, summary: Dict, summary_type: str, company_key: str):
        """Cache the generated summary."""
        try:
            item = {
                'cache_key': cache_key,
                'company_key': company_key,
                'summary_type': summary_type,
                'summary': summary,
                'created_at': int(time.time()),
                'expires_at': int(time.time()) + 86400  # 24 hours
            }
            self.cache_table.put_item(Item=item)
        except Exception as e:
            logger.warning(f"Error caching summary: {e}")
    
    def generate_executive_summary(self, profile_data: Dict) -> Dict:
        """Generate executive summary using Claude 3 Haiku."""
        prompt = f"""
        Generate a concise 2-3 sentence executive summary for this startup:
        
        Company: {profile_data.get('company_name', 'Unknown')}
        Domain: {profile_data.get('domain', 'Unknown')}
        Industry: {profile_data.get('industry', 'Unknown')}
        Description: {profile_data.get('description', 'No description available')}
        Stage: {profile_data.get('startup_stage', 'Unknown')}
        Employee Count: {profile_data.get('employee_count', 'Unknown')}
        
        Focus on: What the company does, its market position, and key value proposition.
        Keep it professional and business-focused.
        """
        
        cache_key = self.get_cache_key(
            profile_data.get('company_key', ''),
            profile_data.get('content_hash', ''),
            'executive_summary'
        )
        
        cached = self.get_cached_summary(cache_key)
        if cached:
            return {
                "summary": cached['summary'],
                "model_id": "cached",
                "cost": 0.0,
                "cached": True
            }
        
        result = self.llm_client.invoke_model(
            prompt=prompt,
            model_id="us.anthropic.claude-3-haiku-20240307-v1:0",
            max_tokens=150,
            temperature=0.1
        )
        
        # Cache the result
        self.cache_summary(cache_key, result['summary'], 'executive_summary', profile_data.get('company_key', ''))
        
        return {
            "summary": result['summary'],
            "model_id": result['model_id'],
            "cost": result['cost'],
            "cached": False
        }
    
    def generate_key_insights(self, profile_data: Dict) -> Dict:
        """Generate key business insights using Claude 3 Haiku."""
        prompt = f"""
        Extract 5-7 key business insights for this startup:
        
        Company: {profile_data.get('company_name', 'Unknown')}
        Domain: {profile_data.get('domain', 'Unknown')}
        Industry: {profile_data.get('industry', 'Unknown')}
        Description: {profile_data.get('description', 'No description available')}
        Stage: {profile_data.get('startup_stage', 'Unknown')}
        Employee Count: {profile_data.get('employee_count', 'Unknown')}
        Funding: {profile_data.get('total_funding', 'Unknown')}
        
        Focus on: Business model, market opportunity, competitive advantages, growth potential, and strategic insights.
        Return as a JSON array of strings.
        """
        
        cache_key = self.get_cache_key(
            profile_data.get('company_key', ''),
            profile_data.get('content_hash', ''),
            'key_insights'
        )
        
        cached = self.get_cached_summary(cache_key)
        if cached:
            return {
                "insights": cached['summary'],
                "model_id": "cached",
                "cost": 0.0,
                "cached": True
            }
        
        result = self.llm_client.invoke_model(
            prompt=prompt,
            model_id="us.anthropic.claude-3-haiku-20240307-v1:0",
            max_tokens=300,
            temperature=0.1
        )
        
        # Parse JSON response
        try:
            insights = json.loads(result['summary'])
        except:
            insights = [result['summary']]
        
        # Cache the result
        self.cache_summary(cache_key, insights, 'key_insights', profile_data.get('company_key', ''))
        
        return {
            "insights": insights,
            "model_id": result['model_id'],
            "cost": result['cost'],
            "cached": False
        }
    
    def process_company(self, company_data: Dict) -> Dict:
        """Process a single company and generate all summaries."""
        try:
            logger.info(f"Processing company: {company_data.get('company_name', 'Unknown')}")
            
            # Generate content hash for change detection
            content = f"{company_data.get('description', '')}{company_data.get('web_content', '')}"
            content_hash = hashlib.md5(content.encode()).hexdigest()
            company_data['content_hash'] = content_hash
            
            # Generate summaries
            executive_summary = self.generate_executive_summary(company_data)
            key_insights = self.generate_key_insights(company_data)
            
            return {
                "company_key": company_data.get('company_key', ''),
                "company_name": company_data.get('company_name', ''),
                "domain": company_data.get('domain', ''),
                "executive_summary": executive_summary['summary'],
                "key_insights": key_insights['insights'],
                "processing_cost": executive_summary['cost'] + key_insights['cost'],
                "models_used": [executive_summary['model_id'], key_insights['model_id']],
                "cached_items": sum([executive_summary.get('cached', False), key_insights.get('cached', False)]),
                "content_hash": content_hash,
                "processed_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error processing company {company_data.get('company_name', 'Unknown')}: {e}")
            return {
                "company_key": company_data.get('company_key', ''),
                "error": str(e),
                "processed_at": datetime.utcnow().isoformat()
            }

def lambda_handler(event, context):
    """Main Lambda handler for advanced summarization."""
    try:
        logger.info("Starting advanced summarization processing")
        
        # Initialize processor
        processor = AdvancedSummarizationProcessor()
        
        # Sample data for testing
        sample_companies = [
            {
                "company_key": "test-001",
                "company_name": "Test Startup Inc",
                "domain": "teststartup.com",
                "industry": "Technology",
                "description": "A revolutionary AI-powered platform for data analysis",
                "startup_stage": "Series A",
                "employee_count": 50,
                "total_funding": 10000000
            }
        ]
        
        results = []
        total_cost = 0.0
        
        # Process companies
        for company in sample_companies:
            result = processor.process_company(company)
            results.append(result)
            total_cost += result.get('processing_cost', 0.0)
        
        # Log cost information
        logger.info(f"Total processing cost: ${total_cost:.4f}")
        logger.info(f"Total LLM calls: {processor.llm_client.total_calls}")
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Advanced summarization completed successfully",
                "companies_processed": len(results),
                "total_cost": total_cost,
                "results": results,
                "llm_stats": {
                    "total_calls": processor.llm_client.total_calls,
                    "total_input_tokens": processor.llm_client.total_input_tokens,
                    "total_output_tokens": processor.llm_client.total_output_tokens,
                    "total_cost": processor.llm_client.total_cost
                }
            })
        }
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e),
                "message": "Advanced summarization failed"
            })
        }
