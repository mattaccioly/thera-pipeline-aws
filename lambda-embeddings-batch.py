import json
import boto3
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import hashlib

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
athena_client = boto3.client('athena')
bedrock_client = boto3.client('bedrock-runtime')
cloudwatch = boto3.client('cloudwatch')
ssm_client = boto3.client('ssm')

# Environment variables
CURATED_BUCKET = os.environ['CURATED_BUCKET']
ATHENA_DATABASE = os.environ['ATHENA_DATABASE']
ATHENA_WORKGROUP = os.environ.get('ATHENA_WORKGROUP', 'primary')
EMBEDDINGS_BUCKET = os.environ['EMBEDDINGS_BUCKET']
BEDROCK_MODEL_ID = os.environ.get('BEDROCK_MODEL_ID', 'amazon.titan-embed-text-v1')
MAX_BATCH_SIZE = int(os.environ.get('MAX_BATCH_SIZE', '25'))
MAX_DAILY_ITEMS = int(os.environ.get('MAX_DAILY_ITEMS', '10000'))
COST_PER_EMBEDDING = float(os.environ.get('COST_PER_EMBEDDING', '0.0001'))  # $0.0001 per embedding
DAILY_BUDGET = float(os.environ.get('DAILY_BUDGET', '100.0'))  # $100 daily budget
WATERMARK_PARAMETER = os.environ.get('WATERMARK_PARAMETER', '/thera/embeddings/watermark')

class BedrockEmbeddingsClient:
    """Bedrock Titan embeddings client with cost tracking"""
    
    def __init__(self, model_id: str = 'amazon.titan-embed-text-v1'):
        self.model_id = model_id
        self.daily_cost = 0.0
        self.daily_items_processed = 0
        self.start_of_day = datetime.utcnow().date()
    
    def reset_daily_counters(self):
        """Reset daily counters if it's a new day"""
        current_date = datetime.utcnow().date()
        if current_date > self.start_of_day:
            self.daily_cost = 0.0
            self.daily_items_processed = 0
            self.start_of_day = current_date
    
    def can_process_more(self) -> Tuple[bool, str]:
        """Check if we can process more items within budget limits"""
        self.reset_daily_counters()
        
        if self.daily_items_processed >= MAX_DAILY_ITEMS:
            return False, f"Daily item limit exceeded: {self.daily_items_processed}/{MAX_DAILY_ITEMS}"
        
        if self.daily_cost >= DAILY_BUDGET:
            return False, f"Daily budget exceeded: ${self.daily_cost:.2f}/${DAILY_BUDGET:.2f}"
        
        return True, "OK"
    
    def get_embeddings(self, texts: List[str]) -> List[Dict]:
        """Get embeddings for a batch of texts"""
        can_process, reason = self.can_process_more()
        if not can_process:
            raise Exception(f"Cannot process more items: {reason}")
        
        try:
            # Prepare request
            request_body = {
                "inputText": texts[0] if len(texts) == 1 else texts
            }
            
            # Call Bedrock
            response = bedrock_client.invoke_model(
                modelId=self.model_id,
                body=json.dumps(request_body)
            )
            
            # Parse response
            response_body = json.loads(response['body'].read())
            
            # Update counters
            items_processed = len(texts)
            cost = items_processed * COST_PER_EMBEDDING
            
            self.daily_items_processed += items_processed
            self.daily_cost += cost
            
            # Extract embeddings
            if 'embedding' in response_body:
                # Single embedding
                embeddings = [response_body['embedding']]
            elif 'embeddings' in response_body:
                # Multiple embeddings
                embeddings = response_body['embeddings']
            else:
                raise Exception("No embeddings found in response")
            
            # Create results
            results = []
            for i, embedding in enumerate(embeddings):
                results.append({
                    'text': texts[i],
                    'embedding': embedding,
                    'model_id': self.model_id,
                    'timestamp': datetime.utcnow().isoformat(),
                    'cost': COST_PER_EMBEDDING
                })
            
            logger.info(f"Generated {len(embeddings)} embeddings, cost: ${cost:.4f}")
            return results
            
        except Exception as e:
            logger.error(f"Error getting embeddings: {e}")
            raise

class EmbeddingsProcessor:
    """Process embeddings for GOLD layer data"""
    
    def __init__(self, embeddings_bucket: str, athena_database: str, athena_workgroup: str):
        self.embeddings_bucket = embeddings_bucket
        self.athena_database = athena_database
        self.athena_workgroup = athena_workgroup
        self.bedrock_client = BedrockEmbeddingsClient(BEDROCK_MODEL_ID)
    
    def get_watermark(self) -> str:
        """Get watermark timestamp from SSM Parameter Store"""
        try:
            response = ssm_client.get_parameter(Name=WATERMARK_PARAMETER)
            return response['Parameter']['Value']
        except ssm_client.exceptions.ParameterNotFound:
            # Return timestamp from 7 days ago if no watermark exists
            return (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logger.error(f"Error getting watermark: {e}")
            return (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    
    def update_watermark(self, timestamp: str) -> None:
        """Update watermark timestamp in SSM Parameter Store"""
        try:
            ssm_client.put_parameter(
                Name=WATERMARK_PARAMETER,
                Value=timestamp,
                Type='String',
                Overwrite=True
            )
            logger.info(f"Updated watermark to: {timestamp}")
        except Exception as e:
            logger.error(f"Error updating watermark: {e}")
    
    def get_gold_data_with_changed_profiles(self, watermark: str) -> List[Dict]:
        """Get GOLD data where profile_text_hash has changed since watermark"""
        try:
            query = f"""
            SELECT 
                company_key,
                profile_text,
                profile_text_hash,
                updated_at,
                company_name,
                industry,
                country
            FROM {self.athena_database}.gold_startup_profiles
            WHERE updated_at > timestamp '{watermark}'
            AND profile_text IS NOT NULL
            AND profile_text != ''
            AND profile_text_hash IS NOT NULL
            ORDER BY updated_at ASC
            LIMIT 1000
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
            
            results = []
            for row in response['ResultSet']['Rows'][1:]:  # Skip header
                if row['Data']:
                    data = {}
                    for i, field in enumerate(['company_key', 'profile_text', 'profile_text_hash', 
                                             'updated_at', 'company_name', 'industry', 'country']):
                        if i < len(row['Data']):
                            data[field] = row['Data'][i].get('VarCharValue', '')
                    
                    if data.get('company_key') and data.get('profile_text'):
                        results.append(data)
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting GOLD data: {e}")
            raise
    
    def process_embeddings_batch(self, data_batch: List[Dict]) -> List[Dict]:
        """Process a batch of data to generate embeddings"""
        try:
            # Extract texts for embedding
            texts = []
            for item in data_batch:
                # Combine profile text with metadata for richer embeddings
                combined_text = f"{item.get('profile_text', '')} | Industry: {item.get('industry', '')} | Country: {item.get('country', '')}"
                texts.append(combined_text)
            
            # Get embeddings from Bedrock
            embeddings_results = self.bedrock_client.get_embeddings(texts)
            
            # Combine with original data
            results = []
            for i, item in enumerate(data_batch):
                if i < len(embeddings_results):
                    embedding_result = embeddings_results[i]
                    
                    result = {
                        'company_key': item.get('company_key'),
                        'company_name': item.get('company_name'),
                        'industry': item.get('industry'),
                        'country': item.get('country'),
                        'profile_text_hash': item.get('profile_text_hash'),
                        'embedding_vector': embedding_result['embedding'],
                        'embedding_model': embedding_result['model_id'],
                        'embedding_timestamp': embedding_result['timestamp'],
                        'embedding_cost': embedding_result['cost'],
                        'updated_at': item.get('updated_at')
                    }
                    
                    results.append(result)
            
            return results
            
        except Exception as e:
            logger.error(f"Error processing embeddings batch: {e}")
            raise
    
    def store_embeddings_parquet(self, embeddings_data: List[Dict], partition_date: str) -> str:
        """Store embeddings as Parquet file in S3"""
        try:
            if not embeddings_data:
                return None
            
            # Create DataFrame
            df = pd.DataFrame(embeddings_data)
            
            # Convert to PyArrow Table
            table = pa.Table.from_pandas(df)
            
            # Write to Parquet in memory
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Upload to S3
            key = f"embeddings/date={partition_date}/embeddings_{int(time.time())}.parquet"
            
            s3_client.put_object(
                Bucket=self.embeddings_bucket,
                Key=key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            logger.info(f"Stored {len(embeddings_data)} embeddings to s3://{self.embeddings_bucket}/{key}")
            return key
            
        except Exception as e:
            logger.error(f"Error storing embeddings: {e}")
            raise

def emit_cloudwatch_metrics(items_processed: int, embeddings_generated: int, total_cost: float) -> None:
    """Emit CloudWatch metrics"""
    try:
        cloudwatch.put_metric_data(
            Namespace='TheraPipeline/Embeddings',
            MetricData=[
                {
                    'MetricName': 'ItemsProcessed',
                    'Value': items_processed,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'EmbeddingsGenerated',
                    'Value': embeddings_generated,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'TotalCost',
                    'Value': total_cost,
                    'Unit': 'None',
                    'Timestamp': datetime.utcnow()
                }
            ]
        )
    except Exception as e:
        logger.error(f"Error emitting metrics: {e}")

def lambda_handler(event, context):
    """Main Lambda handler for Embeddings Batch processing"""
    try:
        # Initialize processor
        processor = EmbeddingsProcessor(EMBEDDINGS_BUCKET, ATHENA_DATABASE, ATHENA_WORKGROUP)
        
        # Get watermark
        watermark = processor.get_watermark()
        logger.info(f"Processing data since watermark: {watermark}")
        
        # Get GOLD data with changed profiles
        gold_data = processor.get_gold_data_with_changed_profiles(watermark)
        
        if not gold_data:
            logger.info("No data to process")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No data to process',
                    'items_processed': 0,
                    'embeddings_generated': 0,
                    'total_cost': 0.0
                })
            }
        
        logger.info(f"Processing {len(gold_data)} items")
        
        # Process in batches
        all_embeddings = []
        total_cost = 0.0
        latest_timestamp = watermark
        
        for i in range(0, len(gold_data), MAX_BATCH_SIZE):
            batch = gold_data[i:i + MAX_BATCH_SIZE]
            
            try:
                # Check if we can process more
                can_process, reason = processor.bedrock_client.can_process_more()
                if not can_process:
                    logger.warning(f"Stopping processing: {reason}")
                    break
                
                # Process batch
                embeddings_batch = processor.process_embeddings_batch(batch)
                all_embeddings.extend(embeddings_batch)
                
                # Update cost tracking
                batch_cost = sum(item.get('embedding_cost', 0) for item in embeddings_batch)
                total_cost += batch_cost
                
                # Update latest timestamp
                for item in batch:
                    if item.get('updated_at'):
                        if item['updated_at'] > latest_timestamp:
                            latest_timestamp = item['updated_at']
                
                logger.info(f"Processed batch {i//MAX_BATCH_SIZE + 1}, items: {len(batch)}, cost: ${batch_cost:.4f}")
                
                # Small delay to avoid overwhelming Bedrock
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error processing batch {i//MAX_BATCH_SIZE + 1}: {e}")
                continue
        
        # Store embeddings if any were generated
        if all_embeddings:
            current_date = datetime.utcnow().strftime('%Y-%m-%d')
            parquet_key = processor.store_embeddings_parquet(all_embeddings, current_date)
            
            # Update watermark
            processor.update_watermark(latest_timestamp)
            
            logger.info(f"Generated {len(all_embeddings)} embeddings, total cost: ${total_cost:.4f}")
        else:
            logger.warning("No embeddings were generated")
            parquet_key = None
        
        # Emit metrics
        emit_cloudwatch_metrics(len(gold_data), len(all_embeddings), total_cost)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Embeddings batch processing completed successfully',
                'items_processed': len(gold_data),
                'embeddings_generated': len(all_embeddings),
                'total_cost': total_cost,
                'parquet_key': parquet_key,
                'watermark_updated': latest_timestamp
            })
        }
        
    except Exception as e:
        logger.error(f"Error in embeddings batch processing: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

# For local testing
if __name__ == "__main__":
    # Test with sample data
    test_event = {}
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))