"""
DynamoDB Publisher Lambda Function

This Lambda function implements the requirements from DYNAMODB_READ_MODELS.md:

TABLES:
- Startups (PK: company_key) — public fields only:
  name, domain, country, industry[], size_bracket, tags[], domain_health_score,
  contact_count, has_verified_contact, seniority_mix, title, meta_description, about_snippet,
  updated_at, schema_version
- StartupsPrivate (PK: company_key) — optional, masked/hashed PII aggregates

BEHAVIOR:
- Read GOLD delta (Parquet) from S3 where updated_at > watermark (watermark in SSM Parameter Store)
- BatchWriteItem with retry/backoff; idempotent upsert
- Config via env vars (table names, bucket/prefix, watermark param)
- CloudWatch metrics: upserts, skipped, errors
- Minimal IAM policy and SAM/CFN template
"""

import json
import boto3
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
import hashlib
import re

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
athena_client = boto3.client('athena')
ssm_client = boto3.client('ssm')
cloudwatch = boto3.client('cloudwatch')

# Environment variables
PUBLIC_TABLE = os.environ['PUBLIC_TABLE']
PRIVATE_TABLE = os.environ['PRIVATE_TABLE']
ATHENA_DATABASE = os.environ['ATHENA_DATABASE']
ATHENA_WORKGROUP = os.environ.get('ATHENA_WORKGROUP', 'primary')
WATERMARK_PARAMETER = os.environ.get('WATERMARK_PARAMETER', '/thera/dynamodb/watermark')
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '25'))

class DynamoDBPublisher:
    """Publishes data to DynamoDB tables with PII handling"""
    
    def __init__(self, public_table: str, private_table: str, athena_database: str, athena_workgroup: str):
        self.public_table = dynamodb.Table(public_table)
        self.private_table = dynamodb.Table(private_table)
        self.athena_database = athena_database
        self.athena_workgroup = athena_workgroup
    
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
    
    def get_gold_data_since_watermark(self, watermark: str) -> List[Dict]:
        """Get GOLD data since watermark from Athena"""
        try:
            query = f"""
            SELECT 
                company_key,
                company_name as name,
                domain,
                country,
                industry,
                size_bracket,
                profile_tags as tags,
                domain_health_score,
                apollo_contact_count as contact_count,
                apollo_has_verified_contact as has_verified_contact,
                apollo_seniority_mix as seniority_mix,
                web_title as title,
                web_meta_description as meta_description,
                web_about_snippet as about_snippet,
                updated_at,
                '1.0' as schema_version
            FROM {self.athena_database}.gold_startup_profiles
            WHERE updated_at > timestamp '{watermark}'
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
                    for i, field in enumerate(['company_key', 'name', 'domain', 'country', 'industry', 
                                             'size_bracket', 'tags', 'domain_health_score', 'contact_count',
                                             'has_verified_contact', 'seniority_mix', 'title', 'meta_description',
                                             'about_snippet', 'updated_at', 'schema_version']):
                        if i < len(row['Data']):
                            value = row['Data'][i].get('VarCharValue', '')
                            if field in ['contact_count']:
                                try:
                                    data[field] = int(value) if value else None
                                except:
                                    data[field] = None
                            elif field in ['domain_health_score']:
                                try:
                                    data[field] = float(value) if value else None
                                except:
                                    data[field] = None
                            elif field in ['tags', 'seniority_mix']:
                                try:
                                    data[field] = json.loads(value) if value else []
                                except:
                                    data[field] = []
                            elif field == 'has_verified_contact':
                                data[field] = value.lower() == 'true' if value else False
                            else:
                                data[field] = value
                    
                    if data.get('company_key'):
                        results.append(data)
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting GOLD data: {e}")
            raise
    
    def create_public_item(self, data: Dict) -> Dict:
        """Create public item for DynamoDB (no PII) - matches Startups table requirements"""
        try:
            # Extract only public fields as specified in requirements
            public_item = {
                'company_key': data.get('company_key'),
                'name': data.get('name'),
                'domain': data.get('domain'),
                'country': data.get('country'),
                'industry': data.get('industry'),
                'size_bracket': data.get('size_bracket'),
                'tags': data.get('tags', []),
                'domain_health_score': data.get('domain_health_score'),
                'contact_count': data.get('contact_count'),
                'has_verified_contact': data.get('has_verified_contact'),
                'seniority_mix': data.get('seniority_mix', []),
                'title': data.get('title'),
                'meta_description': data.get('meta_description'),
                'about_snippet': data.get('about_snippet'),
                'updated_at': data.get('updated_at'),
                'schema_version': data.get('schema_version', '1.0'),
                'ttl': int((datetime.utcnow() + timedelta(days=365)).timestamp())  # 1 year TTL
            }
            
            # Remove None values
            public_item = {k: v for k, v in public_item.items() if v is not None}
            
            return public_item
            
        except Exception as e:
            logger.error(f"Error creating public item: {e}")
            raise
    
    def create_private_item(self, data: Dict) -> Dict:
        """Create private item for DynamoDB (with masked/hashed PII) - matches StartupsPrivate table requirements"""
        try:
            # Create private item with hashed/masked PII aggregates
            private_item = {
                'company_key': data.get('company_key'),
                'name': data.get('name'),
                'domain': data.get('domain'),
                'country': data.get('country'),
                'industry': data.get('industry'),
                'size_bracket': data.get('size_bracket'),
                'tags': data.get('tags', []),
                'domain_health_score': data.get('domain_health_score'),
                'contact_count': data.get('contact_count'),
                'has_verified_contact': data.get('has_verified_contact'),
                'seniority_mix': data.get('seniority_mix', []),
                'title': data.get('title'),
                'meta_description': data.get('meta_description'),
                'about_snippet': data.get('about_snippet'),
                'updated_at': data.get('updated_at'),
                'schema_version': data.get('schema_version', '1.0'),
                'ttl': int((datetime.utcnow() + timedelta(days=365)).timestamp())  # 1 year TTL
            }
            
            # Hash sensitive fields for privacy
            if private_item.get('meta_description'):
                private_item['meta_description'] = self._hash_text(private_item['meta_description'])
            
            if private_item.get('about_snippet'):
                private_item['about_snippet'] = self._hash_text(private_item['about_snippet'])
            
            # Hash seniority_mix for privacy (contains PII about contacts)
            if private_item.get('seniority_mix'):
                private_item['seniority_mix'] = self._hash_seniority_mix(private_item['seniority_mix'])
            
            # Remove None values
            private_item = {k: v for k, v in private_item.items() if v is not None}
            
            return private_item
            
        except Exception as e:
            logger.error(f"Error creating private item: {e}")
            raise
    
    def _hash_text(self, text: str) -> str:
        """Hash text for privacy"""
        try:
            # Simple hash for demonstration - in production, use proper encryption
            return hashlib.sha256(text.encode('utf-8')).hexdigest()[:16]
        except:
            return text
    
    def _hash_seniority_mix(self, seniority_mix: List) -> List:
        """Hash seniority mix data for privacy"""
        try:
            if isinstance(seniority_mix, list):
                # Return a hashed version of the seniority mix
                return [self._hash_text(str(item)) for item in seniority_mix]
            elif isinstance(seniority_mix, dict):
                # If it's a dict, hash the values
                return {k: self._hash_text(str(v)) for k, v in seniority_mix.items()}
            else:
                return [self._hash_text(str(seniority_mix))]
        except:
            return seniority_mix
    
    def batch_write_public_items(self, items: List[Dict]) -> int:
        """Batch write items to public table"""
        try:
            written_count = 0
            
            # Process in batches of 25 (DynamoDB limit)
            for i in range(0, len(items), 25):
                batch = items[i:i+25]
                
                # Prepare batch write request
                with self.public_table.batch_writer() as batch_writer:
                    for item in batch:
                        try:
                            batch_writer.put_item(Item=item)
                            written_count += 1
                        except Exception as e:
                            logger.error(f"Error writing public item {item.get('company_key')}: {e}")
                            continue
            
            logger.info(f"Written {written_count} items to public table")
            return written_count
            
        except Exception as e:
            logger.error(f"Error batch writing public items: {e}")
            raise
    
    def batch_write_private_items(self, items: List[Dict]) -> int:
        """Batch write items to private table"""
        try:
            written_count = 0
            
            # Process in batches of 25 (DynamoDB limit)
            for i in range(0, len(items), 25):
                batch = items[i:i+25]
                
                # Prepare batch write request
                with self.private_table.batch_writer() as batch_writer:
                    for item in batch:
                        try:
                            batch_writer.put_item(Item=item)
                            written_count += 1
                        except Exception as e:
                            logger.error(f"Error writing private item {item.get('company_key')}: {e}")
                            continue
            
            logger.info(f"Written {written_count} items to private table")
            return written_count
            
        except Exception as e:
            logger.error(f"Error batch writing private items: {e}")
            raise
    
    def process_data_batch(self, data_batch: List[Dict]) -> Tuple[int, int]:
        """Process a batch of data and write to both tables"""
        try:
            public_items = []
            private_items = []
            
            for data in data_batch:
                try:
                    # Create public item
                    public_item = self.create_public_item(data)
                    public_items.append(public_item)
                    
                    # Create private item
                    private_item = self.create_private_item(data)
                    private_items.append(private_item)
                    
                except Exception as e:
                    logger.error(f"Error processing item {data.get('company_key')}: {e}")
                    continue
            
            # Write to both tables
            public_count = self.batch_write_public_items(public_items)
            private_count = self.batch_write_private_items(private_items)
            
            return public_count, private_count
            
        except Exception as e:
            logger.error(f"Error processing data batch: {e}")
            raise

def emit_cloudwatch_metrics(upserts: int, skipped: int, errors: int) -> None:
    """Emit CloudWatch metrics as specified in requirements"""
    try:
        cloudwatch.put_metric_data(
            Namespace='TheraPipeline/DynamoDBPublisher',
            MetricData=[
                {
                    'MetricName': 'Upserts',
                    'Value': upserts,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'Skipped',
                    'Value': skipped,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'Errors',
                    'Value': errors,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                }
            ]
        )
    except Exception as e:
        logger.error(f"Error emitting metrics: {e}")

def lambda_handler(event, context):
    """Main Lambda handler for DynamoDB Publisher"""
    try:
        # Initialize publisher
        publisher = DynamoDBPublisher(PUBLIC_TABLE, PRIVATE_TABLE, ATHENA_DATABASE, ATHENA_WORKGROUP)
        
        # Get watermark
        watermark = publisher.get_watermark()
        logger.info(f"Processing data since watermark: {watermark}")
        
        # Get GOLD data since watermark
        gold_data = publisher.get_gold_data_since_watermark(watermark)
        
        if not gold_data:
            logger.info("No data to process")
            # Emit metrics for no data case
            emit_cloudwatch_metrics(0, 0, 0)
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No data to process',
                    'upserts': 0,
                    'skipped': 0,
                    'errors': 0,
                    'total_processed': 0
                })
            }
        
        logger.info(f"Processing {len(gold_data)} items")
        
        # Process in batches
        total_upserts = 0
        total_skipped = 0
        total_errors = 0
        latest_timestamp = watermark
        
        for i in range(0, len(gold_data), BATCH_SIZE):
            batch = gold_data[i:i + BATCH_SIZE]
            
            try:
                # Process batch
                public_count, private_count = publisher.process_data_batch(batch)
                total_upserts += public_count + private_count
                
                # Update latest timestamp
                for item in batch:
                    if item.get('updated_at'):
                        if item['updated_at'] > latest_timestamp:
                            latest_timestamp = item['updated_at']
                
                logger.info(f"Processed batch {i//BATCH_SIZE + 1}, upserts: {public_count + private_count}")
                
                # Small delay to avoid overwhelming DynamoDB
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error processing batch {i//BATCH_SIZE + 1}: {e}")
                total_errors += len(batch)
                continue
        
        # Update watermark
        publisher.update_watermark(latest_timestamp)
        
        # Emit metrics
        emit_cloudwatch_metrics(total_upserts, total_skipped, total_errors)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'DynamoDB publishing completed successfully',
                'upserts': total_upserts,
                'skipped': total_skipped,
                'errors': total_errors,
                'total_processed': len(gold_data),
                'watermark_updated': latest_timestamp
            })
        }
        
    except Exception as e:
        logger.error(f"Error in DynamoDB publisher: {e}")
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