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

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
athena_client = boto3.client('athena')
cloudwatch = boto3.client('cloudwatch')
glue_client = boto3.client('glue')

# Environment variables
CURATED_BUCKET = os.environ['CURATED_BUCKET']
METRICS_BUCKET = os.environ['METRICS_BUCKET']
ATHENA_DATABASE = os.environ['ATHENA_DATABASE']
ATHENA_WORKGROUP = os.environ.get('ATHENA_WORKGROUP', 'primary')
DAYS_LOOKBACK = int(os.environ.get('DAYS_LOOKBACK', '1'))

class AMSJob:
    """AMS (Average Match Score) computation job"""
    
    def __init__(self, curated_bucket: str, metrics_bucket: str, athena_database: str, athena_workgroup: str):
        self.curated_bucket = curated_bucket
        self.metrics_bucket = metrics_bucket
        self.athena_database = athena_database
        self.athena_workgroup = athena_workgroup
    
    def get_shortlist_data(self, target_date: str) -> List[Dict]:
        """Get shortlist data from Athena for AMS computation"""
        try:
            query = f"""
            SELECT 
                challenge_id,
                company_key,
                final_score,
                embedding_similarity,
                ml_score,
                rule_features,
                reason,
                created_at
            FROM {self.athena_database}.shortlists
            WHERE date(created_at) = date '{target_date}'
            ORDER BY challenge_id, final_score DESC
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
            
            shortlist_data = []
            for row in response['ResultSet']['Rows'][1:]:  # Skip header
                if row['Data']:
                    data = {}
                    for i, field in enumerate(['challenge_id', 'company_key', 'final_score', 
                                             'embedding_similarity', 'ml_score', 'rule_features', 
                                             'reason', 'created_at']):
                        if i < len(row['Data']):
                            value = row['Data'][i].get('VarCharValue', '')
                            if field in ['final_score', 'embedding_similarity', 'ml_score']:
                                try:
                                    data[field] = float(value) if value else 0.0
                                except:
                                    data[field] = 0.0
                            elif field == 'rule_features':
                                try:
                                    data[field] = json.loads(value) if value else {}
                                except:
                                    data[field] = {}
                            else:
                                data[field] = value
                    
                    if data.get('challenge_id') and data.get('company_key'):
                        shortlist_data.append(data)
            
            logger.info(f"Retrieved {len(shortlist_data)} shortlist records for {target_date}")
            return shortlist_data
            
        except Exception as e:
            logger.error(f"Error getting shortlist data: {e}")
            return []
    
    def compute_ams_metrics(self, shortlist_data: List[Dict]) -> Dict:
        """Compute AMS metrics from shortlist data"""
        try:
            if not shortlist_data:
                return {
                    'total_challenges': 0,
                    'total_shortlists': 0,
                    'avg_ams_challenge': 0.0,
                    'avg_embedding_similarity': 0.0,
                    'avg_ml_score': 0.0,
                    'challenge_metrics': []
                }
            
            # Group by challenge_id
            challenges = {}
            for item in shortlist_data:
                challenge_id = item.get('challenge_id')
                if challenge_id not in challenges:
                    challenges[challenge_id] = []
                challenges[challenge_id].append(item)
            
            # Compute metrics for each challenge
            challenge_metrics = []
            total_ams = 0.0
            total_embedding_sim = 0.0
            total_ml_score = 0.0
            
            for challenge_id, items in challenges.items():
                # Sort by final_score descending
                items.sort(key=lambda x: x.get('final_score', 0), reverse=True)
                
                # Get top 10 scores
                top_10_scores = [item.get('final_score', 0) for item in items[:10]]
                
                # Compute AMS for this challenge
                ams_challenge = sum(top_10_scores) / len(top_10_scores) if top_10_scores else 0.0
                
                # Compute other metrics
                avg_embedding_sim = sum(item.get('embedding_similarity', 0) for item in items) / len(items)
                avg_ml_score = sum(item.get('ml_score', 0) for item in items) / len(items)
                
                challenge_metric = {
                    'challenge_id': challenge_id,
                    'total_shortlists': len(items),
                    'ams_challenge': ams_challenge,
                    'avg_embedding_similarity': avg_embedding_sim,
                    'avg_ml_score': avg_ml_score,
                    'top_score': max(item.get('final_score', 0) for item in items) if items else 0.0,
                    'min_score': min(item.get('final_score', 0) for item in items) if items else 0.0,
                    'score_std': self._calculate_std([item.get('final_score', 0) for item in items])
                }
                
                challenge_metrics.append(challenge_metric)
                
                # Add to totals
                total_ams += ams_challenge
                total_embedding_sim += avg_embedding_sim
                total_ml_score += avg_ml_score
            
            # Compute overall metrics
            num_challenges = len(challenges)
            overall_metrics = {
                'total_challenges': num_challenges,
                'total_shortlists': len(shortlist_data),
                'avg_ams_challenge': total_ams / num_challenges if num_challenges > 0 else 0.0,
                'avg_embedding_similarity': total_embedding_sim / num_challenges if num_challenges > 0 else 0.0,
                'avg_ml_score': total_ml_score / num_challenges if num_challenges > 0 else 0.0,
                'challenge_metrics': challenge_metrics
            }
            
            return overall_metrics
            
        except Exception as e:
            logger.error(f"Error computing AMS metrics: {e}")
            return {
                'total_challenges': 0,
                'total_shortlists': 0,
                'avg_ams_challenge': 0.0,
                'avg_embedding_similarity': 0.0,
                'avg_ml_score': 0.0,
                'challenge_metrics': []
            }
    
    def _calculate_std(self, values: List[float]) -> float:
        """Calculate standard deviation"""
        try:
            if len(values) < 2:
                return 0.0
            
            mean = sum(values) / len(values)
            variance = sum((x - mean) ** 2 for x in values) / len(values)
            return variance ** 0.5
        except:
            return 0.0
    
    def write_parquet_to_s3(self, metrics: Dict, target_date: str) -> Dict[str, str]:
        """Write AMS metrics as Parquet files to S3"""
        try:
            # Create DataFrame for overall metrics
            overall_data = {
                'date': [target_date],
                'total_challenges': [metrics['total_challenges']],
                'total_shortlists': [metrics['total_shortlists']],
                'avg_ams_challenge': [metrics['avg_ams_challenge']],
                'avg_embedding_similarity': [metrics['avg_embedding_similarity']],
                'avg_ml_score': [metrics['avg_ml_score']],
                'computed_at': [datetime.utcnow().isoformat()]
            }
            
            overall_df = pd.DataFrame(overall_data)
            
            # Create DataFrame for challenge metrics
            challenge_data = []
            for challenge_metric in metrics['challenge_metrics']:
                challenge_data.append({
                    'date': target_date,
                    'challenge_id': challenge_metric['challenge_id'],
                    'total_shortlists': challenge_metric['total_shortlists'],
                    'ams_challenge': challenge_metric['ams_challenge'],
                    'avg_embedding_similarity': challenge_metric['avg_embedding_similarity'],
                    'avg_ml_score': challenge_metric['avg_ml_score'],
                    'top_score': challenge_metric['top_score'],
                    'min_score': challenge_metric['min_score'],
                    'score_std': challenge_metric['score_std'],
                    'computed_at': datetime.utcnow().isoformat()
                })
            
            challenge_df = pd.DataFrame(challenge_data)
            
            # Convert to PyArrow Tables
            overall_table = pa.Table.from_pandas(overall_df)
            challenge_table = pa.Table.from_pandas(challenge_df)
            
            # Write to Parquet in memory
            overall_buffer = BytesIO()
            challenge_buffer = BytesIO()
            
            pq.write_table(overall_table, overall_buffer)
            pq.write_table(challenge_table, challenge_buffer)
            
            overall_buffer.seek(0)
            challenge_buffer.seek(0)
            
            # Upload to S3
            overall_key = f"metrics/ams/overall/date={target_date}/overall_{int(time.time())}.parquet"
            challenge_key = f"metrics/ams/challenges/date={target_date}/challenges_{int(time.time())}.parquet"
            
            s3_client.put_object(
                Bucket=self.metrics_bucket,
                Key=overall_key,
                Body=overall_buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            s3_client.put_object(
                Bucket=self.metrics_bucket,
                Key=challenge_key,
                Body=challenge_buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            logger.info(f"AMS metrics written to s3://{self.metrics_bucket}/{overall_key} and {challenge_key}")
            
            return {
                'overall_key': overall_key,
                'challenge_key': challenge_key
            }
            
        except Exception as e:
            logger.error(f"Error writing Parquet to S3: {e}")
            raise
    
    def create_glue_table_ddl(self) -> str:
        """Create Glue table DDL for AMS metrics"""
        try:
            ddl = f"""
            CREATE TABLE IF NOT EXISTS {self.athena_database}.metrics_ams (
                date string,
                total_challenges int,
                total_shortlists int,
                avg_ams_challenge double,
                avg_embedding_similarity double,
                avg_ml_score double,
                computed_at string
            )
            PARTITIONED BY (dt string)
            STORED AS PARQUET
            LOCATION 's3://{self.metrics_bucket}/metrics/ams/overall/'
            TBLPROPERTIES (
                'classification' = 'parquet',
                'typeOfData' = 'file'
            );
            
            CREATE TABLE IF NOT EXISTS {self.athena_database}.challenge_metrics (
                date string,
                challenge_id string,
                total_shortlists int,
                ams_challenge double,
                avg_embedding_similarity double,
                avg_ml_score double,
                top_score double,
                min_score double,
                score_std double,
                computed_at string
            )
            PARTITIONED BY (dt string)
            STORED AS PARQUET
            LOCATION 's3://{self.metrics_bucket}/metrics/ams/challenges/'
            TBLPROPERTIES (
                'classification' = 'parquet',
                'typeOfData' = 'file'
            );
            """
            
            return ddl
            
        except Exception as e:
            logger.error(f"Error creating Glue table DDL: {e}")
            raise
    
    def create_glue_tables(self) -> bool:
        """Create Glue tables for AMS metrics"""
        try:
            ddl = self.create_glue_table_ddl()
            
            # Execute DDL
            response = athena_client.start_query_execution(
                QueryString=ddl,
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
                    raise Exception(f"DDL execution failed: {response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')}")
                
                time.sleep(2)
            
            logger.info("Glue tables created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating Glue tables: {e}")
            return False

def emit_cloudwatch_metrics(total_challenges: int, total_shortlists: int, avg_ams: float) -> None:
    """Emit CloudWatch metrics"""
    try:
        cloudwatch.put_metric_data(
            Namespace='TheraPipeline/AMSJob',
            MetricData=[
                {
                    'MetricName': 'TotalChallenges',
                    'Value': total_challenges,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'TotalShortlists',
                    'Value': total_shortlists,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'AverageAMS',
                    'Value': avg_ams,
                    'Unit': 'None',
                    'Timestamp': datetime.utcnow()
                }
            ]
        )
    except Exception as e:
        logger.error(f"Error emitting metrics: {e}")

def lambda_handler(event, context):
    """Main Lambda handler for AMS job"""
    try:
        # Get target date from event or use yesterday
        target_date = event.get('target_date')
        if not target_date:
            target_date = (datetime.utcnow() - timedelta(days=DAYS_LOOKBACK)).strftime('%Y-%m-%d')
        
        logger.info(f"Running AMS job for {target_date}")
        
        # Initialize AMS job
        ams_job = AMSJob(CURATED_BUCKET, METRICS_BUCKET, ATHENA_DATABASE, ATHENA_WORKGROUP)
        
        # Get shortlist data
        shortlist_data = ams_job.get_shortlist_data(target_date)
        
        if not shortlist_data:
            logger.warning(f"No shortlist data found for {target_date}")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'No shortlist data found for {target_date}',
                    'target_date': target_date,
                    'total_challenges': 0,
                    'total_shortlists': 0,
                    'avg_ams_challenge': 0.0
                })
            }
        
        # Compute AMS metrics
        metrics = ams_job.compute_ams_metrics(shortlist_data)
        
        # Write Parquet files to S3
        s3_keys = ams_job.write_parquet_to_s3(metrics, target_date)
        
        # Create Glue tables
        tables_created = ams_job.create_glue_tables()
        
        # Emit metrics
        emit_cloudwatch_metrics(
            metrics['total_challenges'],
            metrics['total_shortlists'],
            metrics['avg_ams_challenge']
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'AMS job completed successfully',
                'target_date': target_date,
                'total_challenges': metrics['total_challenges'],
                'total_shortlists': metrics['total_shortlists'],
                'avg_ams_challenge': metrics['avg_ams_challenge'],
                'avg_embedding_similarity': metrics['avg_embedding_similarity'],
                'avg_ml_score': metrics['avg_ml_score'],
                's3_keys': s3_keys,
                'tables_created': tables_created
            })
        }
        
    except Exception as e:
        logger.error(f"Error in AMS job: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

# For local testing
if __name__ == "__main__":
    # Test with sample data
    test_event = {
        'target_date': '2024-01-15'
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))
