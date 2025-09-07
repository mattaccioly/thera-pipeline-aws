import json
import boto3
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, precision_recall_curve, auc
import joblib
from io import BytesIO

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
athena_client = boto3.client('athena')
cloudwatch = boto3.client('cloudwatch')

# Environment variables
CURATED_BUCKET = os.environ['CURATED_BUCKET']
MODEL_BUCKET = os.environ['MODEL_BUCKET']
ATHENA_DATABASE = os.environ['ATHENA_DATABASE']
ATHENA_WORKGROUP = os.environ.get('ATHENA_WORKGROUP', 'primary')
MODEL_KEY = os.environ.get('MODEL_KEY', 'models/match_lr/model.json')
EVAL_KEY = os.environ.get('EVAL_KEY', 'models/match_lr/eval.json')
DAYS_LOOKBACK = int(os.environ.get('DAYS_LOOKBACK', '7'))

class WeeklyTrainer:
    """Weekly ML trainer using scikit-learn logistic regression"""
    
    def __init__(self, curated_bucket: str, model_bucket: str, athena_database: str, athena_workgroup: str):
        self.curated_bucket = curated_bucket
        self.model_bucket = model_bucket
        self.athena_database = athena_database
        self.athena_workgroup = athena_workgroup
    
    def unload_training_data(self, days_lookback: int = 7) -> str:
        """Unload training data from Athena to S3"""
        try:
            # Calculate date range
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=days_lookback)
            
            # Create S3 output path
            output_path = f"s3://{self.curated_bucket}/training_data/weekly/{end_date.strftime('%Y%m%d')}/"
            
            query = f"""
            UNLOAD (
                SELECT 
                    s.challenge_id,
                    s.company_key,
                    s.final_score,
                    s.embedding_similarity,
                    s.ml_score,
                    s.rule_features,
                    s.reason,
                    s.created_at,
                    c.industry,
                    c.country,
                    c.employee_count,
                    c.annual_revenue,
                    c.total_funding,
                    c.domain_health_score,
                    c.content_richness_score,
                    CASE 
                        WHEN s.created_at >= c.updated_at - interval '14 days' 
                        AND s.final_score > 0.7 
                        THEN 1 
                        ELSE 0 
                    END as contacted_within_14_days
                FROM {self.athena_database}.shortlists s
                JOIN {self.athena_database}.gold_startup_profiles c
                    ON s.company_key = c.company_key
                WHERE s.created_at >= timestamp '{start_date.strftime('%Y-%m-%d')}'
                AND s.created_at <= timestamp '{end_date.strftime('%Y-%m-%d')}'
                AND s.final_score IS NOT NULL
                AND s.embedding_similarity IS NOT NULL
                AND s.ml_score IS NOT NULL
                ORDER BY s.created_at DESC
            )
            TO '{output_path}'
            WITH (format = 'PARQUET', compression = 'SNAPPY')
            """
            
            # Execute query
            response = athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.athena_database},
                WorkGroup=self.athena_workgroup
            )
            
            query_execution_id = response['QueryExecutionId']
            logger.info(f"Started data unload with execution ID: {query_execution_id}")
            
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
                
                time.sleep(5)
            
            logger.info(f"Data unloaded successfully to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error unloading training data: {e}")
            raise
    
    def load_training_data_from_s3(self, s3_path: str) -> pd.DataFrame:
        """Load training data from S3 Parquet files"""
        try:
            # Parse S3 path
            bucket = s3_path.split('/')[2]
            prefix = '/'.join(s3_path.split('/')[3:])
            
            # List all Parquet files
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                raise Exception(f"No files found in {s3_path}")
            
            # Load all Parquet files
            dfs = []
            for obj in response['Contents']:
                if obj['Key'].endswith('.parquet'):
                    # Download and read Parquet file
                    file_response = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
                    df = pd.read_parquet(BytesIO(file_response['Body'].read()))
                    dfs.append(df)
            
            if not dfs:
                raise Exception("No Parquet files found")
            
            # Combine all DataFrames
            combined_df = pd.concat(dfs, ignore_index=True)
            logger.info(f"Loaded {len(combined_df)} training records from S3")
            return combined_df
            
        except Exception as e:
            logger.error(f"Error loading training data from S3: {e}")
            raise
    
    def prepare_features(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, List[str]]:
        """Prepare features for training"""
        try:
            # Create feature matrix
            features = []
            feature_names = []
            
            # Embedding similarity
            features.append(df['embedding_similarity'].values)
            feature_names.append('embedding_similarity')
            
            # ML score
            features.append(df['ml_score'].values)
            feature_names.append('ml_score')
            
            # Industry match (from rule_features)
            industry_match = []
            for rule_features in df['rule_features']:
                if isinstance(rule_features, dict):
                    industry_match.append(rule_features.get('industry_match', 0.0))
                else:
                    industry_match.append(0.0)
            features.append(np.array(industry_match))
            feature_names.append('industry_match')
            
            # Geographic match (from rule_features)
            geo_match = []
            for rule_features in df['rule_features']:
                if isinstance(rule_features, dict):
                    geo_match.append(rule_features.get('geo_match', 0.0))
                else:
                    geo_match.append(0.0)
            features.append(np.array(geo_match))
            feature_names.append('geo_match')
            
            # Name similarity (from rule_features)
            name_similarity = []
            for rule_features in df['rule_features']:
                if isinstance(rule_features, dict):
                    name_similarity.append(rule_features.get('name_similarity', 0.0))
                else:
                    name_similarity.append(0.0)
            features.append(np.array(name_similarity))
            feature_names.append('name_similarity')
            
            # Company features
            features.append(df['employee_count'].fillna(0).values)
            feature_names.append('employee_count')
            
            features.append(df['annual_revenue'].fillna(0).values)
            feature_names.append('annual_revenue')
            
            features.append(df['total_funding'].fillna(0).values)
            feature_names.append('total_funding')
            
            features.append(df['domain_health_score'].fillna(0).values)
            feature_names.append('domain_health_score')
            
            features.append(df['content_richness_score'].fillna(0).values)
            feature_names.append('content_richness_score')
            
            # Industry encoding (one-hot)
            industries = df['industry'].fillna('unknown').unique()
            for industry in industries:
                industry_encoded = (df['industry'].fillna('unknown') == industry).astype(int).values
                features.append(industry_encoded)
                feature_names.append(f'industry_{industry}')
            
            # Country encoding (one-hot)
            countries = df['country'].fillna('unknown').unique()
            for country in countries:
                country_encoded = (df['country'].fillna('unknown') == country).astype(int).values
                features.append(country_encoded)
                feature_names.append(f'country_{country}')
            
            # Combine features
            X = np.column_stack(features)
            y = df['contacted_within_14_days'].values
            
            logger.info(f"Prepared features: {X.shape[1]} features, {X.shape[0]} samples")
            return X, y, feature_names
            
        except Exception as e:
            logger.error(f"Error preparing features: {e}")
            raise
    
    def train_model(self, X: np.ndarray, y: np.ndarray, feature_names: List[str]) -> Dict:
        """Train logistic regression model"""
        try:
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42, stratify=y
            )
            
            # Train model
            model = LogisticRegression(
                random_state=42,
                max_iter=1000,
                class_weight='balanced'  # Handle class imbalance
            )
            
            model.fit(X_train, y_train)
            
            # Evaluate model
            y_pred = model.predict(X_test)
            y_pred_proba = model.predict_proba(X_test)[:, 1]
            
            # Calculate metrics
            auc_score = roc_auc_score(y_test, y_pred_proba)
            precision, recall, thresholds = precision_recall_curve(y_test, y_pred_proba)
            pr_auc = auc(recall, precision)
            
            # Get feature importance
            feature_importance = dict(zip(feature_names, model.coef_[0]))
            
            # Create model data
            model_data = {
                'model_type': 'logistic_regression',
                'coefficients': model.coef_[0].tolist(),
                'intercept': model.intercept_[0],
                'feature_order': feature_names,
                'training_samples': len(X_train),
                'test_samples': len(X_test),
                'auc_score': auc_score,
                'pr_auc_score': pr_auc,
                'trained_at': datetime.utcnow().isoformat(),
                'model_version': f"v{int(time.time())}"
            }
            
            logger.info(f"Model trained successfully: AUC={auc_score:.3f}, PR-AUC={pr_auc:.3f}")
            return model_data
            
        except Exception as e:
            logger.error(f"Error training model: {e}")
            raise
    
    def save_model(self, model_data: Dict) -> str:
        """Save model to S3 as JSON"""
        try:
            # Create model JSON
            model_json = json.dumps(model_data, indent=2)
            
            # Upload to S3
            s3_client.put_object(
                Bucket=self.model_bucket,
                Key=MODEL_KEY,
                Body=model_json.encode('utf-8'),
                ContentType='application/json'
            )
            
            logger.info(f"Model saved to s3://{self.model_bucket}/{MODEL_KEY}")
            return MODEL_KEY
            
        except Exception as e:
            logger.error(f"Error saving model: {e}")
            raise
    
    def save_evaluation_metrics(self, model_data: Dict) -> str:
        """Save evaluation metrics to S3 as JSON"""
        try:
            eval_data = {
                'auc_score': model_data['auc_score'],
                'pr_auc_score': model_data['pr_auc_score'],
                'training_samples': model_data['training_samples'],
                'test_samples': model_data['test_samples'],
                'model_version': model_data['model_version'],
                'trained_at': model_data['trained_at']
            }
            
            eval_json = json.dumps(eval_data, indent=2)
            
            # Upload to S3
            s3_client.put_object(
                Bucket=self.model_bucket,
                Key=EVAL_KEY,
                Body=eval_json.encode('utf-8'),
                ContentType='application/json'
            )
            
            logger.info(f"Evaluation metrics saved to s3://{self.model_bucket}/{EVAL_KEY}")
            return EVAL_KEY
            
        except Exception as e:
            logger.error(f"Error saving evaluation metrics: {e}")
            raise

def emit_cloudwatch_metrics(training_samples: int, test_samples: int, auc_score: float, pr_auc_score: float) -> None:
    """Emit CloudWatch metrics"""
    try:
        cloudwatch.put_metric_data(
            Namespace='TheraPipeline/WeeklyTraining',
            MetricData=[
                {
                    'MetricName': 'TrainingSamples',
                    'Value': training_samples,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'TestSamples',
                    'Value': test_samples,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'AUCScore',
                    'Value': auc_score,
                    'Unit': 'None',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'PRAUCScore',
                    'Value': pr_auc_score,
                    'Unit': 'None',
                    'Timestamp': datetime.utcnow()
                }
            ]
        )
    except Exception as e:
        logger.error(f"Error emitting metrics: {e}")

def lambda_handler(event, context):
    """Main Lambda handler for weekly ML training"""
    try:
        # Initialize trainer
        trainer = WeeklyTrainer(CURATED_BUCKET, MODEL_BUCKET, ATHENA_DATABASE, ATHENA_WORKGROUP)
        
        # Unload training data to S3
        s3_path = trainer.unload_training_data(DAYS_LOOKBACK)
        
        # Load training data from S3
        df = trainer.load_training_data_from_s3(s3_path)
        
        if len(df) < 100:  # Minimum samples for training
            logger.warning(f"Insufficient training data: {len(df)} samples")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'Insufficient training data: {len(df)} samples',
                    'training_samples': len(df),
                    'model_saved': False
                })
            }
        
        # Prepare features
        X, y, feature_names = trainer.prepare_features(df)
        
        # Train model
        model_data = trainer.train_model(X, y, feature_names)
        
        # Save model
        model_key = trainer.save_model(model_data)
        
        # Save evaluation metrics
        eval_key = trainer.save_evaluation_metrics(model_data)
        
        # Emit metrics
        emit_cloudwatch_metrics(
            model_data['training_samples'],
            model_data['test_samples'],
            model_data['auc_score'],
            model_data['pr_auc_score']
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Weekly ML training completed successfully',
                'training_samples': model_data['training_samples'],
                'test_samples': model_data['test_samples'],
                'auc_score': model_data['auc_score'],
                'pr_auc_score': model_data['pr_auc_score'],
                'model_key': model_key,
                'eval_key': eval_key,
                's3_data_path': s3_path
            })
        }
        
    except Exception as e:
        logger.error(f"Error in weekly ML training: {e}")
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
