import json
import boto3
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging
from dataclasses import dataclass
from botocore.exceptions import ClientError, BotoCoreError
import pandas as pd
import numpy as np
from sklearn.metrics import roc_auc_score, precision_recall_curve, auc, classification_report, confusion_matrix
from sklearn.model_selection import cross_val_score
import joblib
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
athena_client = boto3.client('athena')
glue_client = boto3.client('glue')

@dataclass
class EvaluationConfig:
    """Configuration for evaluation metrics"""
    cv_folds: int = 5
    confidence_threshold: float = 0.5
    top_k_features: int = 20

@dataclass
class ModelMetrics:
    """Model evaluation metrics"""
    model_name: str
    accuracy: float
    precision: float
    recall: float
    f1_score: float
    auc_score: float
    pr_auc_score: float
    cross_val_mean: float
    cross_val_std: float
    confusion_matrix: List[List[int]]
    feature_importance: Dict[str, float]
    roc_curve: Dict[str, List[float]]
    pr_curve: Dict[str, List[float]]

class ModelEvaluator:
    """Evaluates ML models and generates comprehensive metrics"""
    
    def __init__(self, config: EvaluationConfig):
        self.config = config
    
    def load_latest_models(self) -> List[Dict[str, Any]]:
        """Load the latest trained models from S3"""
        try:
            # List model files in S3
            response = s3_client.list_objects_v2(
                Bucket=os.environ['S3_BUCKET'],
                Prefix=f"{os.environ['S3_PREFIX']}/ml-models/"
            )
            
            if 'Contents' not in response:
                logger.warning("No model files found in S3")
                return []
            
            # Get the most recent model files
            model_files = []
            for obj in response['Contents']:
                if obj['Key'].endswith('.joblib') and 'logistic_regression' in obj['Key']:
                    model_files.append({
                        'key': obj['Key'],
                        'last_modified': obj['LastModified'],
                        'model_type': 'logistic_regression'
                    })
                elif obj['Key'].endswith('.joblib') and 'random_forest' in obj['Key']:
                    model_files.append({
                        'key': obj['Key'],
                        'last_modified': obj['LastModified'],
                        'model_type': 'random_forest'
                    })
            
            # Sort by last modified and get the most recent of each type
            model_files.sort(key=lambda x: x['last_modified'], reverse=True)
            
            latest_models = {}
            for model_file in model_files:
                if model_file['model_type'] not in latest_models:
                    latest_models[model_file['model_type']] = model_file
            
            # Load the models
            loaded_models = []
            for model_type, model_file in latest_models.items():
                try:
                    # Download and load model
                    response = s3_client.get_object(
                        Bucket=os.environ['S3_BUCKET'],
                        Key=model_file['key']
                    )
                    
                    model_data = joblib.loads(response['Body'].read())
                    loaded_models.append({
                        'model_type': model_type,
                        'model_data': model_data,
                        's3_key': model_file['key']
                    })
                    
                    logger.info(f"Loaded {model_type} model from {model_file['key']}")
                    
                except Exception as e:
                    logger.error(f"Failed to load {model_type} model: {e}")
            
            return loaded_models
            
        except Exception as e:
            logger.error(f"Failed to load models from S3: {e}")
            raise
    
    def get_test_data(self) -> Tuple[np.ndarray, np.ndarray, List[str]]:
        """Get test data from Athena for model evaluation"""
        try:
            query = """
            SELECT 
                company_id,
                employee_count,
                annual_revenue,
                founded_year,
                company_age_years,
                domain_health_score,
                total_contacts,
                senior_contacts,
                pages_crawled,
                is_ai_company,
                is_cloud_company,
                has_web_presence,
                has_extracted_content,
                industry,
                headquarters_city,
                headquarters_state,
                headquarters_country,
                company_size_category,
                revenue_stage,
                company_description,
                technologies,
                keywords,
                -- Target variable
                CASE 
                    WHEN annual_revenue > 1000000 AND employee_count > 10 THEN 1
                    WHEN domain_health_score > 80 AND pages_crawled > 0 THEN 1
                    WHEN total_contacts > 5 AND senior_contacts > 2 THEN 1
                    ELSE 0
                END as is_successful
            FROM thera_gold.startup_profiles
            WHERE company_id IS NOT NULL
            AND company_name IS NOT NULL
            AND year = year(current_date)
            AND month = month(current_date)
            AND day = day(current_date)
            """
            
            response = athena_client.start_query_execution(
                QueryString=query,
                WorkGroup=os.environ.get('ATHENA_WORKGROUP', 'primary'),
                ResultConfiguration={
                    'OutputLocation': os.environ['S3_OUTPUT_LOCATION']
                }
            )
            
            query_execution_id = response['QueryExecutionId']
            
            # Wait for query completion
            while True:
                response = athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                status = response['QueryExecution']['Status']['State']
                
                if status in ['SUCCEEDED']:
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    raise Exception(f"Athena query failed: {response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')}")
                
                time.sleep(5)
            
            # Get query results
            response = athena_client.get_query_results(
                QueryExecutionId=query_execution_id
            )
            
            # Convert to DataFrame
            data = []
            for row in response['ResultSet']['Rows'][1:]:  # Skip header row
                if row['Data'] and len(row['Data']) >= 24:
                    data.append([
                        row['Data'][0].get('VarCharValue', ''),
                        int(row['Data'][1].get('VarCharValue', '0') or '0'),
                        int(row['Data'][2].get('VarCharValue', '0') or '0'),
                        int(row['Data'][3].get('VarCharValue', '0') or '0'),
                        int(row['Data'][4].get('VarCharValue', '0') or '0'),
                        float(row['Data'][5].get('VarCharValue', '0') or '0'),
                        int(row['Data'][6].get('VarCharValue', '0') or '0'),
                        int(row['Data'][7].get('VarCharValue', '0') or '0'),
                        int(row['Data'][8].get('VarCharValue', '0') or '0'),
                        int(row['Data'][9].get('VarCharValue', '0') or '0'),
                        int(row['Data'][10].get('VarCharValue', '0') or '0'),
                        int(row['Data'][11].get('VarCharValue', '0') or '0'),
                        int(row['Data'][12].get('VarCharValue', '0') or '0'),
                        row['Data'][13].get('VarCharValue', ''),
                        row['Data'][14].get('VarCharValue', ''),
                        row['Data'][15].get('VarCharValue', ''),
                        row['Data'][16].get('VarCharValue', ''),
                        row['Data'][17].get('VarCharValue', ''),
                        row['Data'][18].get('VarCharValue', ''),
                        row['Data'][19].get('VarCharValue', ''),
                        row['Data'][20].get('VarCharValue', ''),
                        row['Data'][21].get('VarCharValue', ''),
                        row['Data'][22].get('VarCharValue', ''),
                        int(row['Data'][23].get('VarCharValue', '0') or '0')
                    ])
            
            df = pd.DataFrame(data, columns=[
                'company_id', 'employee_count', 'annual_revenue', 'founded_year',
                'company_age_years', 'domain_health_score', 'total_contacts',
                'senior_contacts', 'pages_crawled', 'is_ai_company', 'is_cloud_company',
                'has_web_presence', 'has_extracted_content', 'industry',
                'headquarters_city', 'headquarters_state', 'headquarters_country',
                'company_size_category', 'revenue_stage', 'company_description',
                'technologies', 'keywords', 'is_successful'
            ])
            
            # Preprocess the data (same as training)
            df = self._preprocess_data(df)
            
            # Extract features and target
            feature_columns = [col for col in df.columns if col not in ['company_id', 'is_successful']]
            X = df[feature_columns].values
            y = df['is_successful'].values
            
            logger.info(f"Loaded test data: {len(X)} samples, {len(feature_columns)} features")
            return X, y, feature_columns
            
        except Exception as e:
            logger.error(f"Failed to get test data: {e}")
            raise
    
    def _preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess data for evaluation (same as training)"""
        try:
            # Handle missing values
            df = df.fillna({
                'employee_count': 0,
                'annual_revenue': 0,
                'founded_year': 0,
                'company_age_years': 0,
                'domain_health_score': 0,
                'total_contacts': 0,
                'senior_contacts': 0,
                'pages_crawled': 0
            })
            
            # Encode categorical variables
            categorical_columns = ['industry', 'headquarters_city', 'headquarters_state', 
                                 'headquarters_country', 'company_size_category', 'revenue_stage']
            
            for col in categorical_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str)
                    df[col] = pd.Categorical(df[col]).codes
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to preprocess data: {e}")
            raise
    
    def evaluate_model(self, model_data: Dict[str, Any], X_test: np.ndarray, y_test: np.ndarray, 
                      feature_names: List[str]) -> ModelMetrics:
        """Evaluate a single model and return comprehensive metrics"""
        try:
            model = model_data['model']
            scaler = model_data['scaler']
            vectorizer = model_data['vectorizer']
            
            # Preprocess test data
            X_test_scaled = scaler.transform(X_test)
            
            # Make predictions
            y_pred = model.predict(X_test_scaled)
            y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]
            
            # Calculate basic metrics
            accuracy = model.score(X_test_scaled, y_test)
            
            # Classification report
            report = classification_report(y_test, y_pred, output_dict=True)
            precision = report['weighted avg']['precision']
            recall = report['weighted avg']['recall']
            f1_score = report['weighted avg']['f1-score']
            
            # AUC scores
            auc_score = roc_auc_score(y_test, y_pred_proba)
            
            # Precision-Recall AUC
            precision_curve, recall_curve, _ = precision_recall_curve(y_test, y_pred_proba)
            pr_auc_score = auc(recall_curve, precision_curve)
            
            # Cross-validation scores
            cv_scores = cross_val_score(model, X_test_scaled, y_test, cv=self.config.cv_folds)
            cv_mean = np.mean(cv_scores)
            cv_std = np.std(cv_scores)
            
            # Confusion matrix
            cm = confusion_matrix(y_test, y_pred)
            
            # ROC curve
            from sklearn.metrics import roc_curve
            fpr, tpr, _ = roc_curve(y_test, y_pred_proba)
            roc_curve_data = {'fpr': fpr.tolist(), 'tpr': tpr.tolist()}
            
            # Precision-Recall curve
            pr_curve_data = {'precision': precision_curve.tolist(), 'recall': recall_curve.tolist()}
            
            # Feature importance
            feature_importance = {}
            if hasattr(model, 'coef_'):
                # Logistic Regression
                feature_importance = dict(zip(feature_names, model.coef_[0]))
            elif hasattr(model, 'feature_importances_'):
                # Random Forest
                feature_importance = dict(zip(feature_names, model.feature_importances_))
            
            # Sort by importance and get top features
            sorted_features = sorted(feature_importance.items(), key=lambda x: abs(x[1]), reverse=True)
            top_features = dict(sorted_features[:self.config.top_k_features])
            
            return ModelMetrics(
                model_name=model_data['metadata']['model_name'],
                accuracy=accuracy,
                precision=precision,
                recall=recall,
                f1_score=f1_score,
                auc_score=auc_score,
                pr_auc_score=pr_auc_score,
                cross_val_mean=cv_mean,
                cross_val_std=cv_std,
                confusion_matrix=cm.tolist(),
                feature_importance=top_features,
                roc_curve=roc_curve_data,
                pr_curve=pr_curve_data
            )
            
        except Exception as e:
            logger.error(f"Failed to evaluate model: {e}")
            raise
    
    def save_metrics_to_s3(self, metrics: List[ModelMetrics]) -> str:
        """Save evaluation metrics to S3"""
        try:
            timestamp = datetime.utcnow().isoformat()
            metrics_key = f"{os.environ['S3_PREFIX']}/ml-evaluation/evaluation_metrics_{timestamp}.json"
            
            # Prepare metrics data
            metrics_data = {
                'metadata': {
                    'evaluation_date': timestamp,
                    'evaluation_type': 'weekly_evaluation',
                    'models_evaluated': len(metrics)
                },
                'models': []
            }
            
            for metric in metrics:
                model_data = {
                    'model_name': metric.model_name,
                    'accuracy': metric.accuracy,
                    'precision': metric.precision,
                    'recall': metric.recall,
                    'f1_score': metric.f1_score,
                    'auc_score': metric.auc_score,
                    'pr_auc_score': metric.pr_auc_score,
                    'cross_val_mean': metric.cross_val_mean,
                    'cross_val_std': metric.cross_val_std,
                    'confusion_matrix': metric.confusion_matrix,
                    'feature_importance': metric.feature_importance,
                    'roc_curve': metric.roc_curve,
                    'pr_curve': metric.pr_curve
                }
                metrics_data['models'].append(model_data)
            
            # Save to S3
            s3_client.put_object(
                Bucket=os.environ['S3_BUCKET'],
                Key=metrics_key,
                Body=json.dumps(metrics_data, indent=2),
                ContentType='application/json',
                ServerSideEncryption='AES256'
            )
            
            logger.info(f"Saved evaluation metrics to {metrics_key}")
            return metrics_key
            
        except Exception as e:
            logger.error(f"Failed to save metrics to S3: {e}")
            raise
    
    def save_metrics_to_glue(self, metrics: List[ModelMetrics]) -> str:
        """Save evaluation metrics to Glue table"""
        try:
            # Create Glue table if it doesn't exist
            table_name = f"ml_evaluation_metrics_{os.environ.get('ENVIRONMENT', 'prod')}"
            
            # Prepare data for Glue table
            glue_data = []
            timestamp = datetime.utcnow().isoformat()
            
            for metric in metrics:
                glue_data.append({
                    'evaluation_date': timestamp,
                    'model_name': metric.model_name,
                    'accuracy': metric.accuracy,
                    'precision': metric.precision,
                    'recall': metric.recall,
                    'f1_score': metric.f1_score,
                    'auc_score': metric.auc_score,
                    'pr_auc_score': metric.pr_auc_score,
                    'cross_val_mean': metric.cross_val_mean,
                    'cross_val_std': metric.cross_val_std,
                    'top_features': json.dumps(metric.feature_importance),
                    'confusion_matrix': json.dumps(metric.confusion_matrix),
                    'roc_curve': json.dumps(metric.roc_curve),
                    'pr_curve': json.dumps(metric.pr_curve)
                })
            
            # Save to S3 as Parquet for Glue
            parquet_key = f"{os.environ['S3_PREFIX']}/glue-tables/ml_evaluation_metrics/year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}/evaluation_metrics_{timestamp}.parquet"
            
            # Convert to DataFrame and save as Parquet
            df = pd.DataFrame(glue_data)
            
            # For simplicity, save as JSON (in production, use pyarrow to save as Parquet)
            s3_client.put_object(
                Bucket=os.environ['S3_BUCKET'],
                Key=parquet_key,
                Body=df.to_json(orient='records', lines=True),
                ContentType='application/json',
                ServerSideEncryption='AES256'
            )
            
            logger.info(f"Saved evaluation metrics to Glue table at {parquet_key}")
            return parquet_key
            
        except Exception as e:
            logger.error(f"Failed to save metrics to Glue: {e}")
            raise

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for evaluation metrics
    
    Expected environment variables:
    - ATHENA_WORKGROUP: Athena workgroup name
    - S3_OUTPUT_LOCATION: S3 location for Athena query results
    - S3_BUCKET: S3 bucket for storing metrics
    - S3_PREFIX: S3 prefix for organizing metrics
    - ENVIRONMENT: Environment name
    - CV_FOLDS: Cross-validation folds (default: 5)
    - CONFIDENCE_THRESHOLD: Confidence threshold (default: 0.5)
    - TOP_K_FEATURES: Top K features to include (default: 20)
    """
    logger.info(f"Starting evaluation metrics with event: {json.dumps(event)}")
    
    try:
        # Initialize configuration
        config = EvaluationConfig(
            cv_folds=int(os.environ.get('CV_FOLDS', '5')),
            confidence_threshold=float(os.environ.get('CONFIDENCE_THRESHOLD', '0.5')),
            top_k_features=int(os.environ.get('TOP_K_FEATURES', '20'))
        )
        
        # Initialize evaluator
        evaluator = ModelEvaluator(config)
        
        # Load latest models
        models = evaluator.load_latest_models()
        
        if not models:
            logger.warning("No models found for evaluation")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No models found for evaluation',
                    'success': True
                })
            }
        
        # Get test data
        X_test, y_test, feature_names = evaluator.get_test_data()
        
        if len(X_test) < 10:
            logger.warning("Insufficient test data for evaluation")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Insufficient test data for evaluation',
                    'success': True
                })
            }
        
        # Evaluate models
        metrics = []
        for model_info in models:
            try:
                metric = evaluator.evaluate_model(
                    model_info['model_data'], X_test, y_test, feature_names
                )
                metrics.append(metric)
                logger.info(f"Evaluated {metric.model_name}: AUC={metric.auc_score:.3f}, Accuracy={metric.accuracy:.3f}")
            except Exception as e:
                logger.error(f"Failed to evaluate {model_info['model_type']}: {e}")
        
        if not metrics:
            logger.error("No models were successfully evaluated")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'No models were successfully evaluated',
                    'success': False
                })
            }
        
        # Save metrics to S3
        s3_metrics_key = evaluator.save_metrics_to_s3(metrics)
        
        # Save metrics to Glue
        glue_metrics_key = evaluator.save_metrics_to_glue(metrics)
        
        # Prepare response
        response_data = {
            'metadata': {
                'start_time': datetime.utcnow().isoformat(),
                'config': {
                    'cv_folds': config.cv_folds,
                    'confidence_threshold': config.confidence_threshold,
                    'top_k_features': config.top_k_features
                },
                'success': True
            },
            'evaluation_summary': {
                'models_evaluated': len(metrics),
                'test_samples': len(X_test),
                'test_features': len(feature_names),
                's3_metrics_key': s3_metrics_key,
                'glue_metrics_key': glue_metrics_key
            },
            'model_metrics': [
                {
                    'model_name': m.model_name,
                    'accuracy': m.accuracy,
                    'precision': m.precision,
                    'recall': m.recall,
                    'f1_score': m.f1_score,
                    'auc_score': m.auc_score,
                    'pr_auc_score': m.pr_auc_score,
                    'cross_val_mean': m.cross_val_mean,
                    'cross_val_std': m.cross_val_std
                }
                for m in metrics
            ]
        }
        
        logger.info(f"Evaluation metrics completed: {json.dumps(response_data, indent=2)}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(response_data),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
        
    except Exception as e:
        logger.error(f"Evaluation metrics failed: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'success': False
            }),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
