import json
import boto3
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
from dataclasses import dataclass
from botocore.exceptions import ClientError, BotoCoreError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
athena_client = boto3.client('athena')
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

@dataclass
class CTASConfig:
    """Configuration for CTAS operations"""
    workgroup: str = 'primary'
    output_location: str = ''
    database: str = 'thera_silver'
    timeout_minutes: int = 30

class AthenaQueryExecutor:
    """Handles Athena query execution with proper error handling and monitoring"""
    
    def __init__(self, config: CTASConfig):
        self.config = config
        self.s3_client = s3_client
        self.athena_client = athena_client
    
    def _wait_for_query_completion(self, query_execution_id: str) -> Dict[str, Any]:
        """Wait for Athena query to complete with timeout"""
        start_time = time.time()
        timeout_seconds = self.config.timeout_minutes * 60
        
        while True:
            try:
                response = self.athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                
                status = response['QueryExecution']['Status']['State']
                
                if status in ['SUCCEEDED']:
                    logger.info(f"Query {query_execution_id} completed successfully")
                    return response['QueryExecution']
                
                elif status in ['FAILED', 'CANCELLED']:
                    reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    logger.error(f"Query {query_execution_id} failed: {reason}")
                    raise Exception(f"Athena query failed: {reason}")
                
                elif time.time() - start_time > timeout_seconds:
                    logger.error(f"Query {query_execution_id} timed out after {self.config.timeout_minutes} minutes")
                    self.athena_client.stop_query_execution(QueryExecutionId=query_execution_id)
                    raise Exception(f"Athena query timed out after {self.config.timeout_minutes} minutes")
                
                else:
                    logger.info(f"Query {query_execution_id} status: {status}")
                    time.sleep(10)  # Wait 10 seconds before checking again
                    
            except ClientError as e:
                logger.error(f"Error checking query status: {e}")
                raise
    
    def execute_query(self, query: str, query_name: str) -> Dict[str, Any]:
        """Execute an Athena query and wait for completion"""
        logger.info(f"Executing query: {query_name}")
        
        try:
            # Start query execution
            response = self.athena_client.start_query_execution(
                QueryString=query,
                WorkGroup=self.config.workgroup,
                ResultConfiguration={
                    'OutputLocation': self.config.output_location,
                    'EncryptionConfiguration': {
                        'EncryptionOption': 'SSE_S3'
                    }
                }
            )
            
            query_execution_id = response['QueryExecutionId']
            logger.info(f"Started query execution: {query_execution_id}")
            
            # Wait for completion
            execution_result = self._wait_for_query_completion(query_execution_id)
            
            return {
                'query_execution_id': query_execution_id,
                'status': execution_result['Status']['State'],
                'data_scanned_bytes': execution_result.get('Statistics', {}).get('DataScannedInBytes', 0),
                'execution_time_ms': execution_result.get('Statistics', {}).get('TotalExecutionTimeInMillis', 0),
                'output_location': execution_result.get('ResultConfiguration', {}).get('OutputLocation', '')
            }
            
        except Exception as e:
            logger.error(f"Failed to execute query {query_name}: {e}")
            raise

class BronzeToSilverTransformer:
    """Handles transformation of data from BRONZE to SILVER layer"""
    
    def __init__(self, config: CTASConfig):
        self.config = config
        self.query_executor = AthenaQueryExecutor(config)
    
    def create_companies_silver_table(self) -> Dict[str, Any]:
        """Create silver companies table from bronze data"""
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.config.database}.companies
        WITH (
            format = 'PARQUET',
            parquet_compression = 'SNAPPY',
            external_location = 's3://{self.config.output_location}/silver/companies/',
            partitioned_by = ARRAY['year', 'month', 'day']
        )
        AS
        SELECT 
            id,
            name,
            website_url,
            linkedin_url,
            industry,
            employee_count,
            annual_revenue,
            founded_year,
            headquarters_address,
            headquarters_city,
            headquarters_state,
            headquarters_country,
            headquarters_postal_code,
            phone,
            description,
            keywords,
            technologies,
            social_media_links,
            created_at,
            updated_at,
            year(created_at) as year,
            month(created_at) as month,
            day(created_at) as day
        FROM (
            SELECT 
                CAST(JSON_EXTRACT_SCALAR(data, '$.id') AS VARCHAR) as id,
                CAST(JSON_EXTRACT_SCALAR(data, '$.name') AS VARCHAR) as name,
                CAST(JSON_EXTRACT_SCALAR(data, '$.website_url') AS VARCHAR) as website_url,
                CAST(JSON_EXTRACT_SCALAR(data, '$.linkedin_url') AS VARCHAR) as linkedin_url,
                CAST(JSON_EXTRACT_SCALAR(data, '$.industry') AS VARCHAR) as industry,
                CAST(JSON_EXTRACT_SCALAR(data, '$.employee_count') AS INTEGER) as employee_count,
                CAST(JSON_EXTRACT_SCALAR(data, '$.annual_revenue') AS BIGINT) as annual_revenue,
                CAST(JSON_EXTRACT_SCALAR(data, '$.founded_year') AS INTEGER) as founded_year,
                CAST(JSON_EXTRACT_SCALAR(data, '$.headquarters_address') AS VARCHAR) as headquarters_address,
                CAST(JSON_EXTRACT_SCALAR(data, '$.headquarters_city') AS VARCHAR) as headquarters_city,
                CAST(JSON_EXTRACT_SCALAR(data, '$.headquarters_state') AS VARCHAR) as headquarters_state,
                CAST(JSON_EXTRACT_SCALAR(data, '$.headquarters_country') AS VARCHAR) as headquarters_country,
                CAST(JSON_EXTRACT_SCALAR(data, '$.headquarters_postal_code') AS VARCHAR) as headquarters_postal_code,
                CAST(JSON_EXTRACT_SCALAR(data, '$.phone') AS VARCHAR) as phone,
                CAST(JSON_EXTRACT_SCALAR(data, '$.description') AS VARCHAR) as description,
                CAST(JSON_EXTRACT_SCALAR(data, '$.keywords') AS VARCHAR) as keywords,
                CAST(JSON_EXTRACT_SCALAR(data, '$.technologies') AS VARCHAR) as technologies,
                CAST(JSON_EXTRACT_SCALAR(data, '$.social_media_links') AS VARCHAR) as social_media_links,
                CAST(JSON_EXTRACT_SCALAR(data, '$.created_at') AS TIMESTAMP) as created_at,
                CAST(JSON_EXTRACT_SCALAR(data, '$.updated_at') AS TIMESTAMP) as updated_at
            FROM thera_bronze.apollo_companies
            WHERE year = year(current_date)
            AND month = month(current_date)
            AND day = day(current_date)
        )
        WHERE id IS NOT NULL
        """
        
        return self.query_executor.execute_query(query, "create_companies_silver_table")
    
    def create_apollo_companies_silver_table(self) -> Dict[str, Any]:
        """Create silver apollo companies table from bronze data"""
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.config.database}.apollo_companies
        WITH (
            format = 'PARQUET',
            parquet_compression = 'SNAPPY',
            external_location = 's3://{self.config.output_location}/silver/apollo_companies/',
            partitioned_by = ARRAY['year', 'month', 'day']
        )
        AS
        SELECT 
            id,
            name,
            website_url,
            linkedin_url,
            industry,
            employee_count,
            annual_revenue,
            founded_year,
            headquarters_address,
            headquarters_city,
            headquarters_state,
            headquarters_country,
            headquarters_postal_code,
            phone,
            description,
            keywords,
            technologies,
            social_media_links,
            apollo_id,
            apollo_url,
            apollo_created_at,
            apollo_updated_at,
            created_at,
            updated_at,
            year(created_at) as year,
            month(created_at) as month,
            day(created_at) as day
        FROM (
            SELECT 
                CAST(JSON_EXTRACT_SCALAR(data, '$.id') AS VARCHAR) as id,
                CAST(JSON_EXTRACT_SCALAR(data, '$.name') AS VARCHAR) as name,
                CAST(JSON_EXTRACT_SCALAR(data, '$.website_url') AS VARCHAR) as website_url,
                CAST(JSON_EXTRACT_SCALAR(data, '$.linkedin_url') AS VARCHAR) as linkedin_url,
                CAST(JSON_EXTRACT_SCALAR(data, '$.industry') AS VARCHAR) as industry,
                CAST(JSON_EXTRACT_SCALAR(data, '$.employee_count') AS INTEGER) as employee_count,
                CAST(JSON_EXTRACT_SCALAR(data, '$.annual_revenue') AS BIGINT) as annual_revenue,
                CAST(JSON_EXTRACT_SCALAR(data, '$.founded_year') AS INTEGER) as founded_year,
                CAST(JSON_EXTRACT_SCALAR(data, '$.headquarters_address') AS VARCHAR) as headquarters_address,
                CAST(JSON_EXTRACT_SCALAR(data, '$.headquarters_city') AS VARCHAR) as headquarters_city,
                CAST(JSON_EXTRACT_SCALAR(data, '$.headquarters_state') AS VARCHAR) as headquarters_state,
                CAST(JSON_EXTRACT_SCALAR(data, '$.headquarters_country') AS VARCHAR) as headquarters_country,
                CAST(JSON_EXTRACT_SCALAR(data, '$.headquarters_postal_code') AS VARCHAR) as headquarters_postal_code,
                CAST(JSON_EXTRACT_SCALAR(data, '$.phone') AS VARCHAR) as phone,
                CAST(JSON_EXTRACT_SCALAR(data, '$.description') AS VARCHAR) as description,
                CAST(JSON_EXTRACT_SCALAR(data, '$.keywords') AS VARCHAR) as keywords,
                CAST(JSON_EXTRACT_SCALAR(data, '$.technologies') AS VARCHAR) as technologies,
                CAST(JSON_EXTRACT_SCALAR(data, '$.social_media_links') AS VARCHAR) as social_media_links,
                CAST(JSON_EXTRACT_SCALAR(data, '$.apollo_id') AS VARCHAR) as apollo_id,
                CAST(JSON_EXTRACT_SCALAR(data, '$.apollo_url') AS VARCHAR) as apollo_url,
                CAST(JSON_EXTRACT_SCALAR(data, '$.apollo_created_at') AS TIMESTAMP) as apollo_created_at,
                CAST(JSON_EXTRACT_SCALAR(data, '$.apollo_updated_at') AS TIMESTAMP) as apollo_updated_at,
                CAST(JSON_EXTRACT_SCALAR(data, '$.created_at') AS TIMESTAMP) as created_at,
                CAST(JSON_EXTRACT_SCALAR(data, '$.updated_at') AS TIMESTAMP) as updated_at
            FROM thera_bronze.apollo_companies
            WHERE year = year(current_date)
            AND month = month(current_date)
            AND day = day(current_date)
        )
        WHERE id IS NOT NULL
        """
        
        return self.query_executor.execute_query(query, "create_apollo_companies_silver_table")
    
    def create_apollo_contacts_silver_table(self) -> Dict[str, Any]:
        """Create silver apollo contacts table from bronze data"""
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.config.database}.apollo_contacts
        WITH (
            format = 'PARQUET',
            parquet_compression = 'SNAPPY',
            external_location = 's3://{self.config.output_location}/silver/apollo_contacts/',
            partitioned_by = ARRAY['year', 'month', 'day']
        )
        AS
        SELECT 
            id,
            first_name,
            last_name,
            full_name,
            email,
            phone,
            title,
            department,
            seniority,
            linkedin_url,
            twitter_url,
            facebook_url,
            github_url,
            apollo_id,
            apollo_url,
            apollo_created_at,
            apollo_updated_at,
            organization_id,
            organization_name,
            organization_website_url,
            organization_linkedin_url,
            organization_industry,
            organization_employee_count,
            organization_annual_revenue,
            organization_founded_year,
            organization_headquarters_address,
            organization_headquarters_city,
            organization_headquarters_state,
            organization_headquarters_country,
            organization_headquarters_postal_code,
            created_at,
            updated_at,
            year(created_at) as year,
            month(created_at) as month,
            day(created_at) as day
        FROM (
            SELECT 
                CAST(JSON_EXTRACT_SCALAR(data, '$.id') AS VARCHAR) as id,
                CAST(JSON_EXTRACT_SCALAR(data, '$.first_name') AS VARCHAR) as first_name,
                CAST(JSON_EXTRACT_SCALAR(data, '$.last_name') AS VARCHAR) as last_name,
                CAST(JSON_EXTRACT_SCALAR(data, '$.full_name') AS VARCHAR) as full_name,
                CAST(JSON_EXTRACT_SCALAR(data, '$.email') AS VARCHAR) as email,
                CAST(JSON_EXTRACT_SCALAR(data, '$.phone') AS VARCHAR) as phone,
                CAST(JSON_EXTRACT_SCALAR(data, '$.title') AS VARCHAR) as title,
                CAST(JSON_EXTRACT_SCALAR(data, '$.department') AS VARCHAR) as department,
                CAST(JSON_EXTRACT_SCALAR(data, '$.seniority') AS VARCHAR) as seniority,
                CAST(JSON_EXTRACT_SCALAR(data, '$.linkedin_url') AS VARCHAR) as linkedin_url,
                CAST(JSON_EXTRACT_SCALAR(data, '$.twitter_url') AS VARCHAR) as twitter_url,
                CAST(JSON_EXTRACT_SCALAR(data, '$.facebook_url') AS VARCHAR) as facebook_url,
                CAST(JSON_EXTRACT_SCALAR(data, '$.github_url') AS VARCHAR) as github_url,
                CAST(JSON_EXTRACT_SCALAR(data, '$.apollo_id') AS VARCHAR) as apollo_id,
                CAST(JSON_EXTRACT_SCALAR(data, '$.apollo_url') AS VARCHAR) as apollo_url,
                CAST(JSON_EXTRACT_SCALAR(data, '$.apollo_created_at') AS TIMESTAMP) as apollo_created_at,
                CAST(JSON_EXTRACT_SCALAR(data, '$.apollo_updated_at') AS TIMESTAMP) as apollo_updated_at,
                CAST(JSON_EXTRACT_SCALAR(data, '$.organization_id') AS VARCHAR) as organization_id,
                CAST(JSON_EXTRACT_SCALAR(data, '$.organization_name') AS VARCHAR) as organization_name,
                CAST(JSON_EXTRACT_SCALAR(data, '$.organization_website_url') AS VARCHAR) as organization_website_url,
                CAST(JSON_EXTRACT_SCALAR(data, '$.organization_linkedin_url') AS VARCHAR) as organization_linkedin_url,
                CAST(JSON_EXTRACT_SCALAR(data, '$.organization_industry') AS VARCHAR) as organization_industry,
                CAST(JSON_EXTRACT_SCALAR(data, '$.organization_employee_count') AS INTEGER) as organization_employee_count,
                CAST(JSON_EXTRACT_SCALAR(data, '$.organization_annual_revenue') AS BIGINT) as organization_annual_revenue,
                CAST(JSON_EXTRACT_SCALAR(data, '$.organization_founded_year') AS INTEGER) as organization_founded_year,
                CAST(JSON_EXTRACT_SCALAR(data, '$.organization_headquarters_address') AS VARCHAR) as organization_headquarters_address,
                CAST(JSON_EXTRACT_SCALAR(data, '$.organization_headquarters_city') AS VARCHAR) as organization_headquarters_city,
                CAST(JSON_EXTRACT_SCALAR(data, '$.organization_headquarters_state') AS VARCHAR) as organization_headquarters_state,
                CAST(JSON_EXTRACT_SCALAR(data, '$.organization_headquarters_country') AS VARCHAR) as organization_headquarters_country,
                CAST(JSON_EXTRACT_SCALAR(data, '$.organization_headquarters_postal_code') AS VARCHAR) as organization_headquarters_postal_code,
                CAST(JSON_EXTRACT_SCALAR(data, '$.created_at') AS TIMESTAMP) as created_at,
                CAST(JSON_EXTRACT_SCALAR(data, '$.updated_at') AS TIMESTAMP) as updated_at
            FROM thera_bronze.apollo_contacts
            WHERE year = year(current_date)
            AND month = month(current_date)
            AND day = day(current_date)
        )
        WHERE id IS NOT NULL
        """
        
        return self.query_executor.execute_query(query, "create_apollo_contacts_silver_table")
    
    def update_table_partitions(self, table_name: str) -> Dict[str, Any]:
        """Update table partitions after CTAS operation"""
        try:
            response = glue_client.batch_create_partition(
                DatabaseName=self.config.database,
                TableName=table_name,
                PartitionInputList=[
                    {
                        'Values': [
                            str(datetime.now().year),
                            str(datetime.now().month),
                            str(datetime.now().day)
                        ],
                        'StorageDescriptor': {
                            'Location': f"s3://{self.config.output_location}/silver/{table_name}/year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}/"
                        }
                    }
                ]
            )
            
            logger.info(f"Updated partitions for table {table_name}")
            return {'success': True, 'response': response}
            
        except Exception as e:
            logger.warning(f"Failed to update partitions for table {table_name}: {e}")
            return {'success': False, 'error': str(e)}

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for Athena CTAS BRONZE→SILVER transformation
    
    Expected environment variables:
    - ATHENA_WORKGROUP: Athena workgroup name
    - S3_OUTPUT_LOCATION: S3 location for Athena query results
    - SILVER_DATABASE: Name of the silver database
    """
    logger.info(f"Starting Athena CTAS BRONZE→SILVER transformation with event: {json.dumps(event)}")
    
    try:
        # Initialize configuration
        config = CTASConfig(
            workgroup=os.environ.get('ATHENA_WORKGROUP', 'primary'),
            output_location=os.environ['S3_OUTPUT_LOCATION'],
            database=os.environ.get('SILVER_DATABASE', 'thera_silver'),
            timeout_minutes=int(os.environ.get('TIMEOUT_MINUTES', '30'))
        )
        
        # Initialize transformer
        transformer = BronzeToSilverTransformer(config)
        
        results = {
            'companies': None,
            'apollo_companies': None,
            'apollo_contacts': None,
            'metadata': {
                'start_time': datetime.utcnow().isoformat(),
                'success': True
            }
        }
        
        # Transform companies table
        try:
            logger.info("Creating silver companies table")
            companies_result = transformer.create_companies_silver_table()
            results['companies'] = companies_result
            logger.info(f"Successfully created companies table: {companies_result['query_execution_id']}")
            
        except Exception as e:
            logger.error(f"Failed to create companies table: {e}")
            results['metadata']['companies_error'] = str(e)
        
        # Transform apollo companies table
        try:
            logger.info("Creating silver apollo companies table")
            apollo_companies_result = transformer.create_apollo_companies_silver_table()
            results['apollo_companies'] = apollo_companies_result
            logger.info(f"Successfully created apollo companies table: {apollo_companies_result['query_execution_id']}")
            
        except Exception as e:
            logger.error(f"Failed to create apollo companies table: {e}")
            results['metadata']['apollo_companies_error'] = str(e)
        
        # Transform apollo contacts table
        try:
            logger.info("Creating silver apollo contacts table")
            apollo_contacts_result = transformer.create_apollo_contacts_silver_table()
            results['apollo_contacts'] = apollo_contacts_result
            logger.info(f"Successfully created apollo contacts table: {apollo_contacts_result['query_execution_id']}")
            
        except Exception as e:
            logger.error(f"Failed to create apollo contacts table: {e}")
            results['metadata']['apollo_contacts_error'] = str(e)
        
        # Update table partitions
        for table_name in ['companies', 'apollo_companies', 'apollo_contacts']:
            if results.get(table_name):
                partition_result = transformer.update_table_partitions(table_name)
                results[table_name]['partition_update'] = partition_result
        
        # Determine overall success
        results['metadata']['success'] = any([
            results.get('companies'),
            results.get('apollo_companies'),
            results.get('apollo_contacts')
        ])
        
        results['metadata']['end_time'] = datetime.utcnow().isoformat()
        
        logger.info(f"Athena CTAS BRONZE→SILVER transformation completed: {json.dumps(results, indent=2)}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(results),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
        
    except Exception as e:
        logger.error(f"Athena CTAS BRONZE→SILVER transformation failed: {e}")
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

# Required imports for the Lambda function
import os
