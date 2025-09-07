import json
import boto3
import time
import hashlib
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
    silver_database: str = 'thera_silver'
    gold_database: str = 'thera_gold'
    timeout_minutes: int = 45

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
                    time.sleep(15)  # Wait 15 seconds before checking again
                    
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

class SilverGoldTransformer:
    """Handles transformation of data from SILVER to GOLD layer"""
    
    def __init__(self, config: CTASConfig):
        self.config = config
        self.query_executor = AthenaQueryExecutor(config)
    
    def create_silver_web_extracts_table(self) -> Dict[str, Any]:
        """Create silver web extracts table from Firecrawl data"""
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.config.silver_database}.web_extracts
        WITH (
            format = 'PARQUET',
            parquet_compression = 'SNAPPY',
            external_location = 's3://{self.config.output_location}/silver/web_extracts/',
            partitioned_by = ARRAY['year', 'month', 'day']
        )
        AS
        SELECT 
            c.id as company_id,
            c.name as company_name,
            c.website_url,
            c.industry,
            c.employee_count,
            c.annual_revenue,
            c.founded_year,
            c.headquarters_city,
            c.headquarters_state,
            c.headquarters_country,
            c.description as company_description,
            c.technologies,
            c.keywords,
            we.pages_crawled,
            we.crawled_at,
            we.content_hash,
            we.pages_data,
            we.extracted_content,
            we.crawl_metadata,
            we.last_crawled,
            year(we.crawled_at) as year,
            month(we.crawled_at) as month,
            day(we.crawled_at) as day
        FROM (
            SELECT 
                CAST(JSON_EXTRACT_SCALAR(data, '$.metadata.company_id') AS VARCHAR) as company_id,
                CAST(JSON_EXTRACT_SCALAR(data, '$.metadata.domain') AS VARCHAR) as domain,
                CAST(JSON_EXTRACT_SCALAR(data, '$.metadata.pages_crawled') AS INTEGER) as pages_crawled,
                CAST(JSON_EXTRACT_SCALAR(data, '$.metadata.crawled_at') AS TIMESTAMP) as crawled_at,
                CAST(JSON_EXTRACT_SCALAR(data, '$.metadata.content_hash') AS VARCHAR) as content_hash,
                CAST(JSON_EXTRACT_SCALAR(data, '$.extracted_data.pages') AS ARRAY<JSON>) as pages_data,
                CAST(JSON_EXTRACT_SCALAR(data, '$.extracted_data.extracted_content') AS JSON) as extracted_content,
                CAST(JSON_EXTRACT_SCALAR(data, '$.extracted_data.crawl_metadata') AS JSON) as crawl_metadata,
                CAST(JSON_EXTRACT_SCALAR(data, '$.metadata.crawled_at') AS TIMESTAMP) as last_crawled
            FROM (
                SELECT data
                FROM thera_bronze.firecrawl_data
                WHERE year = year(current_date)
                AND month = month(current_date)
                AND day = day(current_date)
            )
        ) we
        INNER JOIN thera_silver.companies c ON we.company_id = c.id
        WHERE we.company_id IS NOT NULL
        AND we.domain IS NOT NULL
        """
        
        return self.query_executor.execute_query(query, "create_silver_web_extracts_table")
    
    def create_gold_startup_profiles_table(self) -> Dict[str, Any]:
        """Create gold startup profiles table with enriched data"""
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.config.gold_database}.startup_profiles
        WITH (
            format = 'PARQUET',
            parquet_compression = 'SNAPPY',
            external_location = 's3://{self.config.output_location}/gold/startup_profiles/',
            partitioned_by = ARRAY['year', 'month', 'day']
        )
        AS
        SELECT 
            c.id as company_id,
            c.name as company_name,
            c.website_url,
            c.industry,
            c.employee_count,
            c.annual_revenue,
            c.founded_year,
            c.headquarters_city,
            c.headquarters_state,
            c.headquarters_country,
            c.description as company_description,
            c.technologies,
            c.keywords,
            
            -- Web extraction data
            we.pages_crawled,
            we.crawled_at as last_web_crawl,
            we.content_hash as web_content_hash,
            we.extracted_content,
            
            -- Apollo data
            ac.apollo_id,
            ac.apollo_url,
            ac.apollo_created_at,
            ac.apollo_updated_at,
            
            -- Contact data aggregation
            contact_data.total_contacts,
            contact_data.senior_contacts,
            contact_data.contact_titles,
            contact_data.contact_departments,
            
            -- Domain health data
            dh.domain_health_score,
            dh.ssl_valid,
            dh.dns_resolution,
            dh.last_health_check,
            
            -- Computed fields
            CASE 
                WHEN c.employee_count BETWEEN 1 AND 10 THEN 'Startup'
                WHEN c.employee_count BETWEEN 11 AND 50 THEN 'Small'
                WHEN c.employee_count BETWEEN 51 AND 200 THEN 'Medium'
                WHEN c.employee_count > 200 THEN 'Large'
                ELSE 'Unknown'
            END as company_size_category,
            
            CASE 
                WHEN c.annual_revenue < 1000000 THEN 'Early Stage'
                WHEN c.annual_revenue BETWEEN 1000000 AND 10000000 THEN 'Growth Stage'
                WHEN c.annual_revenue > 10000000 THEN 'Mature'
                ELSE 'Unknown'
            END as revenue_stage,
            
            CASE 
                WHEN c.founded_year IS NOT NULL THEN 
                    year(current_date) - c.founded_year
                ELSE NULL
            END as company_age_years,
            
            -- Profile text for embeddings
            CONCAT(
                COALESCE(c.description, ''),
                ' ',
                COALESCE(c.keywords, ''),
                ' ',
                COALESCE(c.technologies, ''),
                ' ',
                COALESCE(we.extracted_content, '')
            ) as profile_text,
            
            -- Metadata
            current_timestamp as profile_created_at,
            current_timestamp as profile_updated_at,
            year(current_date) as year,
            month(current_date) as month,
            day(current_date) as day
            
        FROM thera_silver.companies c
        
        -- Web extracts
        LEFT JOIN thera_silver.web_extracts we ON c.id = we.company_id
        
        -- Apollo companies
        LEFT JOIN thera_silver.apollo_companies ac ON c.id = ac.company_id
        
        -- Contact aggregation
        LEFT JOIN (
            SELECT 
                organization_id,
                COUNT(*) as total_contacts,
                COUNT(CASE WHEN seniority IN ('C-Level', 'VP', 'Director') THEN 1 END) as senior_contacts,
                ARRAY_AGG(DISTINCT title) as contact_titles,
                ARRAY_AGG(DISTINCT department) as contact_departments
            FROM thera_silver.apollo_contacts
            WHERE organization_id IS NOT NULL
            GROUP BY organization_id
        ) contact_data ON c.id = contact_data.organization_id
        
        -- Domain health
        LEFT JOIN thera_silver.domain_health dh ON c.website_url = dh.domain
        
        WHERE c.id IS NOT NULL
        AND c.name IS NOT NULL
        """
        
        return self.query_executor.execute_query(query, "create_gold_startup_profiles_table")
    
    def create_gold_company_analytics_table(self) -> Dict[str, Any]:
        """Create gold company analytics table with aggregated metrics"""
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.config.gold_database}.company_analytics
        WITH (
            format = 'PARQUET',
            parquet_compression = 'SNAPPY',
            external_location = 's3://{self.config.output_location}/gold/company_analytics/',
            partitioned_by = ARRAY['year', 'month', 'day']
        )
        AS
        SELECT 
            sp.company_id,
            sp.company_name,
            sp.industry,
            sp.company_size_category,
            sp.revenue_stage,
            sp.company_age_years,
            sp.domain_health_score,
            sp.total_contacts,
            sp.senior_contacts,
            sp.pages_crawled,
            
            -- Industry rankings
            ROW_NUMBER() OVER (PARTITION BY sp.industry ORDER BY sp.annual_revenue DESC) as industry_revenue_rank,
            ROW_NUMBER() OVER (PARTITION BY sp.industry ORDER BY sp.employee_count DESC) as industry_employee_rank,
            
            -- Health score percentiles
            PERCENT_RANK() OVER (ORDER BY sp.domain_health_score) as health_score_percentile,
            
            -- Contact density metrics
            CASE 
                WHEN sp.employee_count > 0 THEN sp.total_contacts / sp.employee_count
                ELSE 0
            END as contact_density,
            
            CASE 
                WHEN sp.total_contacts > 0 THEN sp.senior_contacts / sp.total_contacts
                ELSE 0
            END as senior_contact_ratio,
            
            -- Web presence metrics
            CASE 
                WHEN sp.pages_crawled > 0 THEN 1
                ELSE 0
            END as has_web_presence,
            
            CASE 
                WHEN sp.web_content_hash IS NOT NULL THEN 1
                ELSE 0
            END as has_extracted_content,
            
            -- Technology stack analysis
            CASE 
                WHEN sp.technologies LIKE '%AI%' OR sp.technologies LIKE '%Machine Learning%' THEN 1
                ELSE 0
            END as is_ai_company,
            
            CASE 
                WHEN sp.technologies LIKE '%Cloud%' OR sp.technologies LIKE '%AWS%' OR sp.technologies LIKE '%Azure%' THEN 1
                ELSE 0
            END as is_cloud_company,
            
            -- Metadata
            current_timestamp as analytics_created_at,
            year(current_date) as year,
            month(current_date) as month,
            day(current_date) as day
            
        FROM thera_gold.startup_profiles sp
        WHERE sp.company_id IS NOT NULL
        """
        
        return self.query_executor.execute_query(query, "create_gold_company_analytics_table")
    
    def update_table_partitions(self, database: str, table_name: str) -> Dict[str, Any]:
        """Update table partitions after CTAS operation"""
        try:
            response = glue_client.batch_create_partition(
                DatabaseName=database,
                TableName=table_name,
                PartitionInputList=[
                    {
                        'Values': [
                            str(datetime.now().year),
                            str(datetime.now().month),
                            str(datetime.now().day)
                        ],
                        'StorageDescriptor': {
                            'Location': f"s3://{self.config.output_location}/{database.split('_')[1]}/{table_name}/year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}/"
                        }
                    }
                ]
            )
            
            logger.info(f"Updated partitions for table {database}.{table_name}")
            return {'success': True, 'response': response}
            
        except Exception as e:
            logger.warning(f"Failed to update partitions for table {database}.{table_name}: {e}")
            return {'success': False, 'error': str(e)}
    
    def create_profile_text_hash(self, profile_text: str) -> str:
        """Create hash for profile text to detect changes"""
        return hashlib.md5(profile_text.encode()).hexdigest()

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for Athena CTAS SILVER→GOLD transformation
    
    Expected environment variables:
    - ATHENA_WORKGROUP: Athena workgroup name
    - S3_OUTPUT_LOCATION: S3 location for Athena query results
    - SILVER_DATABASE: Name of the silver database
    - GOLD_DATABASE: Name of the gold database
    - TIMEOUT_MINUTES: Query timeout in minutes
    """
    logger.info(f"Starting Athena CTAS SILVER→GOLD transformation with event: {json.dumps(event)}")
    
    try:
        # Initialize configuration
        config = CTASConfig(
            workgroup=os.environ.get('ATHENA_WORKGROUP', 'primary'),
            output_location=os.environ['S3_OUTPUT_LOCATION'],
            silver_database=os.environ.get('SILVER_DATABASE', 'thera_silver'),
            gold_database=os.environ.get('GOLD_DATABASE', 'thera_gold'),
            timeout_minutes=int(os.environ.get('TIMEOUT_MINUTES', '45'))
        )
        
        # Initialize transformer
        transformer = SilverGoldTransformer(config)
        
        results = {
            'silver_web_extracts': None,
            'gold_startup_profiles': None,
            'gold_company_analytics': None,
            'metadata': {
                'start_time': datetime.utcnow().isoformat(),
                'success': True
            }
        }
        
        # Create silver web extracts table
        try:
            logger.info("Creating silver web extracts table")
            web_extracts_result = transformer.create_silver_web_extracts_table()
            results['silver_web_extracts'] = web_extracts_result
            logger.info(f"Successfully created web extracts table: {web_extracts_result['query_execution_id']}")
            
        except Exception as e:
            logger.error(f"Failed to create web extracts table: {e}")
            results['metadata']['web_extracts_error'] = str(e)
        
        # Create gold startup profiles table
        try:
            logger.info("Creating gold startup profiles table")
            startup_profiles_result = transformer.create_gold_startup_profiles_table()
            results['gold_startup_profiles'] = startup_profiles_result
            logger.info(f"Successfully created startup profiles table: {startup_profiles_result['query_execution_id']}")
            
        except Exception as e:
            logger.error(f"Failed to create startup profiles table: {e}")
            results['metadata']['startup_profiles_error'] = str(e)
        
        # Create gold company analytics table
        try:
            logger.info("Creating gold company analytics table")
            analytics_result = transformer.create_gold_company_analytics_table()
            results['gold_company_analytics'] = analytics_result
            logger.info(f"Successfully created company analytics table: {analytics_result['query_execution_id']}")
            
        except Exception as e:
            logger.error(f"Failed to create company analytics table: {e}")
            results['metadata']['company_analytics_error'] = str(e)
        
        # Update table partitions
        for table_info in [
            (config.silver_database, 'web_extracts'),
            (config.gold_database, 'startup_profiles'),
            (config.gold_database, 'company_analytics')
        ]:
            if results.get(f"{table_info[0].split('_')[1]}_{table_info[1]}"):
                partition_result = transformer.update_table_partitions(table_info[0], table_info[1])
                results[f"{table_info[0].split('_')[1]}_{table_info[1]}"]['partition_update'] = partition_result
        
        # Determine overall success
        results['metadata']['success'] = any([
            results.get('silver_web_extracts'),
            results.get('gold_startup_profiles'),
            results.get('gold_company_analytics')
        ])
        
        results['metadata']['end_time'] = datetime.utcnow().isoformat()
        
        logger.info(f"Athena CTAS SILVER→GOLD transformation completed: {json.dumps(results, indent=2)}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(results),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
        
    except Exception as e:
        logger.error(f"Athena CTAS SILVER→GOLD transformation failed: {e}")
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
