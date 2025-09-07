import json
import boto3
import os
import asyncio
import aiohttp
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging
from urllib.parse import urlparse
import hashlib
import base64

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
athena_client = boto3.client('athena')
secrets_client = boto3.client('secretsmanager')
cloudwatch = boto3.client('cloudwatch')
stepfunctions = boto3.client('stepfunctions')
sqs = boto3.client('sqs')

# Environment variables
RAW_BUCKET = os.environ['RAW_BUCKET']
CURATED_BUCKET = os.environ['CURATED_BUCKET']
ATHENA_DATABASE = os.environ['ATHENA_DATABASE']
ATHENA_WORKGROUP = os.environ.get('ATHENA_WORKGROUP', 'primary')
FIRECRAWL_SECRET_NAME = os.environ['FIRECRAWL_SECRET_NAME']
MAX_CONCURRENCY = int(os.environ.get('MAX_CONCURRENCY', '2'))
STEP_FUNCTION_ARN = os.environ.get('STEP_FUNCTION_ARN')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
CIRCUIT_BREAKER_THRESHOLD = int(os.environ.get('CIRCUIT_BREAKER_THRESHOLD', '5'))
CIRCUIT_BREAKER_TIMEOUT = int(os.environ.get('CIRCUIT_BREAKER_TIMEOUT', '300'))  # 5 minutes

class CircuitBreaker:
    """Circuit breaker for handling sustained failures"""
    
    def __init__(self, threshold: int = 5, timeout: int = 300):
        self.threshold = threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
    
    def can_execute(self) -> bool:
        """Check if we can execute the operation"""
        if self.state == 'CLOSED':
            return True
        elif self.state == 'OPEN':
            if time.time() - self.last_failure_time > self.timeout:
                self.state = 'HALF_OPEN'
                return True
            return False
        else:  # HALF_OPEN
            return True
    
    def record_success(self):
        """Record successful operation"""
        self.failure_count = 0
        self.state = 'CLOSED'
    
    def record_failure(self):
        """Record failed operation"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.threshold:
            self.state = 'OPEN'
            logger.warning(f"Circuit breaker opened after {self.failure_count} failures")

class FirecrawlClient:
    """Firecrawl API client with rate limiting and error handling"""
    
    def __init__(self, api_key: str, max_concurrency: int = 2):
        self.api_key = api_key
        self.max_concurrency = max_concurrency
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.circuit_breaker = CircuitBreaker(CIRCUIT_BREAKER_THRESHOLD, CIRCUIT_BREAKER_TIMEOUT)
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/json'
            }
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def crawl_domain(self, domain: str, max_depth: int = 1, max_pages: int = 3, 
                          timeout_ms: int = 30000, only_main_content: bool = True) -> Dict:
        """Crawl a domain using Firecrawl API"""
        if not self.circuit_breaker.can_execute():
            raise Exception("Circuit breaker is open - too many failures")
        
        async with self.semaphore:
            try:
                url = f"https://api.firecrawl.dev/v1/scrape"
                
                payload = {
                    "url": f"https://{domain}",
                    "formats": ["markdown", "html"],
                    "onlyMainContent": only_main_content,
                    "maxDepth": max_depth,
                    "maxPages": max_pages,
                    "timeoutMs": timeout_ms,
                    "waitFor": 2000,  # Wait 2 seconds for dynamic content
                    "screenshot": False,
                    "pdf": False
                }
                
                async with self.session.post(url, json=payload) as response:
                    if response.status == 429:
                        # Rate limited - wait and retry
                        retry_after = int(response.headers.get('Retry-After', 60))
                        logger.warning(f"Rate limited, waiting {retry_after} seconds")
                        await asyncio.sleep(retry_after)
                        return await self.crawl_domain(domain, max_depth, max_pages, timeout_ms, only_main_content)
                    
                    response.raise_for_status()
                    result = await response.json()
                    
                    # Record success
                    self.circuit_breaker.record_success()
                    
                    return {
                        'domain': domain,
                        'timestamp': datetime.utcnow().isoformat(),
                        'success': True,
                        'data': result,
                        'content_hash': self._calculate_content_hash(result),
                        'pages_crawled': len(result.get('data', {}).get('pages', [])),
                        'total_links': self._count_links(result),
                        'response_time_ms': result.get('responseTime', 0)
                    }
                    
            except aiohttp.ClientError as e:
                logger.error(f"HTTP error crawling {domain}: {e}")
                self.circuit_breaker.record_failure()
                return {
                    'domain': domain,
                    'timestamp': datetime.utcnow().isoformat(),
                    'success': False,
                    'error': str(e),
                    'error_type': 'http_error'
                }
            except Exception as e:
                logger.error(f"Error crawling {domain}: {e}")
                self.circuit_breaker.record_failure()
                return {
                    'domain': domain,
                    'timestamp': datetime.utcnow().isoformat(),
                    'success': False,
                    'error': str(e),
                    'error_type': 'general_error'
                }
    
    def _calculate_content_hash(self, data: Dict) -> str:
        """Calculate content hash for deduplication"""
        try:
            content = ""
            if 'data' in data and 'pages' in data['data']:
                for page in data['data']['pages']:
                    content += page.get('markdown', '') + page.get('html', '')
            
            return hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]
        except Exception:
            return ""
    
    def _count_links(self, data: Dict) -> int:
        """Count total links found across all pages"""
        try:
            total_links = 0
            if 'data' in data and 'pages' in data['data']:
                for page in data['data']['pages']:
                    total_links += len(page.get('links', []))
            return total_links
        except Exception:
            return 0

class DomainSourceProcessor:
    """Process domain sources from Athena or S3"""
    
    def __init__(self, athena_database: str, athena_workgroup: str):
        self.athena_database = athena_database
        self.athena_workgroup = athena_workgroup
    
    async def get_domains_from_athena(self, query: str) -> List[str]:
        """Execute Athena query to get domains"""
        try:
            # Start query execution
            response = athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.athena_database},
                WorkGroup=self.athena_workgroup
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
                    raise Exception(f"Query failed: {response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')}")
                
                await asyncio.sleep(2)
            
            # Get query results
            response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
            
            domains = []
            for row in response['ResultSet']['Rows'][1:]:  # Skip header row
                if row['Data']:
                    domain = row['Data'][0].get('VarCharValue', '').strip()
                    if domain:
                        domains.append(domain)
            
            return domains
            
        except Exception as e:
            logger.error(f"Error executing Athena query: {e}")
            raise
    
    async def get_domains_from_s3_manifest(self, manifest_key: str) -> List[str]:
        """Get domains from S3 manifest file"""
        try:
            response = s3_client.get_object(Bucket=CURATED_BUCKET, Key=manifest_key)
            content = response['Body'].read().decode('utf-8')
            
            domains = []
            for line in content.strip().split('\n'):
                domain = line.strip()
                if domain:
                    domains.append(domain)
            
            return domains
            
        except Exception as e:
            logger.error(f"Error reading S3 manifest: {e}")
            raise

class StepFunctionsOrchestrator:
    """Orchestrate Firecrawl using Step Functions"""
    
    def __init__(self, state_machine_arn: str):
        self.state_machine_arn = state_machine_arn
    
    async def start_execution(self, domains: List[str]) -> str:
        """Start Step Functions execution for domain crawling"""
        try:
            # Create input for Step Functions
            input_data = {
                'domains': domains,
                'max_concurrency': MAX_CONCURRENCY,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            response = stepfunctions.start_execution(
                stateMachineArn=self.state_machine_arn,
                name=f"firecrawl-{int(time.time())}",
                input=json.dumps(input_data)
            )
            
            return response['executionArn']
            
        except Exception as e:
            logger.error(f"Error starting Step Functions execution: {e}")
            raise

class SQSOrchestrator:
    """Orchestrate Firecrawl using SQS"""
    
    def __init__(self, queue_url: str):
        self.queue_url = queue_url
    
    async def send_domains_to_queue(self, domains: List[str]) -> int:
        """Send domains to SQS queue for processing"""
        try:
            messages_sent = 0
            
            # Send domains in batches (SQS allows up to 10 messages per batch)
            for i in range(0, len(domains), 10):
                batch = domains[i:i+10]
                
                entries = []
                for j, domain in enumerate(batch):
                    entries.append({
                        'Id': str(i + j),
                        'MessageBody': json.dumps({
                            'domain': domain,
                            'timestamp': datetime.utcnow().isoformat(),
                            'max_depth': 1,
                            'max_pages': 3,
                            'timeout_ms': 30000,
                            'only_main_content': True
                        })
                    })
                
                response = sqs.send_message_batch(
                    QueueUrl=self.queue_url,
                    Entries=entries
                )
                
                messages_sent += len(response.get('Successful', []))
                
                if response.get('Failed'):
                    logger.warning(f"Failed to send {len(response['Failed'])} messages to SQS")
            
            return messages_sent
            
        except Exception as e:
            logger.error(f"Error sending domains to SQS: {e}")
            raise

def store_raw_data(bucket: str, key: str, data: List[Dict]) -> None:
    """Store raw Firecrawl data to S3 as JSONL"""
    try:
        jsonl_content = '\n'.join(json.dumps(item) for item in data)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=jsonl_content.encode('utf-8'),
            ContentType='application/jsonl'
        )
        logger.info(f"Stored {len(data)} Firecrawl records to s3://{bucket}/{key}")
    except Exception as e:
        logger.error(f"Error storing raw data: {e}")
        raise

def normalize_firecrawl_data(data: Dict) -> Dict:
    """Normalize Firecrawl data for silver layer"""
    try:
        normalized = {
            'domain': data.get('domain'),
            'timestamp': data.get('timestamp'),
            'success': data.get('success', False),
            'content_hash': data.get('content_hash', ''),
            'pages_crawled': data.get('pages_crawled', 0),
            'total_links': data.get('total_links', 0),
            'response_time_ms': data.get('response_time_ms', 0),
            'error': data.get('error', ''),
            'error_type': data.get('error_type', ''),
            'content_richness_score': calculate_content_richness(data),
            'raw_data': data
        }
        
        # Extract structured content if available
        if data.get('success') and 'data' in data.get('data', {}):
            firecrawl_data = data['data']['data']
            normalized.update({
                'title': firecrawl_data.get('metadata', {}).get('title', ''),
                'description': firecrawl_data.get('metadata', {}).get('description', ''),
                'language': firecrawl_data.get('metadata', {}).get('language', ''),
                'canonical_url': firecrawl_data.get('metadata', {}).get('canonicalUrl', ''),
                'main_content': firecrawl_data.get('markdown', ''),
                'html_content': firecrawl_data.get('html', ''),
                'links': firecrawl_data.get('links', []),
                'images': firecrawl_data.get('images', [])
            })
        
        return normalized
        
    except Exception as e:
        logger.error(f"Error normalizing Firecrawl data: {e}")
        return data

def calculate_content_richness(data: Dict) -> float:
    """Calculate content richness score (0.0 to 1.0)"""
    try:
        if not data.get('success'):
            return 0.0
        
        score = 0.0
        max_score = 0.0
        
        # Text content (40% weight)
        if 'data' in data.get('data', {}):
            firecrawl_data = data['data']['data']
            content = firecrawl_data.get('markdown', '')
            
            if content:
                # Length score (0-1 based on content length)
                length_score = min(len(content) / 5000, 1.0)  # Max score at 5000 chars
                score += length_score * 0.4
            max_score += 0.4
        
        # Links (20% weight)
        links = firecrawl_data.get('links', [])
        if links:
            link_score = min(len(links) / 50, 1.0)  # Max score at 50 links
            score += link_score * 0.2
        max_score += 0.2
        
        # Images (10% weight)
        images = firecrawl_data.get('images', [])
        if images:
            image_score = min(len(images) / 10, 1.0)  # Max score at 10 images
            score += image_score * 0.1
        max_score += 0.1
        
        # Metadata completeness (30% weight)
        metadata = firecrawl_data.get('metadata', {})
        metadata_fields = ['title', 'description', 'language', 'canonicalUrl']
        metadata_score = sum(1 for field in metadata_fields if metadata.get(field)) / len(metadata_fields)
        score += metadata_score * 0.3
        max_score += 0.3
        
        return score / max_score if max_score > 0 else 0.0
        
    except Exception:
        return 0.0

def emit_cloudwatch_metrics(domains_processed: int, successful_crawls: int, failed_crawls: int) -> None:
    """Emit CloudWatch metrics"""
    try:
        cloudwatch.put_metric_data(
            Namespace='TheraPipeline/Firecrawl',
            MetricData=[
                {
                    'MetricName': 'DomainsProcessed',
                    'Value': domains_processed,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'SuccessfulCrawls',
                    'Value': successful_crawls,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'FailedCrawls',
                    'Value': failed_crawls,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                }
            ]
        )
    except Exception as e:
        logger.error(f"Error emitting metrics: {e}")

async def process_domains_direct(domains: List[str], api_key: str) -> List[Dict]:
    """Process domains directly with Firecrawl API"""
    results = []
    
    async with FirecrawlClient(api_key, MAX_CONCURRENCY) as client:
        tasks = [client.crawl_domain(domain) for domain in domains]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions
        valid_results = []
        for result in results:
            if isinstance(result, dict):
                valid_results.append(result)
            else:
                logger.error(f"Domain crawl failed with exception: {result}")
        
        return valid_results

def lambda_handler(event, context):
    """Main Lambda handler for Firecrawl Orchestrator"""
    try:
        # Get Firecrawl API key
        try:
            response = secrets_client.get_secret_value(SecretId=FIRECRAWL_SECRET_NAME)
            secret = json.loads(response['SecretString'])
            api_key = secret['firecrawl_api_key']
        except Exception as e:
            logger.error(f"Error retrieving Firecrawl API key: {e}")
            raise
        
        # Get domains from event
        domains = []
        
        if 'query' in event:
            # Athena query
            processor = DomainSourceProcessor(ATHENA_DATABASE, ATHENA_WORKGROUP)
            domains = await processor.get_domains_from_athena(event['query'])
        elif 'manifest_key' in event:
            # S3 manifest
            processor = DomainSourceProcessor(ATHENA_DATABASE, ATHENA_WORKGROUP)
            domains = await processor.get_domains_from_s3_manifest(event['manifest_key'])
        elif 'domains' in event:
            # Direct domain list
            domains = event['domains']
        else:
            raise ValueError("Event must contain 'query', 'manifest_key', or 'domains'")
        
        if not domains:
            logger.warning("No domains to crawl")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No domains to crawl',
                    'domains_processed': 0
                })
            }
        
        logger.info(f"Processing {len(domains)} domains")
        
        # Choose orchestration method
        if STEP_FUNCTION_ARN and event.get('use_step_functions', False):
            # Use Step Functions
            orchestrator = StepFunctionsOrchestrator(STEP_FUNCTION_ARN)
            execution_arn = await orchestrator.start_execution(domains)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Step Functions execution started',
                    'execution_arn': execution_arn,
                    'domains_count': len(domains)
                })
            }
        
        elif SQS_QUEUE_URL and event.get('use_sqs', False):
            # Use SQS
            orchestrator = SQSOrchestrator(SQS_QUEUE_URL)
            messages_sent = await orchestrator.send_domains_to_queue(domains)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Domains sent to SQS queue',
                    'messages_sent': messages_sent,
                    'domains_count': len(domains)
                })
            }
        
        else:
            # Direct processing
            results = await process_domains_direct(domains, api_key)
            
            # Store raw results
            current_date = datetime.utcnow().strftime('%Y-%m-%d')
            raw_key = f"firecrawl/date={current_date}/crawls_{int(time.time())}.jsonl"
            store_raw_data(RAW_BUCKET, raw_key, results)
            
            # Store normalized results
            normalized_results = [normalize_firecrawl_data(result) for result in results]
            silver_key = f"silver/firecrawl/date={current_date}/crawls_{int(time.time())}.jsonl"
            store_raw_data(CURATED_BUCKET, silver_key, normalized_results)
            
            # Calculate metrics
            successful_crawls = sum(1 for r in results if r.get('success', False))
            failed_crawls = len(results) - successful_crawls
            
            # Emit metrics
            emit_cloudwatch_metrics(len(results), successful_crawls, failed_crawls)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Firecrawl processing completed successfully',
                    'domains_processed': len(results),
                    'successful_crawls': successful_crawls,
                    'failed_crawls': failed_crawls,
                    'raw_s3_key': raw_key,
                    'silver_s3_key': silver_key
                })
            }
        
    except Exception as e:
        logger.error(f"Error in Firecrawl orchestrator: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

# For local testing
if __name__ == "__main__":
    # Test with sample domains
    test_event = {
        'domains': ['example.com', 'github.com'],
        'use_step_functions': False,
        'use_sqs': False
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))