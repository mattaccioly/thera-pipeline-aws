import json
import boto3
import os
import asyncio
import aiohttp
import ssl
import socket
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging
from urllib.parse import urlparse
import time

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
athena_client = boto3.client('athena')
cloudwatch = boto3.client('cloudwatch')

# Environment variables
CURATED_BUCKET = os.environ['CURATED_BUCKET']
ATHENA_DATABASE = os.environ['ATHENA_DATABASE']
ATHENA_WORKGROUP = os.environ.get('ATHENA_WORKGROUP', 'primary')
MAX_CONCURRENCY = int(os.environ.get('MAX_CONCURRENCY', '10'))
REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', '2'))

class DomainHealthChecker:
    """Async domain health checker with DNS, HEAD, TLS, and GET checks"""
    
    def __init__(self, max_concurrency: int = 10, timeout: int = 2):
        self.max_concurrency = max_concurrency
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(max_concurrency)
        
        # SSL context for TLS checks
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
    
    async def check_domain_health(self, domain: str) -> Dict:
        """Check comprehensive health of a domain"""
        async with self.semaphore:
            health_data = {
                'domain': domain,
                'timestamp': datetime.utcnow().isoformat(),
                'dns_status': 'unknown',
                'http_status': 'unknown',
                'https_status': 'unknown',
                'tls_status': 'unknown',
                'content_status': 'unknown',
                'response_time_ms': 0,
                'content_size_bytes': 0,
                'domain_health_score': 0.0,
                'flags': [],
                'errors': []
            }
            
            try:
                # DNS resolution check
                dns_result = await self._check_dns(domain)
                health_data.update(dns_result)
                
                # HTTP HEAD check
                http_result = await self._check_http_head(domain)
                health_data.update(http_result)
                
                # HTTPS HEAD check
                https_result = await self._check_https_head(domain)
                health_data.update(https_result)
                
                # TLS check
                tls_result = await self._check_tls(domain)
                health_data.update(tls_result)
                
                # Content check (small GET request)
                content_result = await self._check_content(domain)
                health_data.update(content_result)
                
                # Calculate overall health score
                health_data['domain_health_score'] = self._calculate_health_score(health_data)
                
                # Set flags based on results
                health_data['flags'] = self._generate_flags(health_data)
                
            except Exception as e:
                logger.error(f"Error checking domain {domain}: {e}")
                health_data['errors'].append(str(e))
                health_data['domain_health_score'] = 0.0
            
            return health_data
    
    async def _check_dns(self, domain: str) -> Dict:
        """Check DNS resolution"""
        try:
            start_time = time.time()
            result = await asyncio.get_event_loop().run_in_executor(
                None, socket.gethostbyname, domain
            )
            response_time = (time.time() - start_time) * 1000
            
            return {
                'dns_status': 'resolved',
                'dns_ip': result,
                'dns_response_time_ms': response_time
            }
        except socket.gaierror as e:
            return {
                'dns_status': 'failed',
                'dns_error': str(e),
                'dns_response_time_ms': 0
            }
    
    async def _check_http_head(self, domain: str) -> Dict:
        """Check HTTP HEAD request"""
        try:
            url = f"http://{domain}"
            start_time = time.time()
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                async with session.head(url, allow_redirects=True) as response:
                    response_time = (time.time() - start_time) * 1000
                    
                    return {
                        'http_status': 'success',
                        'http_status_code': response.status,
                        'http_response_time_ms': response_time,
                        'http_headers': dict(response.headers)
                    }
        except Exception as e:
            return {
                'http_status': 'failed',
                'http_error': str(e),
                'http_response_time_ms': 0
            }
    
    async def _check_https_head(self, domain: str) -> Dict:
        """Check HTTPS HEAD request"""
        try:
            url = f"https://{domain}"
            start_time = time.time()
            
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                connector=aiohttp.TCPConnector(ssl=False)  # Disable SSL verification for speed
            ) as session:
                async with session.head(url, allow_redirects=True) as response:
                    response_time = (time.time() - start_time) * 1000
                    
                    return {
                        'https_status': 'success',
                        'https_status_code': response.status,
                        'https_response_time_ms': response_time,
                        'https_headers': dict(response.headers)
                    }
        except Exception as e:
            return {
                'https_status': 'failed',
                'https_error': str(e),
                'https_response_time_ms': 0
            }
    
    async def _check_tls(self, domain: str) -> Dict:
        """Check TLS certificate"""
        try:
            start_time = time.time()
            
            # Create SSL socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            
            # Wrap with SSL
            ssl_sock = self.ssl_context.wrap_socket(sock, server_hostname=domain)
            
            # Connect
            await asyncio.get_event_loop().run_in_executor(
                None, ssl_sock.connect, (domain, 443)
            )
            
            response_time = (time.time() - start_time) * 1000
            
            # Get certificate info
            cert = ssl_sock.getpeercert()
            ssl_sock.close()
            
            return {
                'tls_status': 'valid',
                'tls_response_time_ms': response_time,
                'tls_cert_subject': cert.get('subject', []),
                'tls_cert_issuer': cert.get('issuer', []),
                'tls_cert_version': cert.get('version'),
                'tls_cert_serial_number': cert.get('serialNumber')
            }
        except Exception as e:
            return {
                'tls_status': 'failed',
                'tls_error': str(e),
                'tls_response_time_ms': 0
            }
    
    async def _check_content(self, domain: str) -> Dict:
        """Check content with small GET request (≤20KB)"""
        try:
            url = f"https://{domain}"
            start_time = time.time()
            
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                connector=aiohttp.TCPConnector(ssl=False)
            ) as session:
                async with session.get(url, allow_redirects=True) as response:
                    response_time = (time.time() - start_time) * 1000
                    
                    # Read only first 20KB
                    content = await response.read(20480)
                    content_size = len(content)
                    
                    return {
                        'content_status': 'success',
                        'content_size_bytes': content_size,
                        'content_response_time_ms': response_time,
                        'content_type': response.headers.get('content-type', ''),
                        'content_preview': content[:500].decode('utf-8', errors='ignore')
                    }
        except Exception as e:
            return {
                'content_status': 'failed',
                'content_error': str(e),
                'content_size_bytes': 0,
                'content_response_time_ms': 0
            }
    
    def _calculate_health_score(self, health_data: Dict) -> float:
        """Calculate overall domain health score (0.0 to 1.0)"""
        score = 0.0
        max_score = 0.0
        
        # DNS resolution (20% weight)
        if health_data.get('dns_status') == 'resolved':
            score += 0.2
        max_score += 0.2
        
        # HTTP availability (20% weight)
        if health_data.get('http_status') == 'success':
            score += 0.2
        max_score += 0.2
        
        # HTTPS availability (30% weight)
        if health_data.get('https_status') == 'success':
            score += 0.3
        max_score += 0.3
        
        # TLS validity (20% weight)
        if health_data.get('tls_status') == 'valid':
            score += 0.2
        max_score += 0.2
        
        # Content availability (10% weight)
        if health_data.get('content_status') == 'success':
            score += 0.1
        max_score += 0.1
        
        return score / max_score if max_score > 0 else 0.0
    
    def _generate_flags(self, health_data: Dict) -> List[str]:
        """Generate flags based on health check results"""
        flags = []
        
        if health_data.get('dns_status') == 'failed':
            flags.append('dns_failed')
        
        if health_data.get('http_status') == 'failed':
            flags.append('http_failed')
        
        if health_data.get('https_status') == 'failed':
            flags.append('https_failed')
        
        if health_data.get('tls_status') == 'failed':
            flags.append('tls_failed')
        
        if health_data.get('content_status') == 'failed':
            flags.append('content_failed')
        
        if health_data.get('domain_health_score', 0) < 0.5:
            flags.append('low_health_score')
        
        if health_data.get('content_size_bytes', 0) == 0:
            flags.append('no_content')
        
        return flags

class DomainQueryProcessor:
    """Process domain queries from Athena or S3 manifest"""
    
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

def store_health_data(bucket: str, key: str, data: List[Dict]) -> None:
    """Store domain health data to S3 as JSONL"""
    try:
        jsonl_content = '\n'.join(json.dumps(item) for item in data)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=jsonl_content.encode('utf-8'),
            ContentType='application/jsonl'
        )
        logger.info(f"Stored {len(data)} health records to s3://{bucket}/{key}")
    except Exception as e:
        logger.error(f"Error storing health data: {e}")
        raise

def emit_cloudwatch_metrics(domains_checked: int, avg_health_score: float) -> None:
    """Emit CloudWatch metrics"""
    try:
        cloudwatch.put_metric_data(
            Namespace='TheraPipeline/DomainHealth',
            MetricData=[
                {
                    'MetricName': 'DomainsChecked',
                    'Value': domains_checked,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'AverageHealthScore',
                    'Value': avg_health_score,
                    'Unit': 'None',
                    'Timestamp': datetime.utcnow()
                }
            ]
        )
    except Exception as e:
        logger.error(f"Error emitting metrics: {e}")

async def process_domains_async(domains: List[str], max_concurrency: int) -> List[Dict]:
    """Process domains asynchronously with concurrency control"""
    checker = DomainHealthChecker(max_concurrency=max_concurrency, timeout=REQUEST_TIMEOUT)
    
    tasks = [checker.check_domain_health(domain) for domain in domains]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Filter out exceptions and return valid results
    valid_results = []
    for result in results:
        if isinstance(result, dict):
            valid_results.append(result)
        else:
            logger.error(f"Domain check failed with exception: {result}")
    
    return valid_results

def lambda_handler(event, context):
    """Main Lambda handler for Domain Health Gate"""
    try:
        # Get domains from event
        domains = []
        
        if 'query' in event:
            # Athena query
            processor = DomainQueryProcessor(ATHENA_DATABASE, ATHENA_WORKGROUP)
            domains = await processor.get_domains_from_athena(event['query'])
        elif 'manifest_key' in event:
            # S3 manifest
            processor = DomainQueryProcessor(ATHENA_DATABASE, ATHENA_WORKGROUP)
            domains = await processor.get_domains_from_s3_manifest(event['manifest_key'])
        elif 'domains' in event:
            # Direct domain list
            domains = event['domains']
        else:
            raise ValueError("Event must contain 'query', 'manifest_key', or 'domains'")
        
        if not domains:
            logger.warning("No domains to check")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No domains to check',
                    'domains_checked': 0
                })
            }
        
        logger.info(f"Checking health for {len(domains)} domains")
        
        # Process domains asynchronously
        health_results = await process_domains_async(domains, MAX_CONCURRENCY)
        
        # Store results
        current_date = datetime.utcnow().strftime('%Y-%m-%d')
        key = f"silver/domain_health/date={current_date}/health_checks_{int(time.time())}.jsonl"
        store_health_data(CURATED_BUCKET, key, health_results)
        
        # Calculate metrics
        avg_health_score = sum(r.get('domain_health_score', 0) for r in health_results) / len(health_results) if health_results else 0.0
        
        # Emit metrics
        emit_cloudwatch_metrics(len(health_results), avg_health_score)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Domain health checks completed successfully',
                'domains_checked': len(health_results),
                'average_health_score': avg_health_score,
                's3_key': key
            })
        }
        
    except Exception as e:
        logger.error(f"Error in domain health gate: {e}")
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
        'domains': ['google.com', 'github.com', 'example.com']
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))