import json
import boto3
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import requests
from botocore.exceptions import ClientError
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')
cloudwatch = boto3.client('cloudwatch')

# Environment variables
APOLLO_QUOTA_TABLE = os.environ['APOLLO_QUOTA_TABLE']
RAW_BUCKET = os.environ['RAW_BUCKET']
BRONZE_BUCKET = os.environ['BRONZE_BUCKET']
SECRET_NAME = os.environ['APOLLO_SECRET_NAME']
REGION = os.environ.get('AWS_REGION', 'us-east-1')

# Rate limits
RATE_LIMITS = {
    'per_minute': 50,
    'per_hour': 200,
    'per_day': 600
}

class ApolloQuotaManager:
    """Manages Apollo API rate limiting using DynamoDB token bucket"""
    
    def __init__(self, table_name: str):
        self.table = dynamodb.Table(table_name)
        self.quota_key = 'apollo_quota'
    
    def get_quota_status(self) -> Dict:
        """Get current quota status from DynamoDB"""
        try:
            response = self.table.get_item(Key={'id': self.quota_key})
            if 'Item' in response:
                return response['Item']
            else:
                # Initialize quota if not exists
                return self._initialize_quota()
        except ClientError as e:
            logger.error(f"Error getting quota status: {e}")
            return self._initialize_quota()
    
    def _initialize_quota(self) -> Dict:
        """Initialize quota counters in DynamoDB"""
        now = int(time.time())
        quota = {
            'id': self.quota_key,
            'minute_count': 0,
            'minute_reset': now + 60,
            'hour_count': 0,
            'hour_reset': now + 3600,
            'day_count': 0,
            'day_reset': now + 86400,
            'last_updated': now
        }
        try:
            self.table.put_item(Item=quota)
            return quota
        except ClientError as e:
            logger.error(f"Error initializing quota: {e}")
            return quota
    
    def can_make_request(self) -> Tuple[bool, str]:
        """Check if we can make a request without exceeding rate limits"""
        quota = self.get_quota_status()
        now = int(time.time())
        
        # Reset counters if time windows have passed
        if now >= quota['minute_reset']:
            quota['minute_count'] = 0
            quota['minute_reset'] = now + 60
        
        if now >= quota['hour_reset']:
            quota['hour_count'] = 0
            quota['hour_reset'] = now + 3600
        
        if now >= quota['day_reset']:
            quota['day_count'] = 0
            quota['day_reset'] = now + 86400
        
        # Check limits
        if quota['minute_count'] >= RATE_LIMITS['per_minute']:
            return False, f"Minute limit exceeded: {quota['minute_count']}/{RATE_LIMITS['per_minute']}"
        
        if quota['hour_count'] >= RATE_LIMITS['per_hour']:
            return False, f"Hour limit exceeded: {quota['hour_count']}/{RATE_LIMITS['per_hour']}"
        
        if quota['day_count'] >= RATE_LIMITS['per_day']:
            return False, f"Day limit exceeded: {quota['day_count']}/{RATE_LIMITS['per_day']}"
        
        return True, "OK"
    
    def increment_quota(self) -> bool:
        """Increment quota counters after successful API call"""
        try:
            quota = self.get_quota_status()
            now = int(time.time())
            
            # Reset counters if time windows have passed
            if now >= quota['minute_reset']:
                quota['minute_count'] = 0
                quota['minute_reset'] = now + 60
            
            if now >= quota['hour_reset']:
                quota['hour_count'] = 0
                quota['hour_reset'] = now + 3600
            
            if now >= quota['day_reset']:
                quota['day_count'] = 0
                quota['day_reset'] = now + 86400
            
            # Increment counters
            quota['minute_count'] += 1
            quota['hour_count'] += 1
            quota['day_count'] += 1
            quota['last_updated'] = now
            
            # Update DynamoDB
            self.table.put_item(Item=quota)
            return True
        except ClientError as e:
            logger.error(f"Error incrementing quota: {e}")
            return False

class ApolloClient:
    """Apollo API client with rate limiting"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.apollo.io/v1"
        self.quota_manager = ApolloQuotaManager(APOLLO_QUOTA_TABLE)
        self.session = requests.Session()
        self.session.headers.update({
            'X-Api-Key': api_key,
            'Content-Type': 'application/json'
        })
    
    def get_companies(self, updated_after: str, page: int = 1, per_page: int = 25) -> Dict:
        """Fetch companies from Apollo API"""
        can_request, reason = self.quota_manager.can_make_request()
        if not can_request:
            raise Exception(f"Rate limit exceeded: {reason}")
        
        url = f"{self.base_url}/mixed_companies/search"
        params = {
            'updated_after': updated_after,
            'page': page,
            'per_page': per_page
        }
        
        try:
            response = self.session.post(url, json=params)
            response.raise_for_status()
            
            # Increment quota after successful request
            self.quota_manager.increment_quota()
            
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching companies: {e}")
            raise
    
    def get_contacts(self, updated_after: str, page: int = 1, per_page: int = 25) -> Dict:
        """Fetch contacts from Apollo API"""
        can_request, reason = self.quota_manager.can_make_request()
        if not can_request:
            raise Exception(f"Rate limit exceeded: {reason}")
        
        url = f"{self.base_url}/mixed_people/search"
        params = {
            'updated_after': updated_after,
            'page': page,
            'per_page': per_page
        }
        
        try:
            response = self.session.post(url, json=params)
            response.raise_for_status()
            
            # Increment quota after successful request
            self.quota_manager.increment_quota()
            
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching contacts: {e}")
            raise

def get_apollo_api_key() -> str:
    """Retrieve Apollo API key from Secrets Manager"""
    try:
        response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(response['SecretString'])
        return secret['apollo_api_key']
    except ClientError as e:
        logger.error(f"Error retrieving API key: {e}")
        raise

def store_raw_data(bucket: str, key: str, data: List[Dict]) -> None:
    """Store raw JSONL data to S3"""
    try:
        jsonl_content = '\n'.join(json.dumps(item) for item in data)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=jsonl_content.encode('utf-8'),
            ContentType='application/jsonl'
        )
        logger.info(f"Stored {len(data)} records to s3://{bucket}/{key}")
    except ClientError as e:
        logger.error(f"Error storing raw data: {e}")
        raise

def normalize_company(company: Dict) -> Dict:
    """Normalize company data for bronze layer"""
    return {
        'id': company.get('id'),
        'name': company.get('name'),
        'website_url': company.get('website_url'),
        'industry': company.get('industry'),
        'founded_year': company.get('founded_year'),
        'employee_count': company.get('employee_count'),
        'annual_revenue': company.get('annual_revenue'),
        'total_funding': company.get('total_funding'),
        'last_funding_round': company.get('last_funding_round'),
        'last_funding_date': company.get('last_funding_date'),
        'linkedin_url': company.get('linkedin_url'),
        'twitter_url': company.get('twitter_url'),
        'facebook_url': company.get('facebook_url'),
        'description': company.get('description'),
        'keywords': company.get('keywords', []),
        'phone': company.get('phone'),
        'address': company.get('address'),
        'city': company.get('city'),
        'state': company.get('state'),
        'country': company.get('country'),
        'postal_code': company.get('postal_code'),
        'updated_at': company.get('updated_at'),
        'raw_data': company
    }

def normalize_contact(contact: Dict) -> Dict:
    """Normalize contact data for bronze layer"""
    return {
        'id': contact.get('id'),
        'first_name': contact.get('first_name'),
        'last_name': contact.get('last_name'),
        'email': contact.get('email'),
        'title': contact.get('title'),
        'company_id': contact.get('organization', {}).get('id') if contact.get('organization') else None,
        'company_name': contact.get('organization', {}).get('name') if contact.get('organization') else None,
        'linkedin_url': contact.get('linkedin_url'),
        'twitter_url': contact.get('twitter_url'),
        'phone_numbers': contact.get('phone_numbers', []),
        'city': contact.get('city'),
        'state': contact.get('state'),
        'country': contact.get('country'),
        'updated_at': contact.get('updated_at'),
        'raw_data': contact
    }

def emit_cloudwatch_metrics(items_fetched: int, calls_used: int) -> None:
    """Emit CloudWatch metrics"""
    try:
        cloudwatch.put_metric_data(
            Namespace='TheraPipeline/Apollo',
            MetricData=[
                {
                    'MetricName': 'ItemsFetched',
                    'Value': items_fetched,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'ApiCallsUsed',
                    'Value': calls_used,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                }
            ]
        )
    except ClientError as e:
        logger.error(f"Error emitting metrics: {e}")

def lambda_handler(event, context):
    """Main Lambda handler for Apollo Delta Pull"""
    try:
        # Get API key
        api_key = get_apollo_api_key()
        client = ApolloClient(api_key)
        
        # Get date for partitioning
        current_date = datetime.utcnow().strftime('%Y-%m-%d')
        
        # Calculate updated_after timestamp (last 24 hours)
        updated_after = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        total_items = 0
        total_calls = 0
        
        # Fetch companies
        logger.info("Starting companies fetch...")
        companies_data = []
        page = 1
        
        while True:
            try:
                response = client.get_companies(updated_after, page=page, per_page=25)
                companies = response.get('companies', [])
                
                if not companies:
                    break
                
                companies_data.extend(companies)
                total_calls += 1
                total_items += len(companies)
                
                logger.info(f"Fetched {len(companies)} companies from page {page}")
                
                # Check if we have more pages
                if len(companies) < 25:
                    break
                
                page += 1
                
                # Small delay to respect rate limits
                time.sleep(1)
                
            except Exception as e:
                if "Rate limit exceeded" in str(e):
                    logger.warning(f"Rate limit exceeded during companies fetch: {e}")
                    break
                else:
                    logger.error(f"Error fetching companies page {page}: {e}")
                    break
        
        # Store raw companies data
        if companies_data:
            raw_key = f"apollo/companies/date={current_date}/companies_{int(time.time())}.jsonl"
            store_raw_data(RAW_BUCKET, raw_key, companies_data)
            
            # Store normalized companies data
            normalized_companies = [normalize_company(company) for company in companies_data]
            bronze_key = f"bronze_apollo_companies/date={current_date}/companies_{int(time.time())}.jsonl"
            store_raw_data(BRONZE_BUCKET, bronze_key, normalized_companies)
        
        # Fetch contacts
        logger.info("Starting contacts fetch...")
        contacts_data = []
        page = 1
        
        while True:
            try:
                response = client.get_contacts(updated_after, page=page, per_page=25)
                contacts = response.get('people', [])
                
                if not contacts:
                    break
                
                contacts_data.extend(contacts)
                total_calls += 1
                total_items += len(contacts)
                
                logger.info(f"Fetched {len(contacts)} contacts from page {page}")
                
                # Check if we have more pages
                if len(contacts) < 25:
                    break
                
                page += 1
                
                # Small delay to respect rate limits
                time.sleep(1)
                
            except Exception as e:
                if "Rate limit exceeded" in str(e):
                    logger.warning(f"Rate limit exceeded during contacts fetch: {e}")
                    break
                else:
                    logger.error(f"Error fetching contacts page {page}: {e}")
                    break
        
        # Store raw contacts data
        if contacts_data:
            raw_key = f"apollo/contacts/date={current_date}/contacts_{int(time.time())}.jsonl"
            store_raw_data(RAW_BUCKET, raw_key, contacts_data)
            
            # Store normalized contacts data
            normalized_contacts = [normalize_contact(contact) for contact in contacts_data]
            bronze_key = f"bronze_apollo_contacts/date={current_date}/contacts_{int(time.time())}.jsonl"
            store_raw_data(BRONZE_BUCKET, bronze_key, normalized_contacts)
        
        # Emit metrics
        emit_cloudwatch_metrics(total_items, total_calls)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Apollo delta pull completed successfully',
                'companies_fetched': len(companies_data),
                'contacts_fetched': len(contacts_data),
                'total_items': total_items,
                'total_calls': total_calls
            })
        }
        
    except Exception as e:
        logger.error(f"Error in Apollo delta pull: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }