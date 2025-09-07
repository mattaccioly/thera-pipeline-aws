import json
import boto3
import os
from datetime import datetime

def lambda_handler(event, context):
    """Simplified Apollo API enrichment function"""
    try:
        # Get domains from event
        domains = event.get('domains', [])
        test_mode = event.get('test_mode', False)
        
        print(f"Processing domains: {domains}")
        
        # Simulate Apollo API enrichment
        enriched_data = []
        
        for domain in domains:
            # Simulate enriched data from Apollo API
            enriched_company = {
                "domain": domain,
                "company_name": domain.split('.')[0].title(),
                "apollo_company_id": f"apollo_{hash(domain) % 10000}",
                "apollo_contacts": [
                    {
                        "name": "CEO Name",
                        "title": "Chief Executive Officer",
                        "email": f"ceo@{domain}",
                        "linkedin": f"https://linkedin.com/in/ceo-{domain.split('.')[0]}"
                    }
                ],
                "apollo_company_data": {
                    "employee_count": "100-500",
                    "industry": "Technology",
                    "founded_year": "2020",
                    "total_funding": "$10M",
                    "last_funding_date": "2023-01-01"
                },
                "enrichment_timestamp": datetime.utcnow().isoformat(),
                "enrichment_source": "Apollo API (Simulated)",
                "status": "enriched"
            }
            
            enriched_data.append(enriched_company)
        
        # Store enriched data in S3
        s3_client = boto3.client('s3')
        bucket_name = os.environ.get('RAW_BUCKET', 'thera-raw')
        
        # Create S3 key with timestamp
        timestamp = datetime.utcnow().strftime('%Y-%m-%d')
        s3_key = f"apollo/enriched_companies/{timestamp}/apollo_enrichment_{int(datetime.utcnow().timestamp())}.json"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(enriched_data, indent=2),
            ContentType='application/json'
        )
        
        print(f"Enriched data uploaded to s3://{bucket_name}/{s3_key}")
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Apollo API enrichment completed successfully!',
                'domains_processed': len(domains),
                'enriched_companies': len(enriched_data),
                's3_location': f"s3://{bucket_name}/{s3_key}",
                'enriched_data': enriched_data
            }
        }
        
    except Exception as e:
        print(f"Error in Apollo enrichment: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'message': 'Error in Apollo API enrichment'
            }
        }
