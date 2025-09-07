import json
import boto3
import os
from datetime import datetime

def lambda_handler(event, context):
    """Simplified Firecrawl API enrichment function"""
    try:
        # Get domains from event
        domains = event.get('domains', [])
        test_mode = event.get('test_mode', False)
        
        print(f"Processing domains for web scraping: {domains}")
        
        # Simulate Firecrawl API enrichment
        scraped_data = []
        
        for domain in domains:
            # Simulate scraped data from Firecrawl API
            scraped_company = {
                "domain": domain,
                "website_url": f"https://{domain}",
                "scraped_content": {
                    "title": f"{domain.split('.')[0].title()} - Company Website",
                    "description": f"Leading technology company based at {domain}",
                    "headlines": [
                        "Innovative Solutions",
                        "Technology Leadership",
                        "Customer Success"
                    ],
                    "technologies": [
                        "React",
                        "Node.js",
                        "AWS",
                        "Python",
                        "Docker"
                    ],
                    "social_links": {
                        "linkedin": f"https://linkedin.com/company/{domain.split('.')[0]}",
                        "twitter": f"https://twitter.com/{domain.split('.')[0]}",
                        "facebook": f"https://facebook.com/{domain.split('.')[0]}"
                    },
                    "contact_info": {
                        "email": f"contact@{domain}",
                        "phone": "+55 11 9999-9999",
                        "address": "São Paulo, Brazil"
                    }
                },
                "scraping_timestamp": datetime.utcnow().isoformat(),
                "scraping_source": "Firecrawl API (Simulated)",
                "status": "scraped"
            }
            
            scraped_data.append(scraped_company)
        
        # Store scraped data in S3
        s3_client = boto3.client('s3')
        bucket_name = os.environ.get('RAW_BUCKET', 'thera-raw')
        
        # Create S3 key with timestamp
        timestamp = datetime.utcnow().strftime('%Y-%m-%d')
        s3_key = f"firecrawl/scraped_websites/{timestamp}/firecrawl_scraping_{int(datetime.utcnow().timestamp())}.json"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(scraped_data, indent=2),
            ContentType='application/json'
        )
        
        print(f"Scraped data uploaded to s3://{bucket_name}/{s3_key}")
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Firecrawl API scraping completed successfully!',
                'domains_processed': len(domains),
                'scraped_websites': len(scraped_data),
                's3_location': f"s3://{bucket_name}/{s3_key}",
                'scraped_data': scraped_data
            }
        }
        
    except Exception as e:
        print(f"Error in Firecrawl scraping: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'message': 'Error in Firecrawl API scraping'
            }
        }
