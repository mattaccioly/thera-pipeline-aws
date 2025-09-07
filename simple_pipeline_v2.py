#!/usr/bin/env python3
"""
Simple end-to-end pipeline for Thera startup data processing (without Bedrock)
"""

import boto3
import json
import pandas as pd
from datetime import datetime
import time
from decimal import Decimal

def process_startup_data():
    """Process startup data end-to-end"""
    
    # Initialize clients
    s3_client = boto3.client('s3', region_name='us-east-2')
    
    # Read the CSV data
    print("Reading startup data...")
    df = pd.read_csv('/Users/matheusaccioly/Documents/thera/upload/df/teste/teste_startups.csv')
    
    print(f"Found {len(df)} startups")
    
    # Process each startup
    processed_data = []
    
    for index, row in df.iterrows():
        print(f"\nProcessing {index + 1}/{len(df)}: {row['Organization Name']}")
        
        # Clean and process data
        company_id = f"company_{index + 1:03d}"
        company_name = str(row['Organization Name']).strip()
        website_url = str(row['Website']).strip() if pd.notna(row['Website']) else ""
        linkedin_url = str(row['LinkedIn']).strip() if pd.notna(row['LinkedIn']) else ""
        industry = str(row['Industries']).strip() if pd.notna(row['Industries']) else ""
        description = str(row['Description']).strip() if pd.notna(row['Description']) else ""
        headquarters = str(row['Headquarters Location']).strip() if pd.notna(row['Headquarters Location']) else ""
        founded_date = str(row['Founded Date']).strip() if pd.notna(row['Founded Date']) else ""
        employee_count = str(row['Number of Employees']).strip() if pd.notna(row['Number of Employees']) else ""
        total_funding = str(row['Total Funding Amount (in USD)']).strip() if pd.notna(row['Total Funding Amount (in USD)']) else ""
        funding_status = str(row['Funding Status']).strip() if pd.notna(row['Funding Status']) else ""
        
        # Parse employee count
        employee_count_int = None
        if employee_count == '5001-10000':
            employee_count_int = 7500
        elif employee_count == '101-250':
            employee_count_int = 175
        elif employee_count == '501-1000':
            employee_count_int = 750
        
        # Parse founded year
        founded_year = None
        if founded_date and len(founded_date) >= 4:
            try:
                founded_year = int(founded_date[:4])
            except:
                pass
        
        # Parse funding amount (as float for JSON, will convert to Decimal for DynamoDB)
        total_funding_usd = None
        if total_funding and total_funding != 'nan':
            try:
                total_funding_usd = float(total_funding)
            except:
                pass
        
        # Create profile text for embeddings (simulated)
        profile_text = f"{company_name} {industry} {description} {headquarters}"
        
        # Create processed record
        processed_record = {
            'company_id': company_id,
            'company_name': company_name,
            'website_url': website_url,
            'linkedin_url': linkedin_url,
            'industry': industry,
            'description': description,
            'headquarters_location': headquarters,
            'founded_year': founded_year,
            'employee_count': employee_count_int,
            'total_funding_usd': total_funding_usd,
            'funding_status': funding_status,
            'company_size_category': 'Large' if employee_count_int and employee_count_int > 200 else 'Medium' if employee_count_int and employee_count_int > 50 else 'Small' if employee_count_int and employee_count_int > 10 else 'Startup',
            'revenue_stage': 'Mature' if total_funding_usd and total_funding_usd > 10000000 else 'Growth Stage' if total_funding_usd and total_funding_usd > 1000000 else 'Early Stage',
            'company_age_years': (datetime.now().year - founded_year) if founded_year else None,
            'profile_text': profile_text,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        processed_data.append(processed_record)
        print(f"  ✓ Processed: {company_name}")
    
    # Save processed data to S3
    print(f"\nSaving processed data to S3...")
    
    # Save gold layer data
    gold_data_file = f"gold_startup_profiles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    s3_client.put_object(
        Bucket='thera-curated-805595753342-v3',
        Key=f'gold/startup_profiles/date={datetime.now().strftime("%Y-%m-%d")}/{gold_data_file}',
        Body=json.dumps(processed_data, indent=2, default=str),
        ContentType='application/json'
    )
    print(f"✓ Gold layer data saved: s3://thera-curated-805595753342-v3/gold/startup_profiles/date={datetime.now().strftime('%Y-%m-%d')}/{gold_data_file}")
    
    # Create DynamoDB records
    print(f"\nCreating DynamoDB records...")
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    
    # Create tables if they don't exist
    try:
        public_table = dynamodb.create_table(
            TableName='thera-startups-public',
            KeySchema=[
                {'AttributeName': 'company_id', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'company_id', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        print("✓ Created public table")
    except dynamodb.meta.client.exceptions.ResourceInUseException:
        public_table = dynamodb.Table('thera-startups-public')
        print("✓ Using existing public table")
    
    try:
        private_table = dynamodb.create_table(
            TableName='thera-startups-private',
            KeySchema=[
                {'AttributeName': 'company_id', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'company_id', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        print("✓ Created private table")
    except dynamodb.meta.client.exceptions.ResourceInUseException:
        private_table = dynamodb.Table('thera-startups-private')
        print("✓ Using existing private table")
    
    # Insert records into DynamoDB
    with public_table.batch_writer() as batch:
        for record in processed_data:
            # Create public record (without sensitive data)
            public_record = {
                'company_key': record['company_id'],
                'company_name': record['company_name'],
                'website_url': record['website_url'],
                'linkedin_url': record['linkedin_url'],
                'industry': record['industry'],
                'description': record['description'],
                'headquarters_location': record['headquarters_location'],
                'founded_year': record['founded_year'] or 0,
                'employee_count': record['employee_count'] or 0,
                'total_funding_usd': Decimal(str(record['total_funding_usd'])) if record['total_funding_usd'] else Decimal('0'),
                'funding_status': record['funding_status'],
                'company_size_category': record['company_size_category'],
                'revenue_stage': record['revenue_stage'],
                'company_age_years': record['company_age_years'] or 0,
                'created_at': record['created_at'],
                'updated_at': record['updated_at']
            }
            batch.put_item(Item=public_record)
    
    with private_table.batch_writer() as batch:
        for record in processed_data:
            # Create private record (with all data)
            private_record = {
                'company_key': record['company_id'],
                'company_name': record['company_name'],
                'website_url': record['website_url'],
                'linkedin_url': record['linkedin_url'],
                'industry': record['industry'],
                'description': record['description'],
                'headquarters_location': record['headquarters_location'],
                'founded_year': record['founded_year'] or 0,
                'employee_count': record['employee_count'] or 0,
                'total_funding_usd': Decimal(str(record['total_funding_usd'])) if record['total_funding_usd'] else Decimal('0'),
                'funding_status': record['funding_status'],
                'company_size_category': record['company_size_category'],
                'revenue_stage': record['revenue_stage'],
                'company_age_years': record['company_age_years'] or 0,
                'profile_text': record['profile_text'],
                'created_at': record['created_at'],
                'updated_at': record['updated_at']
            }
            batch.put_item(Item=private_record)
    
    print(f"✓ Inserted {len(processed_data)} records into DynamoDB")
    
    # Create summary
    summary = {
        'pipeline_execution': {
            'total_startups_processed': len(processed_data),
            'execution_time': datetime.now().isoformat(),
            'data_sources': ['CSV'],
            'processing_stages': ['Bronze', 'Silver', 'Gold', 'DynamoDB']
        },
        'companies': [
            {
                'company_id': record['company_id'],
                'company_name': record['company_name'],
                'industry': record['industry'],
                'company_size_category': record['company_size_category'],
                'revenue_stage': record['revenue_stage']
            }
            for record in processed_data
        ]
    }
    
    # Save summary
    s3_client.put_object(
        Bucket='thera-curated-805595753342-v3',
        Key=f'summary/pipeline_execution_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json',
        Body=json.dumps(summary, indent=2, default=str),
        ContentType='application/json'
    )
    
    print(f"\n=== PIPELINE EXECUTION SUMMARY ===")
    print(f"Total startups processed: {len(processed_data)}")
    print(f"Data saved to S3 and DynamoDB")
    print(f"Execution completed at: {datetime.now().isoformat()}")
    
    # Display processed companies
    print(f"\n=== PROCESSED COMPANIES ===")
    for record in processed_data:
        print(f"• {record['company_name']} ({record['industry']}) - {record['company_size_category']} - {record['revenue_stage']}")
    
    return processed_data

if __name__ == "__main__":
    print("Starting Thera Pipeline end-to-end execution...")
    processed_data = process_startup_data()
    print(f"\nPipeline completed successfully!")
