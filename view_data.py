#!/usr/bin/env python3
"""
Data Viewer Script for Thera Pipeline
Shows different ways to view the processed startup data
"""

import boto3
import json
import pandas as pd
from datetime import datetime

def view_s3_data():
    """View data from S3 bucket"""
    print("=== VIEWING S3 DATA ===")
    
    s3_client = boto3.client('s3', region_name='us-east-2')
    
    # List all files in the gold layer
    response = s3_client.list_objects_v2(
        Bucket='thera-curated-805595753342-v3',
        Prefix='gold/startup_profiles/'
    )
    
    print("Available data files in S3:")
    for obj in response.get('Contents', []):
        print(f"  • {obj['Key']} (Size: {obj['Size']} bytes, Modified: {obj['LastModified']})")
    
    # Download and display the latest file
    if response.get('Contents'):
        latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
        print(f"\nDownloading latest file: {latest_file['Key']}")
        
        obj = s3_client.get_object(Bucket='thera-curated-805595753342-v3', Key=latest_file['Key'])
        data = json.loads(obj['Body'].read().decode('utf-8'))
        
        print(f"\nFound {len(data)} companies:")
        for company in data:
            print(f"  • {company['company_name']} ({company['industry']}) - {company['company_size_category']} - ${company['total_funding_usd']:,.0f}")

def view_dynamodb_data():
    """View data from DynamoDB tables"""
    print("\n=== VIEWING DYNAMODB DATA ===")
    
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    
    # View public table
    print("Public Table (thera-startups-public):")
    public_table = dynamodb.Table('thera-startups-public')
    response = public_table.scan()
    
    for item in response['Items']:
        print(f"  • {item['company_name']} - {item['industry']} - {item['company_size_category']}")
    
    # View private table
    print("\nPrivate Table (thera-startups-private):")
    private_table = dynamodb.Table('thera-startups-private')
    response = private_table.scan()
    
    for item in response['Items']:
        print(f"  • {item['company_name']} - {item['industry']} - {item['company_size_category']}")

def view_athena_data():
    """Show how to query data using Athena"""
    print("\n=== ATHENA QUERIES ===")
    
    print("To query data using Athena, you can use these SQL queries:")
    print("\n1. View all companies:")
    print("""
    SELECT 
        company_name,
        industry,
        headquarters_location,
        founded_year,
        employee_count,
        total_funding_usd,
        funding_status,
        company_size_category,
        revenue_stage
    FROM gold_startup_profiles
    ORDER BY total_funding_usd DESC;
    """)
    
    print("\n2. Filter by company size:")
    print("""
    SELECT company_name, industry, company_size_category
    FROM gold_startup_profiles
    WHERE company_size_category = 'Large';
    """)
    
    print("\n3. Filter by industry:")
    print("""
    SELECT company_name, industry, total_funding_usd
    FROM gold_startup_profiles
    WHERE industry LIKE '%EdTech%'
    ORDER BY total_funding_usd DESC;
    """)

def create_summary_report():
    """Create a summary report of the data"""
    print("\n=== CREATING SUMMARY REPORT ===")
    
    # Read the local JSON file
    with open('downloaded_data.json', 'r') as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    
    print(f"Total Companies: {len(df)}")
    print(f"Industries: {df['industry'].nunique()}")
    print(f"Company Size Distribution:")
    print(df['company_size_category'].value_counts().to_string())
    
    print(f"\nRevenue Stage Distribution:")
    print(df['revenue_stage'].value_counts().to_string())
    
    print(f"\nTop Companies by Funding:")
    top_funding = df.nlargest(3, 'total_funding_usd')[['company_name', 'total_funding_usd', 'industry']]
    for _, row in top_funding.iterrows():
        print(f"  • {row['company_name']}: ${row['total_funding_usd']:,.0f} ({row['industry']})")
    
    print(f"\nAverage Company Age: {df['company_age_years'].mean():.1f} years")
    print(f"Average Employee Count: {df['employee_count'].mean():.0f} employees")
    print(f"Total Funding: ${df['total_funding_usd'].sum():,.0f}")

def main():
    """Main function to run all data viewing methods"""
    print("🔍 Thera Pipeline Data Viewer")
    print("=" * 50)
    
    try:
        view_s3_data()
        view_dynamodb_data()
        view_athena_data()
        create_summary_report()
        
        print("\n✅ Data viewing completed successfully!")
        
    except Exception as e:
        print(f"❌ Error viewing data: {e}")

if __name__ == "__main__":
    main()
