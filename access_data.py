#!/usr/bin/env python3
"""
Data Access Tool for Thera Pipeline
Provides multiple ways to access and query the enriched startup data
"""

import boto3
import json
import pandas as pd
from datetime import datetime
import argparse

def get_s3_client():
    """Initialize S3 client"""
    return boto3.client('s3', region_name='us-east-2')

def get_dynamodb_resource():
    """Initialize DynamoDB resource"""
    return boto3.resource('dynamodb', region_name='us-east-2')

def list_available_data():
    """List all available data in S3"""
    s3_client = get_s3_client()
    
    print("📊 Available Data in S3:")
    print("=" * 50)
    
    # List gold layer data
    print("\n🏆 Gold Layer Data:")
    try:
        response = s3_client.list_objects_v2(
            Bucket='thera-curated-805595753342-v3',
            Prefix='gold/startup_profiles/'
        )
        
        for obj in response.get('Contents', []):
            print(f"  📄 {obj['Key']}")
            print(f"     Size: {obj['Size']} bytes")
            print(f"     Modified: {obj['LastModified']}")
            print()
    except Exception as e:
        print(f"  ❌ Error listing gold data: {e}")
    
    # List embeddings
    print("\n🧠 Embeddings Data:")
    try:
        response = s3_client.list_objects_v2(
            Bucket='thera-embeddings-805595753342-v3',
            Prefix='embeddings/'
        )
        
        for obj in response.get('Contents', []):
            print(f"  📄 {obj['Key']}")
            print(f"     Size: {obj['Size']} bytes")
            print(f"     Modified: {obj['LastModified']}")
            print()
    except Exception as e:
        print(f"  ❌ Error listing embeddings: {e}")
    
    # List summaries
    print("\n📋 Summary Data:")
    try:
        response = s3_client.list_objects_v2(
            Bucket='thera-curated-805595753342-v3',
            Prefix='summary/'
        )
        
        for obj in response.get('Contents', []):
            print(f"  📄 {obj['Key']}")
            print(f"     Size: {obj['Size']} bytes")
            print(f"     Modified: {obj['LastModified']}")
            print()
    except Exception as e:
        print(f"  ❌ Error listing summaries: {e}")

def query_dynamodb_data(table_name='public', limit=10):
    """Query data from DynamoDB"""
    dynamodb = get_dynamodb_resource()
    
    if table_name == 'public':
        table = dynamodb.Table('thera-startups-public')
    else:
        table = dynamodb.Table('thera-startups-private')
    
    print(f"🗄️  Querying {table_name} table (limit: {limit}):")
    print("=" * 50)
    
    try:
        response = table.scan(Limit=limit)
        
        for item in response['Items']:
            print(f"\n🏢 {item.get('company_name', 'Unknown')}")
            print(f"   ID: {item.get('company_key', 'N/A')}")
            print(f"   Industry: {item.get('industry', 'N/A')}")
            print(f"   Size: {item.get('company_size_category', 'N/A')}")
            print(f"   Stage: {item.get('revenue_stage', 'N/A')}")
            print(f"   Website: {item.get('website_url', 'N/A')}")
            
            if table_name == 'private':
                print(f"   Apollo ID: {item.get('apollo_id', 'N/A')}")
                print(f"   Web Content Length: {len(item.get('web_content', ''))} chars")
                print(f"   Enrichment Sources: {item.get('enrichment_sources', [])}")
            
            print(f"   Updated: {item.get('updated_at', 'N/A')}")
            print("-" * 30)
            
    except Exception as e:
        print(f"❌ Error querying DynamoDB: {e}")

def download_s3_data(file_key, output_file=None):
    """Download specific data file from S3"""
    s3_client = get_s3_client()
    
    if not output_file:
        output_file = file_key.split('/')[-1]
    
    try:
        if 'gold' in file_key:
            bucket = 'thera-curated-805595753342-v3'
        elif 'embeddings' in file_key:
            bucket = 'thera-embeddings-805595753342-v3'
        else:
            bucket = 'thera-curated-805595753342-v3'
        
        s3_client.download_file(bucket, file_key, output_file)
        print(f"✅ Downloaded: {file_key} -> {output_file}")
        return output_file
    except Exception as e:
        print(f"❌ Error downloading {file_key}: {e}")
        return None

def search_companies(query, table_name='public'):
    """Search companies by name or industry"""
    dynamodb = get_dynamodb_resource()
    
    if table_name == 'public':
        table = dynamodb.Table('thera-startups-public')
    else:
        table = dynamodb.Table('thera-startups-private')
    
    print(f"🔍 Searching for: '{query}' in {table_name} table")
    print("=" * 50)
    
    try:
        response = table.scan()
        results = []
        
        for item in response['Items']:
            company_name = item.get('company_name', '').lower()
            industry = item.get('industry', '').lower()
            
            if query.lower() in company_name or query.lower() in industry:
                results.append(item)
        
        if results:
            for item in results:
                print(f"\n🏢 {item.get('company_name', 'Unknown')}")
                print(f"   Industry: {item.get('industry', 'N/A')}")
                print(f"   Description: {item.get('description', 'N/A')[:100]}...")
                print(f"   Website: {item.get('website_url', 'N/A')}")
        else:
            print("❌ No companies found matching your query")
            
    except Exception as e:
        print(f"❌ Error searching: {e}")

def get_company_details(company_id, table_name='private'):
    """Get detailed information about a specific company"""
    dynamodb = get_dynamodb_resource()
    
    if table_name == 'public':
        table = dynamodb.Table('thera-startups-public')
    else:
        table = dynamodb.Table('thera-startups-private')
    
    print(f"🔍 Getting details for company: {company_id}")
    print("=" * 50)
    
    try:
        response = table.get_item(Key={'company_key': company_id})
        
        if 'Item' in response:
            item = response['Item']
            
            print(f"🏢 Company: {item.get('company_name', 'Unknown')}")
            print(f"🆔 ID: {item.get('company_key', 'N/A')}")
            print(f"🌐 Website: {item.get('website_url', 'N/A')}")
            print(f"💼 LinkedIn: {item.get('linkedin_url', 'N/A')}")
            print(f"🏭 Industry: {item.get('industry', 'N/A')}")
            print(f"📝 Description: {item.get('description', 'N/A')}")
            print(f"🏢 Headquarters: {item.get('headquarters_location', 'N/A')}")
            print(f"👥 Size: {item.get('company_size_category', 'N/A')}")
            print(f"💰 Revenue Stage: {item.get('revenue_stage', 'N/A')}")
            print(f"📊 Employee Count: {item.get('employee_count', 'N/A')}")
            print(f"💵 Total Funding: ${item.get('total_funding_usd', 'N/A')}")
            print(f"📅 Founded: {item.get('founded_year', 'N/A')}")
            print(f"🕒 Company Age: {item.get('company_age_years', 'N/A')} years")
            
            if table_name == 'private':
                print(f"\n🔗 Apollo ID: {item.get('apollo_id', 'N/A')}")
                print(f"🌐 Web Content Length: {len(item.get('web_content', ''))} characters")
                print(f"📊 Enrichment Sources: {item.get('enrichment_sources', [])}")
                print(f"🕒 Web Crawled: {item.get('web_crawled_at', 'N/A')}")
                
                # Show web content preview
                web_content = item.get('web_content', '')
                if web_content:
                    print(f"\n🌐 Web Content Preview:")
                    print(f"{web_content[:500]}...")
        else:
            print(f"❌ Company {company_id} not found")
            
    except Exception as e:
        print(f"❌ Error getting company details: {e}")

def export_to_csv(table_name='public', output_file=None):
    """Export data to CSV file"""
    dynamodb = get_dynamodb_resource()
    
    if table_name == 'public':
        table = dynamodb.Table('thera-startups-public')
    else:
        table = dynamodb.Table('thera-startups-private')
    
    if not output_file:
        output_file = f"thera_startups_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    print(f"📊 Exporting {table_name} data to CSV...")
    print("=" * 50)
    
    try:
        response = table.scan()
        items = response['Items']
        
        # Convert to DataFrame
        df = pd.DataFrame(items)
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        
        print(f"✅ Exported {len(items)} records to {output_file}")
        print(f"📁 File size: {len(df)} rows, {len(df.columns)} columns")
        
        return output_file
        
    except Exception as e:
        print(f"❌ Error exporting to CSV: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description='Access Thera Pipeline Data')
    parser.add_argument('--action', choices=['list', 'query', 'search', 'details', 'download', 'export'], 
                       default='list', help='Action to perform')
    parser.add_argument('--table', choices=['public', 'private'], default='public', 
                       help='DynamoDB table to query')
    parser.add_argument('--query', type=str, help='Search query or company ID')
    parser.add_argument('--limit', type=int, default=10, help='Limit for queries')
    parser.add_argument('--file', type=str, help='S3 file key to download')
    parser.add_argument('--output', type=str, help='Output file name')
    
    args = parser.parse_args()
    
    print("🚀 Thera Pipeline Data Access Tool")
    print("=" * 50)
    
    if args.action == 'list':
        list_available_data()
    elif args.action == 'query':
        query_dynamodb_data(args.table, args.limit)
    elif args.action == 'search':
        if args.query:
            search_companies(args.query, args.table)
        else:
            print("❌ Please provide a search query with --query")
    elif args.action == 'details':
        if args.query:
            get_company_details(args.query, args.table)
        else:
            print("❌ Please provide a company ID with --query")
    elif args.action == 'download':
        if args.file:
            download_s3_data(args.file, args.output)
        else:
            print("❌ Please provide a file key with --file")
    elif args.action == 'export':
        export_to_csv(args.table, args.output)

if __name__ == "__main__":
    main()
