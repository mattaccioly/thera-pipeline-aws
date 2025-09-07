#!/usr/bin/env python3
"""
Simple embeddings processing script for Thera Pipeline
Processes startup profiles and generates embeddings using Bedrock
"""

import boto3
import json
import pandas as pd
from datetime import datetime
import time

def get_bedrock_client():
    """Initialize Bedrock client"""
    return boto3.client('bedrock-runtime', region_name='us-east-2')

def generate_embedding(text, bedrock_client):
    """Generate embedding for text using Amazon Titan"""
    try:
        response = bedrock_client.invoke_model(
            modelId='amazon.titan-embed-text-v1',
            body=json.dumps({
                'inputText': text
            })
        )
        
        result = json.loads(response['body'].read())
        return result['embedding']
    except Exception as e:
        print(f"Error generating embedding: {e}")
        return None

def process_startup_profiles():
    """Process startup profiles and generate embeddings"""
    
    # Initialize clients
    bedrock_client = get_bedrock_client()
    s3_client = boto3.client('s3', region_name='us-east-2')
    athena_client = boto3.client('athena', region_name='us-east-2')
    
    # Query the gold layer data
    query = """
    SELECT 
        company_id,
        company_name,
        website_url,
        industry,
        description,
        profile_text,
        company_size_category,
        revenue_stage,
        headquarters_city,
        headquarters_country
    FROM thera_analytics.gold_startup_profiles_temp
    WHERE profile_text IS NOT NULL
    """
    
    print("Querying startup profiles...")
    
    # Execute query
    response = athena_client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': 's3://thera-curated-805595753342-v3/athena-results/'
        },
        WorkGroup='primary'
    )
    
    query_execution_id = response['QueryExecutionId']
    
    # Wait for query to complete
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        
        print(f"Query status: {status}")
        time.sleep(2)
    
    if status != 'SUCCEEDED':
        print(f"Query failed: {response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')}")
        return
    
    # Get query results
    results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    
    # Process results and generate embeddings
    embeddings_data = []
    
    print("Processing profiles and generating embeddings...")
    
    for i, row in enumerate(results['ResultSet']['Rows'][1:], 1):  # Skip header
        if i > 3:  # Limit to first 3 for demo
            break
            
        data = row['Data']
        if len(data) < 10:
            continue
            
        profile = {
            'company_id': data[0].get('VarCharValue', ''),
            'company_name': data[1].get('VarCharValue', ''),
            'website_url': data[2].get('VarCharValue', ''),
            'industry': data[3].get('VarCharValue', ''),
            'description': data[4].get('VarCharValue', ''),
            'profile_text': data[5].get('VarCharValue', ''),
            'company_size_category': data[6].get('VarCharValue', ''),
            'revenue_stage': data[7].get('VarCharValue', ''),
            'headquarters_city': data[8].get('VarCharValue', ''),
            'headquarters_country': data[9].get('VarCharValue', '')
        }
        
        print(f"Processing: {profile['company_name']}")
        
        # Generate embedding
        embedding = generate_embedding(profile['profile_text'], bedrock_client)
        
        if embedding:
            embeddings_data.append({
                'company_id': profile['company_id'],
                'company_name': profile['company_name'],
                'profile_text': profile['profile_text'],
                'embedding': embedding,
                'metadata': {
                    'industry': profile['industry'],
                    'company_size_category': profile['company_size_category'],
                    'revenue_stage': profile['revenue_stage'],
                    'headquarters_city': profile['headquarters_city'],
                    'headquarters_country': profile['headquarters_country'],
                    'website_url': profile['website_url']
                },
                'created_at': datetime.now().isoformat()
            })
            
            print(f"✓ Generated embedding for {profile['company_name']}")
        else:
            print(f"✗ Failed to generate embedding for {profile['company_name']}")
    
    # Save embeddings to S3
    if embeddings_data:
        print(f"\nSaving {len(embeddings_data)} embeddings to S3...")
        
        # Create embeddings file
        embeddings_file = f"embeddings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Upload to S3
        s3_client.put_object(
            Bucket='thera-embeddings-805595753342-v3',
            Key=f'embeddings/date={datetime.now().strftime("%Y-%m-%d")}/{embeddings_file}',
            Body=json.dumps(embeddings_data, indent=2),
            ContentType='application/json'
        )
        
        print(f"✓ Embeddings saved to S3: s3://thera-embeddings-805595753342-v3/embeddings/date={datetime.now().strftime('%Y-%m-%d')}/{embeddings_file}")
        
        # Also save a summary
        summary = {
            'total_embeddings': len(embeddings_data),
            'companies': [{'name': item['company_name'], 'id': item['company_id']} for item in embeddings_data],
            'created_at': datetime.now().isoformat()
        }
        
        s3_client.put_object(
            Bucket='thera-embeddings-805595753342-v3',
            Key=f'embeddings/date={datetime.now().strftime("%Y-%m-%d")}/summary.json',
            Body=json.dumps(summary, indent=2),
            ContentType='application/json'
        )
        
        print("✓ Summary saved")
        
        return embeddings_data
    else:
        print("No embeddings generated")
        return []

if __name__ == "__main__":
    print("Starting embeddings processing...")
    embeddings = process_startup_profiles()
    print(f"\nCompleted! Generated {len(embeddings)} embeddings.")
