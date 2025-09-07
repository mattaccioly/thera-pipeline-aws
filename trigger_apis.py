import boto3
import json

# Initialize AWS clients
lambda_client = boto3.client('lambda')
athena_client = boto3.client('athena')

def trigger_apollo_api():
    """Trigger Apollo API to enrich startup data"""
    print("🚀 Triggering Apollo API...")
    
    # Get domains from your data
    domains = [
        "vitru.com.br",
        "educbank.com.br", 
        "descomplica.com.br"
    ]
    
    # Trigger Apollo API
    response = lambda_client.invoke(
        FunctionName='thera-apollo-delta-pull-dev',
        InvocationType='RequestResponse',
        Payload=json.dumps({
            'domains': domains,
            'test_mode': True
        })
    )
    
    result = json.loads(response['Payload'].read())
    print(f"Apollo API Response: {result}")
    return result

def trigger_firecrawl_api():
    """Trigger Firecrawl API to scrape websites"""
    print("🕷️ Triggering Firecrawl API...")
    
    # Get domains from your data
    domains = [
        "vitru.com.br",
        "educbank.com.br",
        "descomplica.com.br"
    ]
    
    # Trigger Firecrawl API
    response = lambda_client.invoke(
        FunctionName='thera-firecrawl-orchestrator-dev',
        InvocationType='RequestResponse',
        Payload=json.dumps({
            'domains': domains,
            'test_mode': True
        })
    )
    
    result = json.loads(response['Payload'].read())
    print(f"Firecrawl API Response: {result}")
    return result

def main():
    print("🎯 Starting API enrichment process...")
    
    # Trigger both APIs
    apollo_result = trigger_apollo_api()
    firecrawl_result = trigger_firecrawl_api()
    
    print("\n✅ API enrichment complete!")
    print(f"Apollo: {apollo_result}")
    print(f"Firecrawl: {firecrawl_result}")

if __name__ == "__main__":
    main()
