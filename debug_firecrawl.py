#!/usr/bin/env python3
"""
Debug Firecrawl API integration
"""

import boto3
import json
import requests
import time

def get_firecrawl_key():
    """Get Firecrawl API key from Secrets Manager"""
    try:
        secrets_client = boto3.client('secretsmanager', region_name='us-east-2')
        response = secrets_client.get_secret_value(SecretId='thera/firecrawl/api-key')
        secret_data = json.loads(response['SecretString'])
        return secret_data.get('firecrawl_api_key')
    except Exception as e:
        print(f"❌ Error getting Firecrawl key: {e}")
        return None

def test_firecrawl_api():
    """Test Firecrawl API with different endpoints and parameters"""
    
    firecrawl_key = get_firecrawl_key()
    if not firecrawl_key:
        print("❌ No Firecrawl API key found")
        return False
    
    print(f"🔑 Firecrawl API key found: {firecrawl_key[:10]}...")
    
    # Test URLs from our data
    test_urls = [
        "https://vitru.com.br/",
        "http://www.educbank.com.br",
        "http://www.descomplica.com.br"
    ]
    
    headers = {
        'Authorization': f'Bearer {firecrawl_key}',
        'Content-Type': 'application/json'
    }
    
    # Test 1: Check API status
    print("\n🧪 Test 1: Checking API status...")
    try:
        status_url = "https://api.firecrawl.dev/v1/health"
        response = requests.get(status_url, headers=headers, timeout=10)
        print(f"  Status Code: {response.status_code}")
        print(f"  Response: {response.text}")
    except Exception as e:
        print(f"  ❌ Health check failed: {e}")
    
    # Test 2: Test scrape endpoint with minimal parameters
    print("\n🧪 Test 2: Testing scrape endpoint...")
    for i, url in enumerate(test_urls, 1):
        print(f"\n  Testing URL {i}: {url}")
        
        try:
            # Try different parameter combinations
            test_configs = [
                {
                    'name': 'Minimal config',
                    'payload': {
                        'url': url,
                        'formats': ['markdown']
                    }
                },
                {
                    'name': 'With onlyMainContent',
                    'payload': {
                        'url': url,
                        'formats': ['markdown'],
                        'onlyMainContent': True
                    }
                },
                {
                    'name': 'With maxDepth',
                    'payload': {
                        'url': url,
                        'formats': ['markdown'],
                        'maxDepth': 0
                    }
                },
                {
                    'name': 'Simple scrape',
                    'payload': {
                        'url': url
                    }
                }
            ]
            
            for config in test_configs:
                print(f"    Testing {config['name']}...")
                
                try:
                    response = requests.post(
                        "https://api.firecrawl.dev/v1/scrape",
                        headers=headers,
                        json=config['payload'],
                        timeout=30
                    )
                    
                    print(f"      Status: {response.status_code}")
                    
                    if response.status_code == 200:
                        data = response.json()
                        print(f"      ✅ Success! Data keys: {list(data.keys())}")
                        if 'data' in data:
                            content = data['data']
                            print(f"      Content keys: {list(content.keys())}")
                            if 'markdown' in content:
                                print(f"      Markdown length: {len(content['markdown'])} chars")
                        break
                    else:
                        print(f"      ❌ Error: {response.text[:200]}")
                        
                except Exception as e:
                    print(f"      ❌ Request failed: {e}")
                
                time.sleep(1)  # Rate limiting
                
        except Exception as e:
            print(f"  ❌ URL test failed: {e}")
    
    # Test 3: Check API limits and usage
    print("\n🧪 Test 3: Checking API usage...")
    try:
        usage_url = "https://api.firecrawl.dev/v1/usage"
        response = requests.get(usage_url, headers=headers, timeout=10)
        print(f"  Status Code: {response.status_code}")
        if response.status_code == 200:
            usage_data = response.json()
            print(f"  Usage data: {json.dumps(usage_data, indent=2)}")
        else:
            print(f"  Response: {response.text}")
    except Exception as e:
        print(f"  ❌ Usage check failed: {e}")
    
    return True

def test_alternative_endpoints():
    """Test alternative Firecrawl endpoints"""
    
    firecrawl_key = get_firecrawl_key()
    if not firecrawl_key:
        return False
    
    headers = {
        'Authorization': f'Bearer {firecrawl_key}',
        'Content-Type': 'application/json'
    }
    
    print("\n🧪 Test 4: Testing alternative endpoints...")
    
    # Test crawl endpoint instead of scrape
    try:
        crawl_payload = {
            'url': 'https://vitru.com.br/',
            'crawlerOptions': {
                'maxDepth': 1,
                'limit': 1
            }
        }
        
        response = requests.post(
            "https://api.firecrawl.dev/v1/crawl",
            headers=headers,
            json=crawl_payload,
            timeout=30
        )
        
        print(f"  Crawl endpoint status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"  ✅ Crawl successful! Job ID: {data.get('jobId')}")
        else:
            print(f"  ❌ Crawl error: {response.text[:200]}")
            
    except Exception as e:
        print(f"  ❌ Crawl test failed: {e}")
    
    return True

if __name__ == "__main__":
    print("🔍 Debugging Firecrawl API Integration")
    print("=" * 50)
    
    test_firecrawl_api()
    test_alternative_endpoints()
    
    print("\n📋 Debug Summary:")
    print("1. Check if API key is valid")
    print("2. Verify URL formats are correct")
    print("3. Check rate limits and usage")
    print("4. Test different parameter combinations")
    print("5. Consider using crawl endpoint instead of scrape")
