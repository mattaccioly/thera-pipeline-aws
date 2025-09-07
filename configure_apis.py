#!/usr/bin/env python3
"""
Interactive API configuration script for Thera Pipeline
"""

import boto3
import json
import getpass
import sys

def configure_apollo_api():
    """Configure Apollo API key"""
    print("\n=== Apollo API Configuration ===")
    print("Get your API key from: https://app.apollo.io/settings/integrations/api")
    
    apollo_key = getpass.getpass("Enter your Apollo API key: ").strip()
    
    if not apollo_key:
        print("❌ No API key provided. Skipping Apollo configuration.")
        return False
    
    try:
        secrets_client = boto3.client('secretsmanager', region_name='us-east-2')
        
        secret_value = {
            "apollo_api_key": apollo_key
        }
        
        # Try to update existing secret
        try:
            secrets_client.update_secret(
                SecretId='thera/apollo/api-key',
                SecretString=json.dumps(secret_value)
            )
            print("✅ Apollo API key updated successfully")
        except secrets_client.exceptions.ResourceNotFoundException:
            # Create new secret
            secrets_client.create_secret(
                Name='thera/apollo/api-key',
                Description='Apollo API key for Thera Pipeline',
                SecretString=json.dumps(secret_value)
            )
            print("✅ Apollo API key created successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ Error configuring Apollo API: {e}")
        return False

def configure_firecrawl_api():
    """Configure Firecrawl API key"""
    print("\n=== Firecrawl API Configuration ===")
    print("Get your API key from: https://firecrawl.dev/")
    
    firecrawl_key = getpass.getpass("Enter your Firecrawl API key: ").strip()
    
    if not firecrawl_key:
        print("❌ No API key provided. Skipping Firecrawl configuration.")
        return False
    
    try:
        secrets_client = boto3.client('secretsmanager', region_name='us-east-2')
        
        secret_value = {
            "firecrawl_api_key": firecrawl_key
        }
        
        # Try to update existing secret
        try:
            secrets_client.update_secret(
                SecretId='thera/firecrawl/api-key',
                SecretString=json.dumps(secret_value)
            )
            print("✅ Firecrawl API key updated successfully")
        except secrets_client.exceptions.ResourceNotFoundException:
            # Create new secret
            secrets_client.create_secret(
                Name='thera/firecrawl/api-key',
                Description='Firecrawl API key for Thera Pipeline',
                SecretString=json.dumps(secret_value)
            )
            print("✅ Firecrawl API key created successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ Error configuring Firecrawl API: {e}")
        return False

def verify_api_keys():
    """Verify that API keys are configured"""
    print("\n=== Verifying API Keys ===")
    
    secrets_client = boto3.client('secretsmanager', region_name='us-east-2')
    
    apollo_configured = False
    firecrawl_configured = False
    
    try:
        apollo_secret = secrets_client.get_secret_value(SecretId='thera/apollo/api-key')
        apollo_data = json.loads(apollo_secret['SecretString'])
        if apollo_data.get('apollo_api_key'):
            print("✅ Apollo API key is configured")
            apollo_configured = True
        else:
            print("❌ Apollo API key is empty")
    except Exception as e:
        print(f"❌ Apollo API key not found: {e}")
    
    try:
        firecrawl_secret = secrets_client.get_secret_value(SecretId='thera/firecrawl/api-key')
        firecrawl_data = json.loads(firecrawl_secret['SecretString'])
        if firecrawl_data.get('firecrawl_api_key'):
            print("✅ Firecrawl API key is configured")
            firecrawl_configured = True
        else:
            print("❌ Firecrawl API key is empty")
    except Exception as e:
        print(f"❌ Firecrawl API key not found: {e}")
    
    return apollo_configured, firecrawl_configured

def main():
    print("🔧 Thera Pipeline API Configuration")
    print("=" * 50)
    
    # Check if we want to configure APIs
    configure = input("\nDo you want to configure API keys? (y/n): ").lower().strip()
    
    if configure == 'y':
        apollo_success = configure_apollo_api()
        firecrawl_success = configure_firecrawl_api()
        
        if apollo_success or firecrawl_success:
            print("\n✅ API configuration completed!")
        else:
            print("\n⚠️  No APIs were configured.")
    
    # Verify current configuration
    apollo_ok, firecrawl_ok = verify_api_keys()
    
    print(f"\n📊 Configuration Status:")
    print(f"  Apollo API: {'✅ Configured' if apollo_ok else '❌ Not configured'}")
    print(f"  Firecrawl API: {'✅ Configured' if firecrawl_ok else '❌ Not configured'}")
    
    if apollo_ok and firecrawl_ok:
        print("\n🚀 Ready for production pipeline execution!")
        return True
    else:
        print("\n⚠️  Some APIs are not configured. Pipeline will run in limited mode.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
