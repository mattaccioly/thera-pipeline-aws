#!/usr/bin/env python3
"""
Enable Bedrock model access for Thera Pipeline
"""

import boto3
import json

def enable_bedrock_models():
    """Enable access to Bedrock models"""
    print("🔧 Enabling Bedrock Model Access")
    print("=" * 40)
    
    try:
        bedrock_client = boto3.client('bedrock', region_name='us-east-2')
        
        # List available models
        print("📋 Available Foundation Models:")
        response = bedrock_client.list_foundation_models()
        
        titan_models = [model for model in response['modelSummaries'] if 'titan' in model['modelId'].lower()]
        
        for model in titan_models:
            print(f"  • {model['modelId']} - {model['modelName']}")
        
        print(f"\n⚠️  Model Access Required")
        print("To enable Bedrock model access, you need to:")
        print("1. Go to AWS Bedrock console: https://us-east-2.console.aws.amazon.com/bedrock/home?region=us-east-2#/modelaccess")
        print("2. Request access to 'Amazon Titan Text Embeddings V2'")
        print("3. Wait for approval (usually takes a few minutes)")
        print("4. Once approved, the pipeline will be able to generate embeddings")
        
        return False
        
    except Exception as e:
        print(f"❌ Error checking Bedrock access: {e}")
        return False

def test_bedrock_access():
    """Test if Bedrock is accessible"""
    print("\n🧪 Testing Bedrock Access...")
    
    try:
        bedrock_client = boto3.client('bedrock-runtime', region_name='us-east-2')
        
        response = bedrock_client.invoke_model(
            modelId='amazon.titan-embed-text-v2:0',
            body=json.dumps({'inputText': 'test embedding'})
        )
        
        result = json.loads(response['body'].read())
        embedding = result.get('embedding', [])
        
        print(f"✅ Bedrock access working! Generated embedding with {len(embedding)} dimensions")
        return True
        
    except Exception as e:
        print(f"❌ Bedrock access failed: {e}")
        return False

if __name__ == "__main__":
    print("Starting Bedrock configuration...")
    
    # Check current access
    if test_bedrock_access():
        print("\n🎉 Bedrock is already configured and working!")
    else:
        enable_bedrock_models()
        print("\n📝 Please enable model access in the AWS console and try again.")
