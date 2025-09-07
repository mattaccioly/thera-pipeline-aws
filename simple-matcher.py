import json
import boto3
import os

def lambda_handler(event, context):
    """Simple matcher function for testing"""
    try:
        # Get parameters from event
        challenge_text = event.get('challenge_text', '')
        industry = event.get('industry', '')
        country = event.get('country', '')
        
        # Simple response
        response = {
            'statusCode': 200,
            'body': {
                'message': 'Matcher function is working!',
                'challenge_text': challenge_text,
                'industry': industry,
                'country': country,
                'matches_found': 0,
                'processing_time': '0.1s'
            }
        }
        
        return response
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'message': 'Error in matcher function'
            }
        }
