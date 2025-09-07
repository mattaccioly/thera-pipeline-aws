#!/usr/bin/env python3
"""
Simple Web Interface for Thera Pipeline Data Access
"""

from flask import Flask, render_template_string, jsonify, request
import boto3
import json
from datetime import datetime

app = Flask(__name__)

def get_dynamodb_data(table_name='public'):
    """Get data from DynamoDB"""
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    table = dynamodb.Table(f'thera-startups-{table_name}')
    
    response = table.scan()
    return response['Items']

def get_s3_data():
    """Get summary data from S3"""
    s3_client = boto3.client('s3', region_name='us-east-2')
    
    try:
        response = s3_client.list_objects_v2(
            Bucket='thera-curated-805595753342-v3',
            Prefix='summary/'
        )
        
        # Get the latest summary
        if response.get('Contents'):
            latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
            file_key = latest_file['Key']
            
            obj = s3_client.get_object(Bucket='thera-curated-805595753342-v3', Key=file_key)
            return json.loads(obj['Body'].read())
    except Exception as e:
        print(f"Error getting S3 data: {e}")
    
    return None

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Thera Pipeline Data Explorer</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .header { text-align: center; margin-bottom: 30px; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .stat-card { background: #007bff; color: white; padding: 20px; border-radius: 8px; text-align: center; }
        .stat-number { font-size: 2em; font-weight: bold; }
        .stat-label { font-size: 0.9em; opacity: 0.9; }
        .company-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 20px; }
        .company-card { border: 1px solid #ddd; border-radius: 8px; padding: 20px; background: white; }
        .company-name { font-size: 1.2em; font-weight: bold; color: #333; margin-bottom: 10px; }
        .company-industry { color: #666; margin-bottom: 8px; }
        .company-description { color: #777; font-size: 0.9em; margin-bottom: 10px; }
        .company-meta { display: flex; justify-content: space-between; font-size: 0.8em; color: #999; }
        .enrichment-badge { background: #28a745; color: white; padding: 2px 8px; border-radius: 12px; font-size: 0.7em; margin-left: 5px; }
        .tabs { display: flex; margin-bottom: 20px; }
        .tab { padding: 10px 20px; background: #f8f9fa; border: 1px solid #ddd; cursor: pointer; margin-right: 5px; }
        .tab.active { background: #007bff; color: white; }
        .tab-content { display: none; }
        .tab-content.active { display: block; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Thera Pipeline Data Explorer</h1>
            <p>Explore your enriched startup data</p>
        </div>
        
        <div class="stats">
            <div class="stat-card">
                <div class="stat-number">{{ stats.total_companies }}</div>
                <div class="stat-label">Total Companies</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{{ stats.apollo_enriched }}</div>
                <div class="stat-label">Apollo Enriched</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{{ stats.firecrawl_enriched }}</div>
                <div class="stat-label">Web Scraped</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{{ stats.embeddings_generated }}</div>
                <div class="stat-label">Embeddings</div>
            </div>
        </div>
        
        <div class="tabs">
            <div class="tab active" onclick="showTab('public')">Public Data</div>
            <div class="tab" onclick="showTab('private')">Private Data</div>
        </div>
        
        <div id="public" class="tab-content active">
            <h2>Public Company Data</h2>
            <div class="company-grid">
                {% for company in public_companies %}
                <div class="company-card">
                    <div class="company-name">{{ company.company_name }}</div>
                    <div class="company-industry">{{ company.industry }}</div>
                    <div class="company-description">{{ company.description[:100] }}...</div>
                    <div class="company-meta">
                        <span>{{ company.company_size_category }} • {{ company.revenue_stage }}</span>
                        <span>{{ company.updated_at[:10] }}</span>
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
        
        <div id="private" class="tab-content">
            <h2>Private Company Data (Full Enrichment)</h2>
            <div class="company-grid">
                {% for company in private_companies %}
                <div class="company-card">
                    <div class="company-name">
                        {{ company.company_name }}
                        {% for source in company.enrichment_sources %}
                        <span class="enrichment-badge">{{ source }}</span>
                        {% endfor %}
                    </div>
                    <div class="company-industry">{{ company.industry }}</div>
                    <div class="company-description">{{ company.description[:100] }}...</div>
                    <div class="company-meta">
                        <span>{{ company.company_size_category }} • {{ company.revenue_stage }}</span>
                        <span>{{ company.updated_at[:10] }}</span>
                    </div>
                    {% if company.apollo_id %}
                    <div style="font-size: 0.8em; color: #666; margin-top: 5px;">
                        Apollo ID: {{ company.apollo_id }}
                    </div>
                    {% endif %}
                    {% if company.web_content %}
                    <div style="font-size: 0.8em; color: #666; margin-top: 5px;">
                        Web Content: {{ company.web_content|length }} chars
                    </div>
                    {% endif %}
                </div>
                {% endfor %}
            </div>
        </div>
    </div>
    
    <script>
        function showTab(tabName) {
            // Hide all tab contents
            document.querySelectorAll('.tab-content').forEach(content => {
                content.classList.remove('active');
            });
            
            // Remove active class from all tabs
            document.querySelectorAll('.tab').forEach(tab => {
                tab.classList.remove('active');
            });
            
            // Show selected tab content
            document.getElementById(tabName).classList.add('active');
            
            // Add active class to clicked tab
            event.target.classList.add('active');
        }
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    """Main dashboard"""
    try:
        public_companies = get_dynamodb_data('public')
        private_companies = get_dynamodb_data('private')
        summary_data = get_s3_data()
        
        # Calculate stats
        stats = {
            'total_companies': len(public_companies),
            'apollo_enriched': len([c for c in private_companies if 'apollo' in c.get('enrichment_sources', [])]),
            'firecrawl_enriched': len([c for c in private_companies if 'firecrawl' in c.get('enrichment_sources', [])]),
            'embeddings_generated': len(private_companies)
        }
        
        return render_template_string(HTML_TEMPLATE, 
                                   public_companies=public_companies,
                                   private_companies=private_companies,
                                   stats=stats)
    except Exception as e:
        return f"Error loading data: {str(e)}", 500

@app.route('/api/companies')
def api_companies():
    """API endpoint for company data"""
    table = request.args.get('table', 'public')
    companies = get_dynamodb_data(table)
    return jsonify(companies)

@app.route('/api/stats')
def api_stats():
    """API endpoint for statistics"""
    try:
        private_companies = get_dynamodb_data('private')
        summary_data = get_s3_data()
        
        stats = {
            'total_companies': len(private_companies),
            'apollo_enriched': len([c for c in private_companies if 'apollo' in c.get('enrichment_sources', [])]),
            'firecrawl_enriched': len([c for c in private_companies if 'firecrawl' in c.get('enrichment_sources', [])]),
            'embeddings_generated': len(private_companies),
            'last_updated': max([c.get('updated_at', '') for c in private_companies]) if private_companies else None
        }
        
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("🌐 Starting Thera Pipeline Web Interface...")
    print("📊 Access your data at: http://localhost:5000")
    print("🔌 API endpoints available at: http://localhost:5000/api/")
    app.run(debug=True, host='0.0.0.0', port=5000)
