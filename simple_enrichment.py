import json
import boto3
import os

def create_enriched_gold_profiles():
    """Create enriched gold profiles using existing data"""
    
    # Your startup data
    startups = [
        {
            "company_name": "Vitru",
            "website_url": "https://vitru.com.br/",
            "linkedin_url": "https://www.linkedin.com/company/vitrueducation/",
            "headquarters_location": "Florianópolis, Santa Catarina, Brazil",
            "industries": "E-Learning, EdTech, Education",
            "description": "Vitru provides online and tutored post-secondary education services in Brazil.",
            "founded_date": "1999-01-01",
            "employee_count": "5001-10000",
            "total_funding_usd": "55766228.0",
            "last_funding_date": "",
            "funding_status": "IPO",
            "country": "Brazil",
            "enrichment_status": "Apollo API Ready",
            "web_scraping_status": "Firecrawl API Ready"
        },
        {
            "company_name": "Educbank",
            "website_url": "http://www.educbank.com.br",
            "linkedin_url": "https://www.linkedin.com/company/educbank/",
            "headquarters_location": "São Paulo, Sao Paulo, Brazil",
            "industries": "EdTech, Education, Financial Services, FinTech",
            "description": "LatAm's first K-12 fintech",
            "founded_date": "2020-01-03",
            "employee_count": "101-250",
            "total_funding_usd": "79200000.0",
            "last_funding_date": "",
            "funding_status": "Early Stage Venture",
            "country": "Brazil",
            "enrichment_status": "Apollo API Ready",
            "web_scraping_status": "Firecrawl API Ready"
        },
        {
            "company_name": "Descomplica",
            "website_url": "http://www.descomplica.com.br",
            "linkedin_url": "https://www.linkedin.com/company/descomplica/",
            "headquarters_location": "Rio De Janeiro, Rio de Janeiro, Brazil",
            "industries": "E-Learning, EdTech, Education, Higher Education, Video on Demand",
            "description": "Descomplica is a provider of an online educational platform designed to prepare students for college entrance exams.",
            "founded_date": "2011-01-01",
            "employee_count": "501-1000",
            "total_funding_usd": "130613111.0",
            "last_funding_date": "",
            "funding_status": "Late Stage Venture",
            "country": "Brazil",
            "enrichment_status": "Apollo API Ready",
            "web_scraping_status": "Firecrawl API Ready"
        }
    ]
    
    # Save enriched profiles
    with open('enriched_gold_profiles.json', 'w') as f:
        json.dump(startups, f, indent=2)
    
    print("✅ Enriched gold profiles created!")
    print(f"�� {len(startups)} startup profiles ready for API enrichment")
    
    return startups

def show_api_trigger_commands():
    """Show how to trigger the APIs"""
    print("\n🚀 API Trigger Commands:")
    print("\n1. Apollo API (Company Data Enrichment):")
    print("aws lambda invoke --function-name thera-apollo-delta-pull-dev --payload '{\"domains\": [\"vitru.com.br\", \"educbank.com.br\", \"descomplica.com.br\"]}' apollo-response.json")
    
    print("\n2. Firecrawl API (Web Scraping):")
    print("aws lambda invoke --function-name thera-firecrawl-orchestrator-dev --payload '{\"domains\": [\"vitru.com.br\", \"educbank.com.br\", \"descomplica.com.br\"]}' firecrawl-response.json")
    
    print("\n3. Matcher API (Similarity Matching):")
    print("aws lambda invoke --function-name thera-matcher-dev --payload '{\"challenge_text\": \"EdTech startup in Brazil\", \"industry\": \"EdTech\", \"country\": \"Brazil\"}' matcher-response.json")

if __name__ == "__main__":
    # Create enriched profiles
    profiles = create_enriched_gold_profiles()
    
    # Show API commands
    show_api_trigger_commands()
    
    print("\n📋 Next Steps:")
    print("1. Deploy Lambda functions with proper dependencies")
    print("2. Trigger Apollo API for company data enrichment")
    print("3. Trigger Firecrawl API for web content scraping")
    print("4. Use Matcher API for similarity matching")
    print("5. Combine all data into comprehensive gold profiles")
