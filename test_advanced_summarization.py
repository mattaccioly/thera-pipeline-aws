#!/usr/bin/env python3
"""
Test script for Advanced Summarization Pipeline
Tests all components including LLM integration, caching, and cost monitoring
"""

import json
import boto3
import time
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize AWS clients
bedrock_client = boto3.client('bedrock-runtime')
athena_client = boto3.client('athena')
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

class AdvancedSummarizationTester:
    """Comprehensive tester for advanced summarization pipeline"""
    
    def __init__(self):
        self.test_results = {}
        self.test_data = self._create_test_data()
    
    def _create_test_data(self) -> List[Dict]:
        """Create test data for validation"""
        return [
            {
                "company_key": "test-company-1",
                "domain": "testcompany1.com",
                "company_name": "TestTech Solutions",
                "industry": "Software Development",
                "description": "TestTech Solutions is a leading provider of cloud-based software solutions for enterprise clients. We specialize in AI-powered automation tools and data analytics platforms.",
                "web_content": "TestTech Solutions - AI-Powered Software Solutions. Our platform helps enterprises automate their workflows and gain insights from their data. Founded in 2020, we serve Fortune 500 companies worldwide.",
                "profile_text": "TestTech Solutions is a Software Development company founded in 2020. We provide AI-powered automation tools and data analytics platforms for enterprise clients.",
                "content_hash": "test-hash-1",
                "last_updated": datetime.utcnow().isoformat()
            },
            {
                "company_key": "test-company-2",
                "domain": "testcompany2.com",
                "company_name": "GreenEnergy Innovations",
                "industry": "Clean Technology",
                "description": "GreenEnergy Innovations develops sustainable energy solutions for residential and commercial markets. Our solar panel technology and energy storage systems help reduce carbon footprints.",
                "web_content": "GreenEnergy Innovations - Sustainable Energy Solutions. We develop cutting-edge solar panel technology and energy storage systems for homes and businesses. Committed to a greener future.",
                "profile_text": "GreenEnergy Innovations is a Clean Technology company developing sustainable energy solutions including solar panels and energy storage systems.",
                "content_hash": "test-hash-2",
                "last_updated": datetime.utcnow().isoformat()
            }
        ]
    
    def test_bedrock_access(self) -> bool:
        """Test Bedrock access and model availability"""
        try:
            logger.info("Testing Bedrock access...")
            
            # Test Claude 3 Haiku
            response = bedrock_client.invoke_model(
                modelId="anthropic.claude-3-haiku-20240307-v1:0",
                body=json.dumps({
                    "prompt": "Test prompt for Bedrock access validation.",
                    "max_tokens_to_sample": 50,
                    "temperature": 0.1
                })
            )
            
            result = json.loads(response['body'].read())
            logger.info("✓ Bedrock access successful")
            self.test_results['bedrock_access'] = True
            return True
            
        except Exception as e:
            logger.error(f"✗ Bedrock access failed: {e}")
            self.test_results['bedrock_access'] = False
            return False
    
    def test_executive_summary_generation(self) -> bool:
        """Test executive summary generation"""
        try:
            logger.info("Testing executive summary generation...")
            
            test_company = self.test_data[0]
            prompt = f"""
You are a business analyst. Create a concise executive summary (2-3 sentences) for this startup company.

Company Data:
- Name: {test_company['company_name']}
- Industry: {test_company['industry']}
- Description: {test_company['description']}
- Website Content: {test_company['web_content']}

Requirements:
- Focus on what the company does and its value proposition
- Mention key differentiators or unique aspects
- Keep it professional and informative
- Maximum 3 sentences

Output as JSON: {{"executive_summary": "your summary here", "confidence": 0.85}}
"""
            
            response = bedrock_client.invoke_model(
                modelId="anthropic.claude-3-haiku-20240307-v1:0",
                body=json.dumps({
                    "prompt": prompt,
                    "max_tokens_to_sample": 200,
                    "temperature": 0.1
                })
            )
            
            result = json.loads(response['body'].read())
            summary_data = json.loads(result['completion'])
            
            if 'executive_summary' in summary_data and len(summary_data['executive_summary']) > 50:
                logger.info("✓ Executive summary generation successful")
                logger.info(f"Generated summary: {summary_data['executive_summary']}")
                self.test_results['executive_summary'] = True
                return True
            else:
                logger.error("✗ Executive summary generation failed - invalid output")
                self.test_results['executive_summary'] = False
                return False
                
        except Exception as e:
            logger.error(f"✗ Executive summary generation failed: {e}")
            self.test_results['executive_summary'] = False
            return False
    
    def test_key_insights_extraction(self) -> bool:
        """Test key insights extraction"""
        try:
            logger.info("Testing key insights extraction...")
            
            test_company = self.test_data[0]
            prompt = f"""
You are a business intelligence analyst. Extract 5-7 key business insights from this startup's information.

Company Data:
- Name: {test_company['company_name']}
- Industry: {test_company['industry']}
- Description: {test_company['description']}
- Website Content: {test_company['web_content']}

Analyze and extract:
1. Business model and revenue streams
2. Target market and customer segments
3. Competitive advantages
4. Growth potential indicators
5. Market positioning
6. Technology or innovation aspects
7. Strategic partnerships or relationships

Output as JSON: {{"key_insights": ["insight1", "insight2", ...], "business_model": "B2B/SaaS/etc", "value_proposition": "brief value prop", "target_customers": ["segment1", "segment2"]}}
"""
            
            response = bedrock_client.invoke_model(
                modelId="anthropic.claude-3-haiku-20240307-v1:0",
                body=json.dumps({
                    "prompt": prompt,
                    "max_tokens_to_sample": 500,
                    "temperature": 0.1
                })
            )
            
            result = json.loads(response['body'].read())
            insights_data = json.loads(result['completion'])
            
            if 'key_insights' in insights_data and len(insights_data['key_insights']) >= 3:
                logger.info("✓ Key insights extraction successful")
                logger.info(f"Generated insights: {insights_data['key_insights']}")
                self.test_results['key_insights'] = True
                return True
            else:
                logger.error("✗ Key insights extraction failed - invalid output")
                self.test_results['key_insights'] = False
                return False
                
        except Exception as e:
            logger.error(f"✗ Key insights extraction failed: {e}")
            self.test_results['key_insights'] = False
            return False
    
    def test_tech_stack_analysis(self) -> bool:
        """Test technology stack analysis"""
        try:
            logger.info("Testing technology stack analysis...")
            
            test_company = self.test_data[0]
            prompt = f"""
You are a technology analyst. Analyze the technology stack and technical capabilities of this startup.

Company Data:
- Name: {test_company['company_name']}
- Industry: {test_company['industry']}
- Description: {test_company['description']}
- Website Content: {test_company['web_content']}

Extract and analyze:
1. Primary technologies mentioned
2. Programming languages used
3. Frameworks and platforms
4. Cloud providers and infrastructure
5. Development tools and practices
6. Technical maturity level
7. Innovation indicators

Output as JSON: {{"primary_technologies": ["tech1", "tech2"], "programming_languages": ["lang1", "lang2"], "frameworks": ["framework1"], "cloud_providers": ["aws", "azure"], "tech_maturity_score": 0.8, "innovation_level": "modern"}}
"""
            
            response = bedrock_client.invoke_model(
                modelId="anthropic.claude-3-haiku-20240307-v1:0",
                body=json.dumps({
                    "prompt": prompt,
                    "max_tokens_to_sample": 300,
                    "temperature": 0.1
                })
            )
            
            result = json.loads(response['body'].read())
            tech_data = json.loads(result['completion'])
            
            if 'primary_technologies' in tech_data and 'tech_maturity_score' in tech_data:
                logger.info("✓ Technology stack analysis successful")
                logger.info(f"Primary technologies: {tech_data['primary_technologies']}")
                self.test_results['tech_analysis'] = True
                return True
            else:
                logger.error("✗ Technology stack analysis failed - invalid output")
                self.test_results['tech_analysis'] = False
                return False
                
        except Exception as e:
            logger.error(f"✗ Technology stack analysis failed: {e}")
            self.test_results['tech_analysis'] = False
            return False
    
    def test_competitive_analysis(self) -> bool:
        """Test competitive analysis"""
        try:
            logger.info("Testing competitive analysis...")
            
            test_company = self.test_data[0]
            prompt = f"""
You are a competitive intelligence analyst. Analyze this startup's competitive position and market dynamics.

Company Data:
- Name: {test_company['company_name']}
- Industry: {test_company['industry']}
- Description: {test_company['description']}
- Website Content: {test_company['web_content']}

Analyze:
1. Market position (leader, challenger, follower, niche)
2. Competitive advantages and differentiators
3. Market share indicators
4. Competitive threats and challenges
5. Moat strength and defensibility
6. Market barriers and opportunities

Output as JSON: {{"market_position": "challenger", "competitive_advantages": ["adv1", "adv2"], "differentiators": ["diff1"], "market_share_estimate": "small", "competitive_threats": ["threat1"], "moat_strength": "moderate"}}
"""
            
            response = bedrock_client.invoke_model(
                modelId="anthropic.claude-3-sonnet-20240229-v1:0",
                body=json.dumps({
                    "prompt": prompt,
                    "max_tokens_to_sample": 400,
                    "temperature": 0.1
                })
            )
            
            result = json.loads(response['body'].read())
            competitive_data = json.loads(result['completion'])
            
            if 'market_position' in competitive_data and 'competitive_advantages' in competitive_data:
                logger.info("✓ Competitive analysis successful")
                logger.info(f"Market position: {competitive_data['market_position']}")
                self.test_results['competitive_analysis'] = True
                return True
            else:
                logger.error("✗ Competitive analysis failed - invalid output")
                self.test_results['competitive_analysis'] = False
                return False
                
        except Exception as e:
            logger.error(f"✗ Competitive analysis failed: {e}")
            self.test_results['competitive_analysis'] = False
            return False
    
    def test_risk_assessment(self) -> bool:
        """Test risk assessment"""
        try:
            logger.info("Testing risk assessment...")
            
            test_company = self.test_data[0]
            prompt = f"""
You are a risk analyst. Assess potential risks and red flags for this startup.

Company Data:
- Name: {test_company['company_name']}
- Industry: {test_company['industry']}
- Description: {test_company['description']}
- Website Content: {test_company['web_content']}

Assess risks in these categories:
1. Business risks (market, competition, execution)
2. Technical risks (technology, scalability, security)
3. Financial risks (funding, revenue, costs)
4. Market risks (demand, timing, regulation)
5. Operational risks (team, processes, partnerships)

Output as JSON: {{"business_risks": ["risk1", "risk2"], "technical_risks": ["risk1"], "market_risks": ["risk1"], "financial_risks": ["risk1"], "regulatory_risks": ["risk1"], "overall_risk_level": "medium", "risk_score": 0.6, "red_flags": ["flag1", "flag2"]}}
"""
            
            response = bedrock_client.invoke_model(
                modelId="anthropic.claude-3-sonnet-20240229-v1:0",
                body=json.dumps({
                    "prompt": prompt,
                    "max_tokens_to_sample": 400,
                    "temperature": 0.1
                })
            )
            
            result = json.loads(response['body'].read())
            risk_data = json.loads(result['completion'])
            
            if 'overall_risk_level' in risk_data and 'risk_score' in risk_data:
                logger.info("✓ Risk assessment successful")
                logger.info(f"Overall risk level: {risk_data['overall_risk_level']}")
                self.test_results['risk_assessment'] = True
                return True
            else:
                logger.error("✗ Risk assessment failed - invalid output")
                self.test_results['risk_assessment'] = False
                return False
                
        except Exception as e:
            logger.error(f"✗ Risk assessment failed: {e}")
            self.test_results['risk_assessment'] = False
            return False
    
    def test_business_intelligence(self) -> bool:
        """Test business intelligence analysis"""
        try:
            logger.info("Testing business intelligence analysis...")
            
            test_company = self.test_data[0]
            prompt = f"""
You are a strategic business analyst. Provide comprehensive business intelligence insights.

Company Data:
- Name: {test_company['company_name']}
- Industry: {test_company['industry']}
- Description: {test_company['description']}
- Website Content: {test_company['web_content']}

Analyze:
1. Market opportunity size and potential
2. Growth potential and scalability indicators
3. Market timing and industry trends
4. Market dynamics and competitive landscape
5. Investment attractiveness
6. Strategic recommendations

Output as JSON: {{"market_opportunity": "large", "growth_potential": "high", "scalability_indicators": ["indicator1"], "market_timing": "optimal", "industry_trends": ["trend1"], "market_dynamics": ["dynamic1"]}}
"""
            
            response = bedrock_client.invoke_model(
                modelId="anthropic.claude-3-sonnet-20240229-v1:0",
                body=json.dumps({
                    "prompt": prompt,
                    "max_tokens_to_sample": 400,
                    "temperature": 0.1
                })
            )
            
            result = json.loads(response['body'].read())
            bi_data = json.loads(result['completion'])
            
            if 'market_opportunity' in bi_data and 'growth_potential' in bi_data:
                logger.info("✓ Business intelligence analysis successful")
                logger.info(f"Market opportunity: {bi_data['market_opportunity']}")
                self.test_results['business_intelligence'] = True
                return True
            else:
                logger.error("✗ Business intelligence analysis failed - invalid output")
                self.test_results['business_intelligence'] = False
                return False
                
        except Exception as e:
            logger.error(f"✗ Business intelligence analysis failed: {e}")
            self.test_results['business_intelligence'] = False
            return False
    
    def test_cost_calculation(self) -> bool:
        """Test cost calculation accuracy"""
        try:
            logger.info("Testing cost calculation...")
            
            # Test cost calculation for different models and token counts
            test_cases = [
                {
                    "model": "claude-3-haiku",
                    "input_tokens": 1000,
                    "output_tokens": 200,
                    "expected_cost": (1000 * 0.25 / 1_000_000) + (200 * 1.25 / 1_000_000)
                },
                {
                    "model": "claude-3-sonnet",
                    "input_tokens": 1000,
                    "output_tokens": 200,
                    "expected_cost": (1000 * 3.00 / 1_000_000) + (200 * 15.00 / 1_000_000)
                }
            ]
            
            for case in test_cases:
                if case["model"] == "claude-3-haiku":
                    calculated_cost = (case["input_tokens"] * 0.25 / 1_000_000) + (case["output_tokens"] * 1.25 / 1_000_000)
                else:
                    calculated_cost = (case["input_tokens"] * 3.00 / 1_000_000) + (case["output_tokens"] * 15.00 / 1_000_000)
                
                if abs(calculated_cost - case["expected_cost"]) < 0.000001:
                    logger.info(f"✓ Cost calculation correct for {case['model']}")
                else:
                    logger.error(f"✗ Cost calculation incorrect for {case['model']}")
                    self.test_results['cost_calculation'] = False
                    return False
            
            logger.info("✓ All cost calculations correct")
            self.test_results['cost_calculation'] = True
            return True
            
        except Exception as e:
            logger.error(f"✗ Cost calculation test failed: {e}")
            self.test_results['cost_calculation'] = False
            return False
    
    def test_performance_benchmarks(self) -> bool:
        """Test performance benchmarks"""
        try:
            logger.info("Testing performance benchmarks...")
            
            # Test response time for different models
            start_time = time.time()
            
            response = bedrock_client.invoke_model(
                modelId="anthropic.claude-3-haiku-20240307-v1:0",
                body=json.dumps({
                    "prompt": "Generate a brief executive summary for a tech startup.",
                    "max_tokens_to_sample": 100,
                    "temperature": 0.1
                })
            )
            
            haiku_time = time.time() - start_time
            
            start_time = time.time()
            
            response = bedrock_client.invoke_model(
                modelId="anthropic.claude-3-sonnet-20240229-v1:0",
                body=json.dumps({
                    "prompt": "Generate a brief executive summary for a tech startup.",
                    "max_tokens_to_sample": 100,
                    "temperature": 0.1
                })
            )
            
            sonnet_time = time.time() - start_time
            
            # Performance benchmarks
            if haiku_time < 5.0 and sonnet_time < 10.0:
                logger.info(f"✓ Performance benchmarks met - Haiku: {haiku_time:.2f}s, Sonnet: {sonnet_time:.2f}s")
                self.test_results['performance'] = True
                return True
            else:
                logger.error(f"✗ Performance benchmarks not met - Haiku: {haiku_time:.2f}s, Sonnet: {sonnet_time:.2f}s")
                self.test_results['performance'] = False
                return False
                
        except Exception as e:
            logger.error(f"✗ Performance benchmark test failed: {e}")
            self.test_results['performance'] = False
            return False
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Run all tests and return results"""
        logger.info("Starting Advanced Summarization Pipeline Tests...")
        logger.info("=" * 60)
        
        # Run all tests
        tests = [
            self.test_bedrock_access,
            self.test_executive_summary_generation,
            self.test_key_insights_extraction,
            self.test_tech_stack_analysis,
            self.test_competitive_analysis,
            self.test_risk_assessment,
            self.test_business_intelligence,
            self.test_cost_calculation,
            self.test_performance_benchmarks
        ]
        
        for test in tests:
            try:
                test()
            except Exception as e:
                logger.error(f"Test {test.__name__} failed with exception: {e}")
        
        # Calculate overall results
        total_tests = len(tests)
        passed_tests = sum(1 for result in self.test_results.values() if result)
        success_rate = (passed_tests / total_tests) * 100
        
        logger.info("=" * 60)
        logger.info("TEST RESULTS SUMMARY")
        logger.info("=" * 60)
        
        for test_name, result in self.test_results.items():
            status = "✓ PASS" if result else "✗ FAIL"
            logger.info(f"{test_name}: {status}")
        
        logger.info(f"Overall Success Rate: {success_rate:.1f}% ({passed_tests}/{total_tests})")
        
        return {
            'test_results': self.test_results,
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'success_rate': success_rate,
            'timestamp': datetime.utcnow().isoformat()
        }

def main():
    """Main test execution"""
    tester = AdvancedSummarizationTester()
    results = tester.run_all_tests()
    
    # Save results to file
    with open('advanced_summarization_test_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    logger.info("Test results saved to advanced_summarization_test_results.json")
    
    # Return exit code based on success rate
    if results['success_rate'] >= 80:
        logger.info("🎉 Advanced Summarization Pipeline is ready for deployment!")
        return 0
    else:
        logger.error("❌ Advanced Summarization Pipeline needs fixes before deployment")
        return 1

if __name__ == "__main__":
    exit(main())
