import json
import boto3
import os
import time
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from dataclasses import dataclass
from enum import Enum

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
bedrock_client = boto3.client('bedrock-runtime')
athena_client = boto3.client('athena')
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
cloudwatch = boto3.client('cloudwatch')
ssm_client = boto3.client('ssm')

# Environment variables
CURATED_BUCKET = os.environ['CURATED_BUCKET']
ATHENA_DATABASE = os.environ['ATHENA_DATABASE']
ATHENA_WORKGROUP = os.environ.get('ATHENA_WORKGROUP', 'primary')
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE', 'thera-advanced-summaries')
DAILY_BUDGET_USD = float(os.environ.get('DAILY_BUDGET_USD', '50.0'))
MAX_BATCH_SIZE = int(os.environ.get('MAX_BATCH_SIZE', '10'))
MAX_DAILY_COMPANIES = int(os.environ.get('MAX_DAILY_COMPANIES', '1000'))

# Cost tracking
COST_PER_TOKEN_HAIKU_INPUT = 0.25 / 1_000_000  # $0.25 per 1M tokens
COST_PER_TOKEN_HAIKU_OUTPUT = 1.25 / 1_000_000  # $1.25 per 1M tokens
COST_PER_TOKEN_SONNET_INPUT = 3.00 / 1_000_000  # $3.00 per 1M tokens
COST_PER_TOKEN_SONNET_OUTPUT = 15.00 / 1_000_000  # $15.00 per 1M tokens

class TaskType(Enum):
    EXECUTIVE_SUMMARY = "executive_summary"
    KEY_INSIGHTS = "key_insights"
    TECH_ANALYSIS = "tech_analysis"
    COMPETITIVE_ANALYSIS = "competitive_analysis"
    RISK_ASSESSMENT = "risk_assessment"
    BUSINESS_INTELLIGENCE = "business_intelligence"

@dataclass
class CompanyData:
    company_key: str
    domain: str
    company_name: str
    industry: str
    description: str
    web_content: str
    profile_text: str
    content_hash: str
    last_updated: str

@dataclass
class ProcessingResult:
    company_key: str
    task_type: TaskType
    result: Dict[str, Any]
    tokens_used: int
    cost_usd: float
    processing_time_ms: int
    success: bool
    error_message: Optional[str] = None

class CostController:
    """Manages LLM costs and budget limits"""
    
    def __init__(self, daily_budget: float):
        self.daily_budget = daily_budget
        self.current_cost = 0.0
        self.daily_key = datetime.utcnow().strftime('%Y-%m-%d')
        self.cost_cache = {}
    
    def get_daily_cost(self) -> float:
        """Get current daily cost from cache or DynamoDB"""
        if self.daily_key in self.cost_cache:
            return self.cost_cache[self.daily_key]
        
        try:
            # Try to get from DynamoDB cost tracking
            table = dynamodb.Table('thera-llm-cost-tracking')
            response = table.get_item(Key={'date': self.daily_key})
            
            if 'Item' in response:
                cost = float(response['Item'].get('total_cost_usd', 0.0))
                self.cost_cache[self.daily_key] = cost
                return cost
        except Exception as e:
            logger.warning(f"Could not retrieve daily cost: {e}")
        
        return 0.0
    
    def can_process(self, estimated_cost: float) -> Tuple[bool, str]:
        """Check if we can process within budget"""
        current_cost = self.get_daily_cost()
        
        if current_cost + estimated_cost > self.daily_budget:
            return False, f"Budget exceeded: ${current_cost:.2f} + ${estimated_cost:.2f} > ${self.daily_budget:.2f}"
        
        return True, "OK"
    
    def update_cost(self, cost: float) -> None:
        """Update daily cost tracking"""
        self.current_cost += cost
        self.cost_cache[self.daily_key] = self.get_daily_cost() + cost

class CacheManager:
    """Manages caching of LLM results"""
    
    def __init__(self, table_name: str):
        self.table = dynamodb.Table(table_name)
        self.cache_ttl = 7 * 24 * 60 * 60  # 7 days in seconds
    
    def get_cached_result(self, company_key: str, task_type: TaskType, content_hash: str) -> Optional[Dict]:
        """Get cached result if available and valid"""
        try:
            cache_key = f"{company_key}#{task_type.value}#{content_hash}"
            response = self.table.get_item(Key={'cache_key': cache_key})
            
            if 'Item' in response:
                item = response['Item']
                # Check if cache is still valid
                if item.get('expires_at', 0) > int(time.time()):
                    logger.info(f"Cache hit for {company_key} - {task_type.value}")
                    return json.loads(item['result'])
                else:
                    # Cache expired, delete it
                    self.table.delete_item(Key={'cache_key': cache_key})
        except Exception as e:
            logger.warning(f"Cache retrieval error: {e}")
        
        return None
    
    def cache_result(self, company_key: str, task_type: TaskType, content_hash: str, result: Dict) -> None:
        """Cache the result"""
        try:
            cache_key = f"{company_key}#{task_type.value}#{content_hash}"
            expires_at = int(time.time()) + self.cache_ttl
            
            self.table.put_item(Item={
                'cache_key': cache_key,
                'company_key': company_key,
                'task_type': task_type.value,
                'content_hash': content_hash,
                'result': json.dumps(result),
                'created_at': int(time.time()),
                'expires_at': expires_at
            })
            logger.info(f"Cached result for {company_key} - {task_type.value}")
        except Exception as e:
            logger.warning(f"Cache storage error: {e}")

class PromptManager:
    """Manages and optimizes prompts for different tasks"""
    
    def __init__(self):
        self.prompts = self._load_prompts()
    
    def _load_prompts(self) -> Dict[TaskType, str]:
        """Load optimized prompts for each task type"""
        return {
            TaskType.EXECUTIVE_SUMMARY: """
You are a business analyst. Create a concise executive summary (2-3 sentences) for this startup company.

Company Data:
- Name: {company_name}
- Industry: {industry}
- Description: {description}
- Website Content: {web_content}

Requirements:
- Focus on what the company does and its value proposition
- Mention key differentiators or unique aspects
- Keep it professional and informative
- Maximum 3 sentences

Output as JSON: {{"executive_summary": "your summary here", "confidence": 0.85}}
""",
            
            TaskType.KEY_INSIGHTS: """
You are a business intelligence analyst. Extract 5-7 key business insights from this startup's information.

Company Data:
- Name: {company_name}
- Industry: {industry}
- Description: {description}
- Website Content: {web_content}

Analyze and extract:
1. Business model and revenue streams
2. Target market and customer segments
3. Competitive advantages
4. Growth potential indicators
5. Market positioning
6. Technology or innovation aspects
7. Strategic partnerships or relationships

Output as JSON: {{"key_insights": ["insight1", "insight2", ...], "business_model": "B2B/SaaS/etc", "value_proposition": "brief value prop", "target_customers": ["segment1", "segment2"]}}
""",
            
            TaskType.TECH_ANALYSIS: """
You are a technology analyst. Analyze the technology stack and technical capabilities of this startup.

Company Data:
- Name: {company_name}
- Industry: {industry}
- Description: {description}
- Website Content: {web_content}

Extract and analyze:
1. Primary technologies mentioned
2. Programming languages used
3. Frameworks and platforms
4. Cloud providers and infrastructure
5. Development tools and practices
6. Technical maturity level
7. Innovation indicators

Output as JSON: {{"primary_technologies": ["tech1", "tech2"], "programming_languages": ["lang1", "lang2"], "frameworks": ["framework1"], "cloud_providers": ["aws", "azure"], "tech_maturity_score": 0.8, "innovation_level": "modern"}}
""",
            
            TaskType.COMPETITIVE_ANALYSIS: """
You are a competitive intelligence analyst. Analyze this startup's competitive position and market dynamics.

Company Data:
- Name: {company_name}
- Industry: {industry}
- Description: {description}
- Website Content: {web_content}

Analyze:
1. Market position (leader, challenger, follower, niche)
2. Competitive advantages and differentiators
3. Market share indicators
4. Competitive threats and challenges
5. Moat strength and defensibility
6. Market barriers and opportunities

Output as JSON: {{"market_position": "challenger", "competitive_advantages": ["adv1", "adv2"], "differentiators": ["diff1"], "market_share_estimate": "small", "competitive_threats": ["threat1"], "moat_strength": "moderate"}}
""",
            
            TaskType.RISK_ASSESSMENT: """
You are a risk analyst. Assess potential risks and red flags for this startup.

Company Data:
- Name: {company_name}
- Industry: {industry}
- Description: {description}
- Website Content: {web_content}

Assess risks in these categories:
1. Business risks (market, competition, execution)
2. Technical risks (technology, scalability, security)
3. Financial risks (funding, revenue, costs)
4. Market risks (demand, timing, regulation)
5. Operational risks (team, processes, partnerships)

Output as JSON: {{"business_risks": ["risk1", "risk2"], "technical_risks": ["risk1"], "market_risks": ["risk1"], "financial_risks": ["risk1"], "regulatory_risks": ["risk1"], "overall_risk_level": "medium", "risk_score": 0.6, "red_flags": ["flag1", "flag2"]}}
""",
            
            TaskType.BUSINESS_INTELLIGENCE: """
You are a strategic business analyst. Provide comprehensive business intelligence insights.

Company Data:
- Name: {company_name}
- Industry: {industry}
- Description: {description}
- Website Content: {web_content}

Analyze:
1. Market opportunity size and potential
2. Growth potential and scalability indicators
3. Market timing and industry trends
4. Market dynamics and competitive landscape
5. Investment attractiveness
6. Strategic recommendations

Output as JSON: {{"market_opportunity": "large", "growth_potential": "high", "scalability_indicators": ["indicator1"], "market_timing": "optimal", "industry_trends": ["trend1"], "market_dynamics": ["dynamic1"]}}
"""
        }
    
    def get_prompt(self, task_type: TaskType, company_data: CompanyData) -> str:
        """Get optimized prompt for specific task and company"""
        prompt_template = self.prompts[task_type]
        
        # Truncate content to optimize token usage
        max_content_length = 2000
        truncated_web_content = company_data.web_content[:max_content_length] if company_data.web_content else ""
        truncated_description = company_data.description[:500] if company_data.description else ""
        
        return prompt_template.format(
            company_name=company_data.company_name,
            industry=company_data.industry or "Unknown",
            description=truncated_description,
            web_content=truncated_web_content
        )

class LLMProcessor:
    """Handles LLM processing with cost optimization"""
    
    def __init__(self):
        self.cost_controller = CostController(DAILY_BUDGET_USD)
        self.cache_manager = CacheManager(DYNAMODB_TABLE)
        self.prompt_manager = PromptManager()
    
    def process_company(self, company_data: CompanyData, task_types: List[TaskType]) -> List[ProcessingResult]:
        """Process a single company for multiple task types"""
        results = []
        
        for task_type in task_types:
            try:
                # Check cache first
                cached_result = self.cache_manager.get_cached_result(
                    company_data.company_key, task_type, company_data.content_hash
                )
                
                if cached_result:
                    results.append(ProcessingResult(
                        company_key=company_data.company_key,
                        task_type=task_type,
                        result=cached_result,
                        tokens_used=0,
                        cost_usd=0.0,
                        processing_time_ms=0,
                        success=True
                    ))
                    continue
                
                # Process with LLM
                result = self._process_with_llm(company_data, task_type)
                results.append(result)
                
                # Cache successful results
                if result.success:
                    self.cache_manager.cache_result(
                        company_data.company_key, task_type, company_data.content_hash, result.result
                    )
                
            except Exception as e:
                logger.error(f"Error processing {company_data.company_key} for {task_type.value}: {e}")
                results.append(ProcessingResult(
                    company_key=company_data.company_key,
                    task_type=task_type,
                    result={},
                    tokens_used=0,
                    cost_usd=0.0,
                    processing_time_ms=0,
                    success=False,
                    error_message=str(e)
                ))
        
        return results
    
    def _process_with_llm(self, company_data: CompanyData, task_type: TaskType) -> ProcessingResult:
        """Process single task with LLM"""
        start_time = time.time()
        
        # Choose model based on task complexity
        model_id = self._select_model(task_type)
        
        # Get optimized prompt
        prompt = self.prompt_manager.get_prompt(task_type, company_data)
        
        # Estimate cost
        estimated_tokens = len(prompt.split()) * 1.3  # Rough estimation
        estimated_cost = self._estimate_cost(model_id, estimated_tokens, estimated_tokens * 0.3)
        
        # Check budget
        can_process, reason = self.cost_controller.can_process(estimated_cost)
        if not can_process:
            raise Exception(f"Budget constraint: {reason}")
        
        try:
            # Call Bedrock
            response = bedrock_client.invoke_model(
                modelId=model_id,
                body=json.dumps({
                    "prompt": prompt,
                    "max_tokens_to_sample": 1000,
                    "temperature": 0.1,
                    "top_p": 0.9
                })
            )
            
            # Parse response
            response_body = json.loads(response['body'].read())
            result_text = response_body['completion']
            
            # Parse JSON result
            try:
                result = json.loads(result_text)
            except json.JSONDecodeError:
                # Fallback if JSON parsing fails
                result = {"raw_response": result_text}
            
            # Calculate actual cost
            input_tokens = len(prompt.split())
            output_tokens = len(result_text.split())
            actual_cost = self._calculate_cost(model_id, input_tokens, output_tokens)
            
            # Update cost tracking
            self.cost_controller.update_cost(actual_cost)
            
            processing_time = int((time.time() - start_time) * 1000)
            
            return ProcessingResult(
                company_key=company_data.company_key,
                task_type=task_type,
                result=result,
                tokens_used=input_tokens + output_tokens,
                cost_usd=actual_cost,
                processing_time_ms=processing_time,
                success=True
            )
            
        except Exception as e:
            processing_time = int((time.time() - start_time) * 1000)
            return ProcessingResult(
                company_key=company_data.company_key,
                task_type=task_type,
                result={},
                tokens_used=0,
                cost_usd=0.0,
                processing_time_ms=processing_time,
                success=False,
                error_message=str(e)
            )
    
    def _select_model(self, task_type: TaskType) -> str:
        """Select appropriate model based on task complexity"""
        # Use Claude 3 Haiku for most tasks (cost-effective)
        if task_type in [TaskType.EXECUTIVE_SUMMARY, TaskType.KEY_INSIGHTS, TaskType.TECH_ANALYSIS]:
            return "anthropic.claude-3-haiku-20240307-v1:0"
        else:
            # Use Claude 3 Sonnet for complex analysis
            return "anthropic.claude-3-sonnet-20240229-v1:0"
    
    def _estimate_cost(self, model_id: str, input_tokens: int, output_tokens: int) -> float:
        """Estimate cost for processing"""
        if "haiku" in model_id:
            return (input_tokens * COST_PER_TOKEN_HAIKU_INPUT + 
                   output_tokens * COST_PER_TOKEN_HAIKU_OUTPUT)
        else:
            return (input_tokens * COST_PER_TOKEN_SONNET_INPUT + 
                   output_tokens * COST_PER_TOKEN_SONNET_OUTPUT)
    
    def _calculate_cost(self, model_id: str, input_tokens: int, output_tokens: int) -> float:
        """Calculate actual cost for processing"""
        return self._estimate_cost(model_id, input_tokens, output_tokens)

class AdvancedSummarizationProcessor:
    """Main processor for advanced summarization"""
    
    def __init__(self):
        self.llm_processor = LLMProcessor()
        self.athena_database = ATHENA_DATABASE
        self.athena_workgroup = ATHENA_WORKGROUP
        self.curated_bucket = CURATED_BUCKET
    
    def get_companies_for_processing(self, limit: int = 100) -> List[CompanyData]:
        """Get companies that need advanced summarization"""
        try:
            query = f"""
            SELECT 
                company_key,
                domain,
                company_name,
                industry,
                description,
                web_content,
                profile_text,
                profile_text_hash as content_hash,
                updated_at
            FROM {self.athena_database}.gold_startup_profiles
            WHERE profile_text IS NOT NULL
            AND profile_text != ''
            AND updated_at > current_timestamp - interval '7' day
            ORDER BY updated_at DESC
            LIMIT {limit}
            """
            
            # Execute query
            response = athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.athena_database},
                WorkGroup=self.athena_workgroup
            )
            
            query_execution_id = response['QueryExecutionId']
            
            # Wait for completion
            while True:
                response = athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                
                status = response['QueryExecution']['Status']['State']
                
                if status in ['SUCCEEDED']:
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    raise Exception(f"Query failed: {response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')}")
                
                time.sleep(2)
            
            # Get results
            response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
            
            companies = []
            for row in response['ResultSet']['Rows'][1:]:  # Skip header
                if row['Data']:
                    data = {}
                    for i, field in enumerate(['company_key', 'domain', 'company_name', 'industry', 
                                             'description', 'web_content', 'profile_text', 'content_hash', 'updated_at']):
                        if i < len(row['Data']):
                            data[field] = row['Data'][i].get('VarCharValue', '')
                    
                    if data.get('company_key'):
                        companies.append(CompanyData(
                            company_key=data['company_key'],
                            domain=data.get('domain', ''),
                            company_name=data.get('company_name', ''),
                            industry=data.get('industry', ''),
                            description=data.get('description', ''),
                            web_content=data.get('web_content', ''),
                            profile_text=data.get('profile_text', ''),
                            content_hash=data.get('content_hash', ''),
                            last_updated=data.get('updated_at', '')
                        ))
            
            return companies
            
        except Exception as e:
            logger.error(f"Error getting companies for processing: {e}")
            return []
    
    def process_batch(self, companies: List[CompanyData]) -> List[ProcessingResult]:
        """Process a batch of companies"""
        all_results = []
        
        # Define task types to process
        task_types = [
            TaskType.EXECUTIVE_SUMMARY,
            TaskType.KEY_INSIGHTS,
            TaskType.TECH_ANALYSIS,
            TaskType.COMPETITIVE_ANALYSIS,
            TaskType.RISK_ASSESSMENT,
            TaskType.BUSINESS_INTELLIGENCE
        ]
        
        for company in companies:
            try:
                results = self.llm_processor.process_company(company, task_types)
                all_results.extend(results)
                
                # Small delay to avoid overwhelming the API
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error processing company {company.company_key}: {e}")
                continue
        
        return all_results
    
    def store_results(self, results: List[ProcessingResult], partition_date: str) -> str:
        """Store processing results in S3"""
        try:
            if not results:
                return None
            
            # Group results by company
            company_results = {}
            for result in results:
                if result.company_key not in company_results:
                    company_results[result.company_key] = {}
                company_results[result.company_key][result.task_type.value] = result.result
            
            # Create summary records
            summary_records = []
            for company_key, task_results in company_results.items():
                summary_record = {
                    'company_key': company_key,
                    'executive_summary': task_results.get('executive_summary', {}).get('executive_summary', ''),
                    'key_insights': task_results.get('key_insights', {}).get('key_insights', []),
                    'technology_stack_analysis': task_results.get('tech_analysis', {}),
                    'competitive_analysis': task_results.get('competitive_analysis', {}),
                    'risk_assessment': task_results.get('risk_assessment', {}),
                    'business_intelligence': task_results.get('business_intelligence', {}),
                    'summary_quality_score': 0.8,  # Placeholder
                    'confidence_level': 'medium',  # Placeholder
                    'llm_model_used': 'claude-3-haiku',
                    'processing_cost_usd': sum(r.cost_usd for r in results if r.company_key == company_key),
                    'created_at': datetime.utcnow().isoformat(),
                    'updated_at': datetime.utcnow().isoformat(),
                    'dt': partition_date
                }
                summary_records.append(summary_record)
            
            # Create DataFrame and store as Parquet
            df = pd.DataFrame(summary_records)
            table = pa.Table.from_pandas(df)
            
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Upload to S3
            key = f"gold/advanced_summaries/date={partition_date}/summaries_{int(time.time())}.parquet"
            
            s3_client.put_object(
                Bucket=self.curated_bucket,
                Key=key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            logger.info(f"Stored {len(summary_records)} advanced summaries to s3://{self.curated_bucket}/{key}")
            return key
            
        except Exception as e:
            logger.error(f"Error storing results: {e}")
            raise

def emit_cloudwatch_metrics(companies_processed: int, summaries_generated: int, total_cost: float) -> None:
    """Emit CloudWatch metrics"""
    try:
        cloudwatch.put_metric_data(
            Namespace='TheraPipeline/AdvancedSummarization',
            MetricData=[
                {
                    'MetricName': 'CompaniesProcessed',
                    'Value': companies_processed,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'SummariesGenerated',
                    'Value': summaries_generated,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                },
                {
                    'MetricName': 'TotalCost',
                    'Value': total_cost,
                    'Unit': 'None',
                    'Timestamp': datetime.utcnow()
                }
            ]
        )
    except Exception as e:
        logger.error(f"Error emitting metrics: {e}")

def lambda_handler(event, context):
    """Main Lambda handler for Advanced Summarization processing"""
    try:
        # Initialize processor
        processor = AdvancedSummarizationProcessor()
        
        # Get companies for processing
        companies = processor.get_companies_for_processing(MAX_DAILY_COMPANIES)
        
        if not companies:
            logger.info("No companies to process")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No companies to process',
                    'companies_processed': 0,
                    'summaries_generated': 0,
                    'total_cost': 0.0
                })
            }
        
        logger.info(f"Processing {len(companies)} companies")
        
        # Process companies in batches
        all_results = []
        total_cost = 0.0
        
        for i in range(0, len(companies), MAX_BATCH_SIZE):
            batch = companies[i:i + MAX_BATCH_SIZE]
            
            try:
                batch_results = processor.process_batch(batch)
                all_results.extend(batch_results)
                
                batch_cost = sum(r.cost_usd for r in batch_results)
                total_cost += batch_cost
                
                logger.info(f"Processed batch {i//MAX_BATCH_SIZE + 1}, companies: {len(batch)}, cost: ${batch_cost:.4f}")
                
                # Small delay between batches
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing batch {i//MAX_BATCH_SIZE + 1}: {e}")
                continue
        
        # Store results
        current_date = datetime.utcnow().strftime('%Y-%m-%d')
        parquet_key = processor.store_results(all_results, current_date)
        
        # Emit metrics
        emit_cloudwatch_metrics(len(companies), len(all_results), total_cost)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Advanced summarization processing completed successfully',
                'companies_processed': len(companies),
                'summaries_generated': len(all_results),
                'total_cost': total_cost,
                'parquet_key': parquet_key
            })
        }
        
    except Exception as e:
        logger.error(f"Error in advanced summarization processing: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

# For local testing
if __name__ == "__main__":
    test_event = {}
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))
