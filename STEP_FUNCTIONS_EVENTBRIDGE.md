# Prompt for Cursor – AWS Step Functions (Serverless DAG)

You are an **AWS Step Functions** expert.  
Generate a **State Machine (ASL JSON/YAML)** and **EventBridge schedules** to orchestrate the pipeline with constraints:

## Daily (01:00 America/Sao_Paulo)
1) Apollo Delta Pull (Lambda) with throttling (50/min; 200/hour; 600/day)
2) Athena CTAS: BRONZE→SILVER (companies + apollo)
3) Domain-Health Gate (Lambda)
4) Firecrawl Orchestrator (Lambda) with **Map/Iterator** and **MaxConcurrency: 2**
   - Select domains where domain_health_score ≥ 70 and (content_hash is null or drifted)
   - Call Firecrawl with tight limits: maxDepth=1, maxPages≤3, timeoutMs
5) Athena CTAS to build `silver_web_extracts` and `gold_startup_profiles`
6) Embeddings batch (Lambda using Bedrock) for rows where profile_text_hash changed
7) Compute AMS (Lambda or Athena INSERT OVERWRITE)
8) DynamoDB Publisher (Lambda) to upsert read models

## Weekly (Mon 03:00 America/Sao_Paulo)
- Train logistic regression (Lambda container with scikit-learn), read events from S3/Athena, write model params JSON to S3
- Write evaluation metrics (AUC/PR) to S3/Glue table

## Requirements
- Use **EventBridge** rules to trigger the daily and weekly executions.
- Use **Retry** with backoff for each task.
- Pass SSM Parameter Store/Secrets Manager ARNs for Apollo + Firecrawl keys.
- Output execution metrics to CloudWatch Logs.

Produce:
- State Machine definition (JSON/YAML)
- Example EventBridge rules (CLI or CloudFormation snippets)
- IAM notes for the state machine role (invoke Lambda, start/query Athena, read/write S3, decrypt secrets).
