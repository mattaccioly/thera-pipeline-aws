# Prompt for Cursor – AWS Lambda Jobs (Python)

You are a **Python on AWS** engineer.  
Generate **Lambda function code + SAM/CloudFormation templates** for:

## A) Apollo Delta Puller (rate-limited)
- Reads API key from Secrets Manager.
- Respects 50/min, 200/hour, 600/day:
  - Token bucket persisted in DynamoDB (`apollo_quota` item with rolling counters + reset times).
  - If quota would be exceeded, exit gracefully and let next EventBridge trigger continue.
- Pull companies then contacts (delta by updated_at), store raw JSONL to `s3://thera-raw/apollo/date=.../`
- Append normalized rows to S3 `bronze_apollo_*` folders (JSONL)
- Emit CloudWatch metrics (items fetched, calls used).

## B) Domain-Health Gate
- Input query (via Athena or S3 manifest) for domains needing checks
- For each domain: DNS → HEAD → TLS → small GET (≤20KB)
- Compute `domain_health_score` + flags, write JSONL to `s3://thera-curated/silver/domain_health/date=.../`
- Tight timeouts (≤2s call), concurrency with `asyncio`, capped at N.

## C) Firecrawl Orchestrator (concurrency ≤ 2)
- Source selection: read list of candidate domains (Athena query results in S3)
- Use **Step Functions Map MaxConcurrency=2** OR **SQS + Lambda with reserved concurrency=2**
- For each domain:
  - call Firecrawl (API key from Secrets) with `{maxDepth:1,maxPages:<=3,timeoutMs,onlyMainContent:true}`
  - store raw response JSONL → `s3://thera-raw/firecrawl/date=.../`
- Optionally do light normalization and emit `content_hash`
- Handle 429/5xx with exponential backoff; circuit-break on sustained failures.

## D) Embeddings Batch (Bedrock)
- For GOLD rows where `profile_text_hash` changed, call **Bedrock Titan embeddings** in batches.
- Write Parquet `embeddings` as rows: {company_key, vector (array<float>), updated_at} to `s3://thera-curated/embeddings/`
- Cost guardrails: throttle TPS; stop after N items if daily budget exceeded (config var).

## E) Matcher + AMS
- **Matcher** Lambda: given a challenge text:
  - Call Bedrock embeddings (single call)
  - Athena pre-filter (industry/country) to pull ≤ 5k candidates (company_key + vectors)
  - Compute cosine similarity in-memory; produce top-20 with rule features + ML score (see model below)
  - Return scores + reason strings
- **AMS** Lambda (daily):
  - Read prior day’s shortlists & scores from S3/events
  - Compute `AMS_challenge = AVG(final_score ranks 1..10)`
  - Write partitioned Parquet into `s3://thera-curated/metrics/ams/`

## F) Reader Publisher (DynamoDB)
- Upsert **public** items to `Startups` table (no PII)
- Optional **private** items to `StartupsPrivate` table (masked/hashed PII)
- Incremental by `updated_at` watermark stored in SSM Parameter Store.

## G) Trainer (Weekly, scikit-learn)
- Reads events (shortlisted/contacted within 14 days) from S3/Athena
- Trains logistic regression on: sim_embedding, industry/geo overlaps, Apollo aggregates, Firecrawl content_richness
- Saves `model.json` (coefficients, intercept, feature order) to `s3://thera-curated/models/match_lr/model.json`
- Inference in Matcher = simple dot product + sigmoid (no SageMaker required)

## Deliverables
- Lambda code (Python 3.12), requirements.txt, and SAM/CloudFormation templates for easy deploy
- Environment vars for buckets, databases, table names, region
- IAM notes: S3 read/write, Athena StartQueryExecution/GetQueryResults, DynamoDB access, Secrets, SSM.
