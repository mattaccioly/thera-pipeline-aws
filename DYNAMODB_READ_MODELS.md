# Prompt for Cursor – DynamoDB Publisher

You are an **AWS DynamoDB + Lambda** engineer.  
Generate a **publisher Lambda** that upserts read models:

## Tables
- `Startups` (PK: company_key) — public fields only:
  name, domain, country, industry[], size_bracket, tags[], domain_health_score,
  contact_count, has_verified_contact, seniority_mix, title, meta_description, about_snippet,
  updated_at, schema_version
- `StartupsPrivate` (PK: company_key) — optional, masked/hashed PII aggregates

## Behavior
- Read **GOLD** delta (Parquet) from S3 where `updated_at > watermark` (watermark in SSM Parameter Store)
- BatchWriteItem with retry/backoff; idempotent upsert
- Config via env vars (table names, bucket/prefix, watermark param)
- CloudWatch metrics: upserts, skipped, errors
- Minimal IAM policy and SAM/CFN template

Provide code + template with comments.