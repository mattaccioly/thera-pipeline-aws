# Prompt for Cursor – Athena/Glue SQL (AWS, with Firecrawl)

You are an **Athena/Glue SQL expert**.  
Generate **DDL + SQL** to model BRONZE → SILVER → GOLD using S3 + Glue + Athena.

## Buckets & prefixes
- s3://thera-raw/         (CSV dumps: crunchbase/)
- s3://thera-curated/     (Parquet for silver/ and gold/)
- s3://thera-logs/        (optional)

## Glue databases
- `thera_bronze`, `thera_silver`, `thera_gold`

## Source tables (BRONZE)
- `bronze_crunchbase_ext` (external CSV over s3://thera-raw/crunchbase/)
- `bronze_apollo_companies` (JSON lines)
- `bronze_apollo_contacts`  (JSON lines)
- `bronze_firecrawl_pages`  (JSON lines from Firecrawl raw)

## SILVER targets (Parquet, partitioned by dt)
- `silver_companies` (canonical company row; dedupe by domain + fuzzy on name; survivorship)
- `silver_apollo_companies` (normalized)
- `silver_apollo_contacts`  (normalized; PII columns minimized/masked)
- `silver_domain_health`    (from gate job: score + flags)
- `silver_web_extracts`     (Firecrawl → compact fields)
  - company_key, domain, crawl_ts
  - title, meta_description, keywords ARRAY<STRING>
  - about_snippet (first 1k chars)
  - social_links (STRUCTs), clients_mentions ARRAY<STRING>, tech_hints ARRAY<STRING>
  - content_hash

## GOLD target (Parquet, partitioned by dt)
- `gold_startup_profiles`
  - identity: name, domain, linkedin
  - country, industry, size_bracket
  - domain_health_score, flags
  - apollo aggregates: contact_count, has_verified_contact, seniority_mix
  - web fields: title/meta/about_snippet/tags
  - profile_text (LLM-ready concatenation)
  - profile_text_hash, updated_at

## Requirements
- Provide:
  1) **CREATE EXTERNAL TABLE** for BRONZE (CSV/JSON) with SerDes
  2) **CTAS** (CREATE TABLE AS SELECT) to write SILVER/GOLD into Parquet under `s3://thera-curated/...`
  3) De-dup + survivorship rules (domain exact > linkedin > fuzzy name)
  4) Basic data quality: NOT NULL company_key/domain; coalesce country/industry
  5) Lightweight **content richness score** in `silver_web_extracts` from Firecrawl fields

Generate all SQL + comments on where to adjust S3 paths and databases.