# Prompt for Cursor – Training + Scoring + AMS

You are an **ML on AWS** engineer.  
Produce:
1) **Weekly Trainer (Python)**: scikit-learn logistic regression as described; read data via Athena UNLOAD (to S3), train, write `model.json` to S3, write eval (AUC/PR) to S3 as JSON.
2) **Matcher scoring snippet** (Python): load `model.json`, compute ML score, combine with:
   - final_score = 0.6*sim_embedding + 0.2*industry_geo + 0.1*apollo + 0.1*ml_pred
3) **AMS job (Python)**: compute Top-10 average per challenge and write Parquet to `s3://.../metrics/ams/` with Glue table DDL.
4) **Athena DDL** for:
   - `events_matching` (partitioned by dt)
   - `metrics_ams` (partitioned by dt)

Keep it small, dependency-light, and Lambda-friendly.