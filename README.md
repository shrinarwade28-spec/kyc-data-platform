# KYC Data Platform

End-to-end AWS-native data engineering pipeline for KYC data ingestion,
PII masking, data quality checks, and analytics-ready outputs.

## Architecture
- Source: MySQL
- Ingestion: Python / AWS Glue
- Storage: Amazon S3 (Raw, Masked, Curated zones)
- Security: PII masking, IAM, encryption
- CI/CD: GitHub + AWS CodeBuild
