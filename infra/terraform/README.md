# LobbyWatch AWS Terraform

This stack provisions AWS infrastructure for the AWS-first deployment model:

- CloudFront edge routing
  - `/*` -> S3 static frontend
  - `/api/*` -> FastAPI Lambda via API Gateway HTTP API
- RDS PostgreSQL (private subnets)
- SQS + DLQ + EventBridge schedule + worker Lambda
- SSM Parameter Store secure config
- CloudWatch alarms + AWS budget alerts

## Prerequisites

- Terraform >= 1.6
- AWS credentials with permissions for Lambda, CloudFront, S3, RDS, IAM, SSM, SQS, EventBridge, Budgets, CloudWatch
- Lambda artifacts built at:
  - `dist/lambda_api.zip`
  - `dist/lambda_worker.zip`

Build artifacts:

```bash
./scripts/build_lambda_packages.sh
```

## Apply

```bash
cd infra/terraform
terraform init
terraform workspace select dev || terraform workspace new dev
terraform apply \
  -var="environment=dev" \
  -var="rds_password=CHANGE_ME" \
  -var="domain_name=lobby.watch" \
  -var="acm_certificate_arn=arn:aws:acm:us-east-1:...:certificate/..." \
  -var="lambda_api_package=../../dist/lambda_api.zip" \
  -var="lambda_worker_package=../../dist/lambda_worker.zip"
```

## Important Variables

- `enable_nat_gateway`:
  - `false` for lowest cost
  - `true` if VPC Lambdas must call external APIs reliably (Congress/Census/Zippopotam)
- `retention_years`: metadata used by worker/data tooling (default `2`)
- `ssm_secure_params`: map of additional SecureString parameters (API keys, etc.)

## Outputs

- `cloudfront_domain_name`
- `site_bucket_name`
- `api_gateway_endpoint`
- `rds_endpoint`
- `dns_instructions` for Porkbun ALIAS/ANAME setup

## Porkbun DNS

Set `lobby.watch` ALIAS/ANAME to `cloudfront_domain_name` output.
Keep Route 53 disabled for this project.
