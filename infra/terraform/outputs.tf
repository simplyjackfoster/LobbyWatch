output "cloudfront_domain_name" {
  value       = aws_cloudfront_distribution.main.domain_name
  description = "CloudFront distribution domain"
}

output "cloudfront_hosted_zone_id" {
  value       = local.cloudfront_zone_id
  description = "Hosted zone ID for ALIAS/ANAME records"
}

output "api_gateway_endpoint" {
  value       = aws_apigatewayv2_api.http_api.api_endpoint
  description = "API Gateway endpoint backing /api/*"
}

output "site_bucket_name" {
  value       = aws_s3_bucket.site.id
  description = "S3 bucket for static frontend artifacts"
}

output "rds_endpoint" {
  value       = aws_db_instance.main.address
  description = "RDS PostgreSQL endpoint"
}

output "rds_port" {
  value = aws_db_instance.main.port
}

output "worker_queue_url" {
  value = aws_sqs_queue.worker.id
}

output "dns_instructions" {
  value = {
    apex_alias_target = aws_cloudfront_distribution.main.domain_name
    apex_alias_zone   = local.cloudfront_zone_id
    note              = "Configure Porkbun ALIAS/ANAME to CloudFront domain."
  }
}

output "ssm_prefix" {
  value = local.secure_param_prefix
}
