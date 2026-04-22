variable "aws_region" {
  description = "AWS region for core infra"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project slug"
  type        = string
  default     = "lobbywatch"
}

variable "environment" {
  description = "Environment name (dev/prod)"
  type        = string
}

variable "domain_name" {
  description = "Primary custom domain managed externally (optional)"
  type        = string
  default     = null
}

variable "domain_aliases" {
  description = "Additional aliases on CloudFront"
  type        = list(string)
  default     = []
}

variable "acm_certificate_arn" {
  description = "ACM certificate ARN in us-east-1 for CloudFront (optional)"
  type        = string
  default     = null
}

variable "enable_nat_gateway" {
  description = "Enable NAT for outbound internet from VPC Lambdas"
  type        = bool
  default     = false
}

variable "rds_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t4g.micro"
}

variable "rds_allocated_storage" {
  description = "RDS storage in GB"
  type        = number
  default     = 20
}

variable "rds_max_allocated_storage" {
  description = "RDS storage autoscaling max GB"
  type        = number
  default     = 100
}

variable "rds_db_name" {
  description = "Primary DB name"
  type        = string
  default     = "lobbywatch"
}

variable "rds_username" {
  description = "RDS admin username"
  type        = string
  default     = "lobbywatch"
}

variable "rds_password" {
  description = "RDS admin password"
  type        = string
  sensitive   = true
}

variable "rds_backup_retention_days" {
  description = "RDS backup retention period"
  type        = number
  default     = 7
}

variable "lambda_api_package" {
  description = "Path to API Lambda zip package"
  type        = string
  default     = "dist/lambda_api.zip"
}

variable "lambda_worker_package" {
  description = "Path to worker Lambda zip package"
  type        = string
  default     = "dist/lambda_worker.zip"
}

variable "lambda_runtime" {
  description = "Lambda runtime"
  type        = string
  default     = "python3.12"
}

variable "lambda_memory_mb" {
  description = "API Lambda memory"
  type        = number
  default     = 1024
}

variable "lambda_timeout_seconds" {
  description = "API Lambda timeout"
  type        = number
  default     = 30
}

variable "worker_memory_mb" {
  description = "Worker Lambda memory"
  type        = number
  default     = 1024
}

variable "worker_timeout_seconds" {
  description = "Worker Lambda timeout"
  type        = number
  default     = 300
}

variable "worker_schedule_expression" {
  description = "EventBridge schedule for ingest queueing"
  type        = string
  default     = "rate(12 hours)"
}

variable "api_throttle_rate_limit" {
  description = "API Gateway steady-state requests per second across routes"
  type        = number
  default     = 20
}

variable "api_throttle_burst_limit" {
  description = "API Gateway burst request limit across routes"
  type        = number
  default     = 40
}

variable "monthly_budget_limit_usd" {
  description = "Monthly budget limit in USD"
  type        = string
  default     = "20"
}

variable "budget_alert_emails" {
  description = "Budget alert email recipients"
  type        = list(string)
  default     = []
}

variable "ssm_secure_params" {
  description = "Secure SSM params written by Terraform"
  type        = map(string)
  default     = {}
  sensitive   = true
}

variable "retention_years" {
  description = "Fact-table retention years in hosted RDS"
  type        = number
  default     = 2
}
