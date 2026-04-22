data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

locals {
  name_prefix          = "${var.project}-${var.environment}"
  tags                 = { Project = var.project, Environment = var.environment, ManagedBy = "terraform" }
  secure_param_prefix  = "/${var.project}/${var.environment}"
  cloudfront_aliases   = compact(concat(var.domain_name != null ? [var.domain_name] : [], var.domain_aliases))
  cloudfront_zone_id   = "Z2FDTNDATAQYW2"
}

resource "random_password" "origin_verify" {
  length  = 32
  special = false
}

resource "aws_vpc" "main" {
  cidr_block           = "10.40.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags                 = merge(local.tags, { Name = "${local.name_prefix}-vpc" })
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = merge(local.tags, { Name = "${local.name_prefix}-igw" })
}

resource "aws_subnet" "public_a" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.40.0.0/20"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true
  tags                    = merge(local.tags, { Name = "${local.name_prefix}-public-a" })
}

resource "aws_subnet" "public_b" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.40.16.0/20"
  availability_zone       = "${var.aws_region}b"
  map_public_ip_on_launch = true
  tags                    = merge(local.tags, { Name = "${local.name_prefix}-public-b" })
}

resource "aws_subnet" "private_a" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.40.32.0/20"
  availability_zone = "${var.aws_region}a"
  tags              = merge(local.tags, { Name = "${local.name_prefix}-private-a" })
}

resource "aws_subnet" "private_b" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.40.48.0/20"
  availability_zone = "${var.aws_region}b"
  tags              = merge(local.tags, { Name = "${local.name_prefix}-private-b" })
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
  tags = merge(local.tags, { Name = "${local.name_prefix}-public-rt" })
}

resource "aws_route_table_association" "public_a" {
  route_table_id = aws_route_table.public.id
  subnet_id      = aws_subnet.public_a.id
}

resource "aws_route_table_association" "public_b" {
  route_table_id = aws_route_table.public.id
  subnet_id      = aws_subnet.public_b.id
}

resource "aws_eip" "nat" {
  count  = var.enable_nat_gateway ? 1 : 0
  domain = "vpc"
  tags   = merge(local.tags, { Name = "${local.name_prefix}-nat-eip" })
}

resource "aws_nat_gateway" "main" {
  count         = var.enable_nat_gateway ? 1 : 0
  allocation_id = aws_eip.nat[0].id
  subnet_id     = aws_subnet.public_a.id
  tags          = merge(local.tags, { Name = "${local.name_prefix}-nat" })
  depends_on    = [aws_internet_gateway.main]
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  dynamic "route" {
    for_each = var.enable_nat_gateway ? [1] : []
    content {
      cidr_block     = "0.0.0.0/0"
      nat_gateway_id = aws_nat_gateway.main[0].id
    }
  }

  tags = merge(local.tags, { Name = "${local.name_prefix}-private-rt" })
}

resource "aws_route_table_association" "private_a" {
  route_table_id = aws_route_table.private.id
  subnet_id      = aws_subnet.private_a.id
}

resource "aws_route_table_association" "private_b" {
  route_table_id = aws_route_table.private.id
  subnet_id      = aws_subnet.private_b.id
}

resource "aws_security_group" "lambda" {
  name        = "${local.name_prefix}-lambda-sg"
  description = "Lambda access"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.tags
}

resource "aws_security_group" "rds" {
  name        = "${local.name_prefix}-rds-sg"
  description = "RDS access from lambda"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.tags
}

resource "aws_db_subnet_group" "main" {
  name       = "${local.name_prefix}-db-subnets"
  subnet_ids = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  tags       = local.tags
}

resource "aws_db_instance" "main" {
  identifier                 = "${local.name_prefix}-postgres"
  engine                     = "postgres"
  engine_version             = "16.3"
  instance_class             = var.rds_instance_class
  allocated_storage          = var.rds_allocated_storage
  max_allocated_storage      = var.rds_max_allocated_storage
  storage_type               = "gp3"
  db_name                    = var.rds_db_name
  username                   = var.rds_username
  password                   = var.rds_password
  db_subnet_group_name       = aws_db_subnet_group.main.name
  vpc_security_group_ids     = [aws_security_group.rds.id]
  backup_retention_period    = var.rds_backup_retention_days
  multi_az                   = false
  auto_minor_version_upgrade = true
  deletion_protection        = false
  skip_final_snapshot        = true
  publicly_accessible        = false
  apply_immediately          = true
  tags                       = local.tags
}

resource "aws_ssm_parameter" "managed" {
  for_each = var.ssm_secure_params

  name      = "${local.secure_param_prefix}/${each.key}"
  type      = "SecureString"
  value     = each.value
  overwrite = true
  tags      = local.tags
}

resource "aws_ssm_parameter" "database_url" {
  name      = "${local.secure_param_prefix}/database_url"
  type      = "SecureString"
  value     = "postgresql://${var.rds_username}:${var.rds_password}@${aws_db_instance.main.address}:5432/${var.rds_db_name}"
  overwrite = true
  tags      = local.tags
}

resource "aws_ssm_parameter" "origin_verify" {
  name      = "${local.secure_param_prefix}/cloudfront_origin_verify"
  type      = "SecureString"
  value     = random_password.origin_verify.result
  overwrite = true
  tags      = local.tags
}

resource "aws_iam_role" "lambda_api" {
  name = "${local.name_prefix}-lambda-api-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{ Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })

  tags = local.tags
}

resource "aws_iam_role" "lambda_worker" {
  name = "${local.name_prefix}-lambda-worker-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{ Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })

  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "lambda_api_basic" {
  role       = aws_iam_role.lambda_api.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_api_vpc" {
  role       = aws_iam_role.lambda_api.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_worker_basic" {
  role       = aws_iam_role.lambda_worker.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_worker_vpc" {
  role       = aws_iam_role.lambda_worker.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "lambda_api_inline" {
  name = "${local.name_prefix}-lambda-api-inline"
  role = aws_iam_role.lambda_api.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = ["ssm:GetParameter", "ssm:GetParameters", "ssm:GetParametersByPath"],
        Resource = ["arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:parameter${local.secure_param_prefix}/*"]
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_worker_inline" {
  name = "${local.name_prefix}-lambda-worker-inline"
  role = aws_iam_role.lambda_worker.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = ["ssm:GetParameter", "ssm:GetParameters", "ssm:GetParametersByPath"],
        Resource = ["arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:parameter${local.secure_param_prefix}/*"]
      },
      {
        Effect   = "Allow",
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:SendMessage"],
        Resource = [aws_sqs_queue.worker.arn, aws_sqs_queue.worker_dlq.arn]
      }
    ]
  })
}

resource "aws_sqs_queue" "worker_dlq" {
  name                      = "${local.name_prefix}-worker-dlq"
  message_retention_seconds = 1209600
  tags                      = local.tags
}

resource "aws_sqs_queue" "worker" {
  name = "${local.name_prefix}-worker"

  visibility_timeout_seconds = max(60, var.worker_timeout_seconds + 30)
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.worker_dlq.arn
    maxReceiveCount     = 5
  })
  tags = local.tags
}

resource "aws_lambda_function" "api" {
  function_name = "${local.name_prefix}-api"
  role          = aws_iam_role.lambda_api.arn
  runtime       = var.lambda_runtime
  handler       = "lambda_api.handler"
  timeout       = var.lambda_timeout_seconds
  memory_size   = var.lambda_memory_mb
  filename         = var.lambda_api_package
  source_code_hash = filebase64sha256(var.lambda_api_package)

  vpc_config {
    subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      LOBBYWATCH_ENV                  = var.environment
      DATABASE_URL_PARAM              = aws_ssm_parameter.database_url.name
      SSM_PARAM_PREFIX                = local.secure_param_prefix
      ENABLE_SSM_CONFIG               = "1"
      SERVE_FRONTEND                  = "0"
      CF_API_SHARED_SECRET_PARAM      = aws_ssm_parameter.origin_verify.name
      SQLALCHEMY_POOL_SIZE            = "1"
      SQLALCHEMY_MAX_OVERFLOW         = "0"
      SQLALCHEMY_POOL_TIMEOUT_SECONDS = "5"
      SQLALCHEMY_POOL_RECYCLE_SECONDS = "300"
    }
  }

  tags = local.tags
}

resource "aws_apigatewayv2_api" "http_api" {
  name          = "${local.name_prefix}-http-api"
  protocol_type = "HTTP"
  tags          = local.tags
}

resource "aws_apigatewayv2_integration" "api_lambda" {
  api_id                 = aws_apigatewayv2_api.http_api.id
  integration_type       = "AWS_PROXY"
  integration_method     = "POST"
  integration_uri        = aws_lambda_function.api.invoke_arn
  payload_format_version = "2.0"
  timeout_milliseconds   = 30000
}

resource "aws_apigatewayv2_route" "proxy" {
  api_id    = aws_apigatewayv2_api.http_api.id
  route_key = "ANY /{proxy+}"
  target    = "integrations/${aws_apigatewayv2_integration.api_lambda.id}"
}

resource "aws_apigatewayv2_route" "root" {
  api_id    = aws_apigatewayv2_api.http_api.id
  route_key = "ANY /"
  target    = "integrations/${aws_apigatewayv2_integration.api_lambda.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.http_api.id
  name        = "$default"
  auto_deploy = true

  default_route_settings {
    throttling_burst_limit = var.api_throttle_burst_limit
    throttling_rate_limit  = var.api_throttle_rate_limit
  }

  tags        = local.tags
}

resource "aws_lambda_permission" "api_gateway_invoke" {
  statement_id  = "AllowApiGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name           = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.http_api.execution_arn}/*/*"
}

resource "aws_lambda_function" "worker" {
  function_name = "${local.name_prefix}-worker"
  role          = aws_iam_role.lambda_worker.arn
  runtime       = var.lambda_runtime
  handler       = "lambda_worker.handler"
  timeout       = var.worker_timeout_seconds
  memory_size   = var.worker_memory_mb
  filename         = var.lambda_worker_package
  source_code_hash = filebase64sha256(var.lambda_worker_package)

  vpc_config {
    subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      LOBBYWATCH_ENV             = var.environment
      DATABASE_URL_PARAM         = aws_ssm_parameter.database_url.name
      SSM_PARAM_PREFIX           = local.secure_param_prefix
      ENABLE_SSM_CONFIG          = "1"
      WORKER_QUEUE_URL           = aws_sqs_queue.worker.id
      RETENTION_YEARS            = tostring(var.retention_years)
      CF_API_SHARED_SECRET_PARAM = aws_ssm_parameter.origin_verify.name
    }
  }

  tags = local.tags
}

resource "aws_lambda_event_source_mapping" "worker_sqs" {
  event_source_arn = aws_sqs_queue.worker.arn
  function_name    = aws_lambda_function.worker.arn
  batch_size       = 5
}

resource "aws_cloudwatch_event_rule" "worker_schedule" {
  name                = "${local.name_prefix}-worker-schedule"
  schedule_expression = var.worker_schedule_expression
  tags                = local.tags
}

resource "aws_cloudwatch_event_target" "worker_schedule_to_sqs" {
  rule      = aws_cloudwatch_event_rule.worker_schedule.name
  target_id = "queue"
  arn       = aws_sqs_queue.worker.arn
  input = jsonencode({
    task = "scheduled_ingest"
  })
}

resource "aws_sqs_queue_policy" "worker_policy" {
  queue_url = aws_sqs_queue.worker.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid       = "AllowEventBridgeSend",
        Effect    = "Allow",
        Principal = { Service = "events.amazonaws.com" },
        Action    = "sqs:SendMessage",
        Resource  = aws_sqs_queue.worker.arn,
        Condition = { ArnEquals = { "aws:SourceArn" = aws_cloudwatch_event_rule.worker_schedule.arn } }
      }
    ]
  })
}

resource "aws_s3_bucket" "site" {
  bucket        = "${local.name_prefix}-site-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
  tags          = local.tags
}

resource "aws_s3_bucket_public_access_block" "site" {
  bucket                  = aws_s3_bucket.site.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_cloudfront_origin_access_control" "site" {
  name                              = "${local.name_prefix}-oac"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_cloudfront_function" "api_path_rewrite" {
  name    = "${local.name_prefix}-api-rewrite"
  runtime = "cloudfront-js-1.0"
  publish = true
  code    = <<-EOT
function handler(event) {
  var request = event.request;
  if (request.uri.startsWith('/api/')) {
    request.uri = request.uri.substring(4);
    if (request.uri === '') {
      request.uri = '/';
    }
  } else if (request.uri === '/api') {
    request.uri = '/';
  }
  return request;
}
EOT
}

resource "aws_cloudfront_distribution" "main" {
  enabled             = true
  comment             = "${local.name_prefix} distribution"
  default_root_object = "index.html"
  aliases             = local.cloudfront_aliases

  origin {
    domain_name              = aws_s3_bucket.site.bucket_regional_domain_name
    origin_id                = "s3-site"
    origin_access_control_id = aws_cloudfront_origin_access_control.site.id
  }

  origin {
    domain_name = trimsuffix(replace(aws_apigatewayv2_api.http_api.api_endpoint, "https://", ""), "/")
    origin_id   = "lambda-api"

    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "https-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }

    custom_header {
      name  = "x-origin-verify"
      value = random_password.origin_verify.result
    }
  }

  default_cache_behavior {
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "s3-site"
    viewer_protocol_policy = "redirect-to-https"
    compress               = true

    cache_policy_id          = "658327ea-f89d-4fab-a63d-7e88639e58f6"
    origin_request_policy_id = "88a5eaf4-2fd4-4709-b370-b4c650ea3fcf"
  }

  ordered_cache_behavior {
    path_pattern           = "/api/*"
    allowed_methods        = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "lambda-api"
    viewer_protocol_policy = "redirect-to-https"
    compress               = true

    cache_policy_id          = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"
    origin_request_policy_id = "216adef6-5c7f-47e4-b989-5492eafa07d3"

    function_association {
      event_type   = "viewer-request"
      function_arn = aws_cloudfront_function.api_path_rewrite.arn
    }
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = var.acm_certificate_arn == null
    acm_certificate_arn            = var.acm_certificate_arn
    ssl_support_method             = var.acm_certificate_arn == null ? null : "sni-only"
    minimum_protocol_version       = var.acm_certificate_arn == null ? "TLSv1" : "TLSv1.2_2021"
  }

  custom_error_response {
    error_code            = 403
    response_code         = 200
    response_page_path    = "/index.html"
    error_caching_min_ttl = 0
  }

  custom_error_response {
    error_code            = 404
    response_code         = 200
    response_page_path    = "/index.html"
    error_caching_min_ttl = 0
  }

  tags = local.tags
}

resource "aws_s3_bucket_policy" "site" {
  bucket = aws_s3_bucket.site.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid = "AllowCloudFrontRead",
        Effect = "Allow",
        Principal = { Service = "cloudfront.amazonaws.com" },
        Action   = ["s3:GetObject"],
        Resource = ["${aws_s3_bucket.site.arn}/*"],
        Condition = {
          StringEquals = {
            "AWS:SourceArn" = aws_cloudfront_distribution.main.arn
          }
        }
      }
    ]
  })
}

resource "aws_cloudwatch_metric_alarm" "api_lambda_errors" {
  alarm_name          = "${local.name_prefix}-api-lambda-errors"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"

  dimensions = {
    FunctionName = aws_lambda_function.api.function_name
  }

  alarm_description = "API Lambda error count >= 1"
  tags              = local.tags
}

resource "aws_cloudwatch_metric_alarm" "worker_dlq_messages" {
  alarm_name          = "${local.name_prefix}-worker-dlq-messages"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"

  dimensions = {
    QueueName = aws_sqs_queue.worker_dlq.name
  }

  alarm_description = "Worker DLQ has visible messages"
  tags              = local.tags
}

resource "aws_cloudwatch_metric_alarm" "rds_cpu" {
  alarm_name          = "${local.name_prefix}-rds-cpu-high"
  namespace           = "AWS/RDS"
  metric_name         = "CPUUtilization"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 2
  threshold           = 80
  comparison_operator = "GreaterThanOrEqualToThreshold"

  dimensions = {
    DBInstanceIdentifier = aws_db_instance.main.id
  }

  alarm_description = "RDS CPU utilization high"
  tags              = local.tags
}

resource "aws_budgets_budget" "monthly" {
  name              = "${local.name_prefix}-monthly-budget"
  budget_type       = "COST"
  limit_amount      = var.monthly_budget_limit_usd
  limit_unit        = "USD"
  time_unit         = "MONTHLY"
  time_period_start = "2025-01-01_00:00"

  dynamic "notification" {
    for_each = toset(length(var.budget_alert_emails) > 0 ? [50, 100, 150] : [])
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = var.budget_alert_emails
    }
  }
}
