# Fourteen resources most Terraform configurations start from, the set the roadmap
# review (#1597) applied against substrate. Each is written the way a module
# commonly writes it, rather than trimmed to what substrate is known to accept.

locals {
  tags = {
    Project = var.name
  }
}

# --- Network ------------------------------------------------------------------

resource "aws_vpc" "main" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = merge(local.tags, { Name = "${var.name}-vpc" })
}

resource "aws_subnet" "main" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.42.1.0/24"
  availability_zone = "${var.region}a"
  tags              = merge(local.tags, { Name = "${var.name}-subnet" })
}

resource "aws_security_group" "app" {
  name        = "${var.name}-app"
  description = "HTTPS within the VPC"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "HTTPS from the VPC"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  egress {
    description = "HTTPS to the VPC"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  tags = local.tags
}

# --- Network controls (#1614) -------------------------------------------------
# The default network ACL every VPC comes with, a custom ACL with an inline rule and
# a standalone one, a DHCP option set associated with the VPC, and a flow log. Each
# is recorded and read back; substrate enforces no rule and delivers no flow log.

resource "aws_default_network_acl" "main" {
  default_network_acl_id = aws_vpc.main.default_network_acl_id

  ingress {
    protocol   = -1
    rule_no    = 100
    action     = "allow"
    cidr_block = "0.0.0.0/0"
    from_port  = 0
    to_port    = 0
  }

  egress {
    protocol   = -1
    rule_no    = 100
    action     = "allow"
    cidr_block = "0.0.0.0/0"
    from_port  = 0
    to_port    = 0
  }

  # The subnets move to aws_network_acl.private, which manages their association.
  lifecycle {
    ignore_changes = [subnet_ids]
  }

  tags = local.tags
}

resource "aws_network_acl" "private" {
  vpc_id     = aws_vpc.main.id
  subnet_ids = [aws_subnet.main.id]
  tags       = merge(local.tags, { Name = "${var.name}-private" })
}

resource "aws_network_acl_rule" "https_in" {
  network_acl_id = aws_network_acl.private.id
  rule_number    = 100
  egress         = false
  protocol       = "tcp"
  rule_action    = "allow"
  cidr_block     = aws_vpc.main.cidr_block
  from_port      = 443
  to_port        = 443
}

resource "aws_network_acl_rule" "ephemeral_out" {
  network_acl_id = aws_network_acl.private.id
  rule_number    = 100
  egress         = true
  protocol       = "tcp"
  rule_action    = "allow"
  cidr_block     = aws_vpc.main.cidr_block
  from_port      = 1024
  to_port        = 65535
}

resource "aws_vpc_dhcp_options" "main" {
  domain_name         = "${var.name}.internal"
  domain_name_servers = ["AmazonProvidedDNS"]
  tags                = local.tags
}

resource "aws_vpc_dhcp_options_association" "main" {
  vpc_id          = aws_vpc.main.id
  dhcp_options_id = aws_vpc_dhcp_options.main.id
}

data "aws_iam_policy_document" "assume_flow_logs" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["vpc-flow-logs.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "flow_logs" {
  name               = "${var.name}-flow-logs"
  assume_role_policy = data.aws_iam_policy_document.assume_flow_logs.json
  tags               = local.tags
}

resource "aws_cloudwatch_log_group" "flow_logs" {
  name              = "/vpc/${var.name}-flow-logs"
  retention_in_days = 7
  tags              = local.tags
}

resource "aws_flow_log" "vpc" {
  vpc_id          = aws_vpc.main.id
  traffic_type    = "REJECT"
  iam_role_arn    = aws_iam_role.flow_logs.arn
  log_destination = aws_cloudwatch_log_group.flow_logs.arn
  tags            = local.tags
}

# --- IAM ----------------------------------------------------------------------

data "aws_iam_policy_document" "assume_lambda" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "handler" {
  name               = "${var.name}-handler"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda.json
  tags               = local.tags
}

data "aws_iam_policy_document" "handler" {
  statement {
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.jobs.arn]
  }
  statement {
    actions   = ["dynamodb:PutItem", "dynamodb:GetItem"]
    resources = [aws_dynamodb_table.items.arn]
  }
}

resource "aws_iam_policy" "handler" {
  name   = "${var.name}-handler"
  policy = data.aws_iam_policy_document.handler.json
  tags   = local.tags
}

resource "aws_iam_role_policy_attachment" "handler" {
  role       = aws_iam_role.handler.name
  policy_arn = aws_iam_policy.handler.arn
}

# --- Storage and messaging ----------------------------------------------------

resource "aws_kms_key" "main" {
  description             = "${var.name} data key"
  deletion_window_in_days = 7
  enable_key_rotation     = true
  tags                    = local.tags
}

resource "aws_s3_bucket" "artifacts" {
  bucket = "${var.name}-artifacts"
  tags   = local.tags
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.main.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_sqs_queue" "jobs" {
  name                       = "${var.name}-jobs"
  visibility_timeout_seconds = 60
  message_retention_seconds  = 86400
  kms_master_key_id          = aws_kms_key.main.id
  tags                       = local.tags
}

resource "aws_sns_topic" "alerts" {
  name              = "${var.name}-alerts"
  kms_master_key_id = aws_kms_key.main.id
  tags              = local.tags
}

resource "aws_dynamodb_table" "items" {
  name         = "${var.name}-items"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.main.arn
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = local.tags
}

resource "aws_secretsmanager_secret" "api" {
  name                    = "${var.name}/api-key"
  description             = "API key the handler reads"
  kms_key_id              = aws_kms_key.main.id
  recovery_window_in_days = 0
  tags                    = local.tags
}

# --- Observability and events -------------------------------------------------

resource "aws_cloudwatch_log_group" "handler" {
  name              = "/aws/lambda/${var.name}-handler"
  retention_in_days = 14
  kms_key_id        = aws_kms_key.main.arn
  tags              = local.tags
}

resource "aws_cloudwatch_metric_alarm" "queue_depth" {
  alarm_name          = "${var.name}-queue-depth"
  alarm_description   = "Jobs are backing up"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.jobs.name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 100
  comparison_operator = "GreaterThanThreshold"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  tags                = local.tags
}

resource "aws_cloudwatch_event_rule" "nightly" {
  name                = "${var.name}-nightly"
  description         = "Nightly batch"
  schedule_expression = "cron(0 3 * * ? *)"
  tags                = local.tags
}

# --- Compute ------------------------------------------------------------------

# handler.zip holds handler/index.mjs. Substrate records the package and never runs
# it, so the code only has to be a valid archive.
resource "aws_lambda_function" "handler" {
  function_name    = "${var.name}-handler"
  role             = aws_iam_role.handler.arn
  runtime          = "nodejs22.x"
  handler          = "index.handler"
  filename         = "${path.module}/handler.zip"
  source_code_hash = filebase64sha256("${path.module}/handler.zip")
  timeout          = 30
  memory_size      = 256

  tracing_config {
    mode = "Active"
  }

  environment {
    variables = {
      TABLE_NAME = aws_dynamodb_table.items.name
      QUEUE_URL  = aws_sqs_queue.jobs.url
    }
  }

  depends_on = [aws_iam_role_policy_attachment.handler, aws_cloudwatch_log_group.handler]
  tags       = local.tags
}
