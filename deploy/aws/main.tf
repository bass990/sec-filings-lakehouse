# AWS target for the SEC lakehouse: S3 (bronze + Iceberg warehouse), Glue Data
# Catalog (Iceberg catalog Athena/Spark/pyiceberg all read), least-privilege IAM
# for the pipeline, lifecycle rules, and a budget alarm.
#
#   terraform init && terraform apply -var account_alias=<yours>
#   export LAKEHOUSE_TARGET=aws LAKEHOUSE_BUCKET=$(terraform output -raw bucket)
#
# Athena reads gold.* directly from the Glue catalog once the pipeline has run
# with LAKEHOUSE_TARGET=aws. Cost at portfolio scale: cents/month (S3 storage
# for ~1 GB, Glue catalog free tier, Athena $5/TB scanned).

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.60" }
  }
}

provider "aws" {
  region = var.region
}

variable "region" { default = "us-east-2" }
variable "account_alias" {
  description = "Short unique suffix for the bucket name (bucket names are global)."
  type        = string
}
variable "budget_usd" {
  description = "Monthly budget alert threshold."
  default     = 10
}
variable "alert_email" {
  description = "Where budget alerts go."
  type        = string
  default     = ""
}

data "aws_caller_identity" "me" {}

resource "aws_s3_bucket" "lakehouse" {
  bucket        = "sec-lakehouse-${var.account_alias}"
  force_destroy = true
  tags          = { project = "sec-lakehouse" }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

# bronze zips are read once at load time; tier them down quickly. Iceberg data files stay hot.
resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    id     = "bronze-to-ia"
    status = "Enabled"
    filter { prefix = "bronze/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    noncurrent_version_expiration { noncurrent_days = 30 }
  }
  rule {
    id     = "abort-multipart"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 2 }
  }
}

resource "aws_glue_catalog_database" "silver" {
  name         = "silver"
  location_uri = "s3://${aws_s3_bucket.lakehouse.bucket}/warehouse/silver"
}

resource "aws_glue_catalog_database" "gold" {
  name         = "gold"
  location_uri = "s3://${aws_s3_bucket.lakehouse.bucket}/warehouse/gold"
}

# least-privilege policy for whatever runs the pipeline (attach to a user or a role)
data "aws_iam_policy_document" "pipeline" {
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lakehouse.arn]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.lakehouse.arn}/*"]
  }
  statement {
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase",
      "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.me.account_id}:catalog",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.me.account_id}:database/*",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.me.account_id}:table/*/*",
    ]
  }
}

resource "aws_iam_policy" "pipeline" {
  name   = "sec-lakehouse-pipeline"
  policy = data.aws_iam_policy_document.pipeline.json
}

resource "aws_budgets_budget" "monthly" {
  name         = "sec-lakehouse-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  dynamic "notification" {
    for_each = var.alert_email == "" ? [] : [1]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = 80
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = [var.alert_email]
    }
  }
}

output "bucket" { value = aws_s3_bucket.lakehouse.bucket }
output "warehouse" { value = "s3://${aws_s3_bucket.lakehouse.bucket}/warehouse" }
output "pipeline_policy_arn" { value = aws_iam_policy.pipeline.arn }
output "glue_databases" { value = [aws_glue_catalog_database.silver.name, aws_glue_catalog_database.gold.name] }
