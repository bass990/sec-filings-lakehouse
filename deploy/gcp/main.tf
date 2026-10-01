# GCP target: a GCS bucket (bronze + Iceberg warehouse) and a BigQuery dataset
# in which BigLake external Iceberg tables can be declared over the same files.
# Iceberg tables use pyiceberg's SQL catalog (Postgres) with GCS file IO.
#
#   gcloud auth application-default login
#   terraform init && terraform apply -var project=<project-id> -var account_alias=<yours>
#   export LAKEHOUSE_TARGET=gcp LAKEHOUSE_BUCKET=$(terraform output -raw bucket)

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = { source = "hashicorp/google", version = "~> 6.0" }
  }
}

variable "project" { type = string }
variable "region" { default = "us-central1" }
variable "account_alias" { type = string }
variable "budget_usd" { default = 10 }

provider "google" {
  project = var.project
  region  = var.region
}

resource "google_storage_bucket" "lakehouse" {
  name                        = "sec-lakehouse-${var.account_alias}"
  location                    = upper(var.region)
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true
  public_access_prevention    = "enforced"

  lifecycle_rule {
    condition {
      age            = 30
      matches_prefix = ["bronze/"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  labels = { project = "sec-lakehouse" }
}

resource "google_bigquery_dataset" "gold" {
  dataset_id  = "sec_lakehouse_gold"
  location    = upper(var.region)
  description = "BigLake external Iceberg tables over the gold marts in GCS."
  labels      = { project = "sec-lakehouse" }
}

output "bucket" { value = google_storage_bucket.lakehouse.name }
output "warehouse" { value = "gs://${google_storage_bucket.lakehouse.name}/warehouse" }
output "bigquery_dataset" { value = google_bigquery_dataset.gold.dataset_id }
