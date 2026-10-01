# Deploying the lakehouse to a cloud

The pipeline code is the same on every target. `LAKEHOUSE_TARGET` picks the
object store, the Iceberg catalog and the credentials; nothing else changes.

| Target  | Object store            | Iceberg catalog                         | Query engines that read it natively        | Status |
|---------|-------------------------|------------------------------------------|--------------------------------------------|--------|
| `local` | MinIO (docker compose)  | REST catalog (docker compose)            | DuckDB, Spark, Trino                        | verified end to end |
| `aws`   | S3                      | AWS Glue Data Catalog                    | Athena, EMR Spark, Redshift Spectrum        | Terraform applied; pipeline verified 2026-09-30: 2025q2 bronze in S3, Iceberg silver in Glue (3,409,904 facts), gold marts published to Glue and queried from Athena (`gold.company_quarter` 27,520 rows) |
| `azure` | ADLS Gen2               | pyiceberg SQL catalog (Postgres) + ADLS IO | Synapse, Fabric, Databricks               | Terraform applied (northcentralus; the student subscription's policy allows only canadacentral, westus, norwayeast, northcentralus, mexicocentral); pipeline verified: 2025q2 landed in ADLS bronze and loaded into Iceberg silver on ADLS (7,009 submissions, 3,409,904 facts) with the SQL catalog in Postgres |
| `gcp`   | GCS                     | pyiceberg SQL catalog (Postgres) + GCS IO  | BigQuery (BigLake), Dataproc              | Terraform applied (bucket `sec-lakehouse-bass990`, BigQuery dataset `sec_lakehouse_gold`); pipeline verified: 2025q2 landed in GCS bronze and loaded into Iceberg silver on GCS (3,409,904 facts) with the SQL catalog in Postgres |

Each module is small on purpose: a bucket with tiering + public-access
blocking, a catalog/dataset, least-privilege access, and a budget. No compute
is provisioned; the pipeline runs from a laptop, a GitHub Actions runner, or
any container against the cloud storage. That is a deliberate FinOps choice:
storage for the whole corpus is around 1 GB, so the monthly bill is cents.

## AWS

```bash
cd deploy/aws
terraform init
terraform apply -var account_alias=<short-unique-suffix> -var alert_email=you@example.com
export LAKEHOUSE_TARGET=aws LAKEHOUSE_BUCKET=$(terraform output -raw bucket) AWS_REGION=us-east-2
make ingest silver marts
```

Attach `pipeline_policy_arn` to the IAM user/role that runs the pipeline. Set
`S3_ENDPOINT_URL`, `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` to empty strings
for the run if a local `.env` defines them, otherwise the S3 client points at MinIO.

Athena reads the Iceberg tables straight from Glue. The workgroup needs a result
location, which can be passed per query:

```bash
aws athena start-query-execution --region us-east-2 \
  --query-string "SELECT cik, period_end, fp, revenue, net_income FROM gold.company_quarter WHERE cik = 320193 ORDER BY period_end DESC LIMIT 5" \
  --result-configuration OutputLocation=s3://<bucket>/athena-results/
```

Verified 2026-09-30: `count(*)` over `gold.company_quarter` returned 27,520 after
scanning 250 KB in 0.7 s; Apple's quarter ending 2025-03-31 came back with revenue
9.5359E10 and net income 2.478E10.

## Azure

```bash
az login
cd deploy/azure && terraform init && terraform apply -var account_alias=<3-11 lowercase alnum>
export LAKEHOUSE_TARGET=azure LAKEHOUSE_BUCKET=sec-lakehouse
export AZURE_STORAGE_ACCOUNT=$(terraform output -raw storage_account)
export AZURE_STORAGE_KEY=$(az storage account keys list -g rg-sec-lakehouse -n $AZURE_STORAGE_ACCOUNT --query "[0].value" -o tsv)
export PG_DB=lakehouse_azure     # the SQL catalog lives in the compose Postgres; one database per target
make ingest silver
```

On Azure the bronze store speaks Blob/ADLS through a small adapter with the
same four calls the S3 client exposes (`storage.AdlsClient`), and Iceberg table
metadata lives in pyiceberg's SQL catalog in Postgres with ADLS file IO. No
managed Iceberg catalog is provisioned; Synapse/Fabric/Databricks read the
tables by metadata location.

## GCP

```bash
gcloud auth application-default login
cd deploy/gcp && terraform init && terraform apply -var project=<project-id> -var account_alias=<suffix>
export LAKEHOUSE_TARGET=gcp LAKEHOUSE_BUCKET=$(terraform output -raw bucket) GOOGLE_CLOUD_PROJECT=<project-id>
export PG_DB=lakehouse_gcp       # SQL catalog database for this target
make ingest silver
```

Same shape as Azure: `storage.GcsClient` for bronze, pyiceberg SQL catalog +
GCS file IO for the tables, Application Default Credentials for auth. BigLake
external tables over the gold Parquet can be declared in the provisioned
`sec_lakehouse_gold` dataset.

## Tearing down

`terraform destroy` in the module directory. Buckets are created with
`force_destroy` so destroy works even with data in them.
