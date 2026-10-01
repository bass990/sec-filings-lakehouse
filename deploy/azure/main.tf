# Azure target: a resource group, a Data Lake Storage Gen2 account (hierarchical
# namespace, which Iceberg/ADLS clients expect) with one filesystem for bronze +
# warehouse, and a budget. Iceberg tables use pyiceberg's SQL catalog (Postgres,
# see deploy/README.md) with ADLS file IO; Synapse/Fabric/Databricks can read the
# same files.
#
#   az login && terraform init && terraform apply -var account_alias=<yours>
#   export LAKEHOUSE_TARGET=azure AZURE_STORAGE_ACCOUNT=$(terraform output -raw storage_account)

terraform {
  required_version = ">= 1.6"
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 4.0" }
  }
}

provider "azurerm" {
  features {}
}

# Azure for Students subscriptions carry an "Allowed resource deployment regions" policy; as of 2026-09 it allows
# canadacentral, westus, norwayeast, northcentralus, mexicocentral. Check: az policy assignment list --disable-scope-strict-match
variable "location" { default = "northcentralus" }
variable "account_alias" {
  type        = string
  description = "3-11 lowercase alphanumerics; storage account names are global."
}
variable "budget_usd" { default = 10 }
variable "alert_email" {
  type    = string
  default = ""
}

resource "azurerm_resource_group" "lakehouse" {
  name     = "rg-sec-lakehouse"
  location = var.location
  tags     = { project = "sec-lakehouse" }
}

resource "azurerm_storage_account" "lakehouse" {
  name                     = "seclakehouse${var.account_alias}"
  resource_group_name      = azurerm_resource_group.lakehouse.name
  location                 = azurerm_resource_group.lakehouse.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # ADLS Gen2
  min_tls_version          = "TLS1_2"
  allow_nested_items_to_be_public = false

  blob_properties {
    versioning_enabled = false
    delete_retention_policy { days = 7 }
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "lakehouse" {
  name               = "sec-lakehouse"
  storage_account_id = azurerm_storage_account.lakehouse.id
}

resource "azurerm_storage_management_policy" "tiering" {
  storage_account_id = azurerm_storage_account.lakehouse.id
  rule {
    name    = "bronze-to-cool"
    enabled = true
    filters {
      prefix_match = ["sec-lakehouse/bronze/"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob { tier_to_cool_after_days_since_modification_greater_than = 30 }
    }
  }
}

resource "azurerm_consumption_budget_resource_group" "monthly" {
  name              = "sec-lakehouse-monthly"
  resource_group_id = azurerm_resource_group.lakehouse.id
  amount            = var.budget_usd
  time_grain        = "Monthly"
  time_period {
    start_date = formatdate("YYYY-MM-01'T'00:00:00Z", timestamp())
  }
  dynamic "notification" {
    for_each = var.alert_email == "" ? [] : [1]
    content {
      enabled        = true
      threshold      = 80
      operator       = "GreaterThan"
      contact_emails = [var.alert_email]
    }
  }
  lifecycle { ignore_changes = [time_period] }
}

output "resource_group" { value = azurerm_resource_group.lakehouse.name }
output "storage_account" { value = azurerm_storage_account.lakehouse.name }
output "filesystem" { value = azurerm_storage_data_lake_gen2_filesystem.lakehouse.name }
output "warehouse" { value = "abfss://${azurerm_storage_data_lake_gen2_filesystem.lakehouse.name}@${azurerm_storage_account.lakehouse.name}.dfs.core.windows.net/warehouse" }
