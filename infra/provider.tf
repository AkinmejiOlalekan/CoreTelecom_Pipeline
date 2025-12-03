terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = ">= 1.0.0"
    }
  }
}

provider "aws" {
  region = "eu-north-1"
}

provider "snowflake" {
  organization_name = "MEWTCQX"
  account_name      = "LP86232"
  user              = "Akinsj6"
  password          = var.password
  role              = var.snowflake_admin_role
  preview_features_enabled  = ["snowflake_storage_integration_resource", "snowflake_file_format_resource", "snowflake_stage_resource"]  
}