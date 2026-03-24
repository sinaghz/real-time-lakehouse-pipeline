variable "bucket_name" {
  type        = string
  description = "The S3 bucket name passed from Docker Compose"
}

variable "aws_region" {
  type        = string
  description = "The AWS Region passed from Docker Compose"
}

variable "environment" {
  type        = string
  description = "Deployment Environment"
}

variable "project_name" {
  type        = string
  description = "Project Name"
}

variable "agg_db_name" {
  type        = string
  description = "Agg Database Name"
}

variable "raw_db_name" {
  type        = string
  description = "Raw Database Name"
}


provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Environment = var.environment
      Project     = var.project_name
      ManagedBy   = "Terraform"
    }
  }
}

resource "aws_s3_bucket" "datalake" {
  bucket        = var.bucket_name
  force_destroy = true
}

resource "aws_cloudwatch_log_group" "flink_pipeline_logs" {
  name              = "/ecs/${var.project_name}"
  retention_in_days = 7
}

resource "aws_glue_catalog_database" "raw_db" {
  name = var.raw_db_name
}

resource "aws_glue_catalog_database" "agg_db" {
  name = var.agg_db_name
}