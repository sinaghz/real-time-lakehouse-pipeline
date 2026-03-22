variable "bucket_name" {
  type        = string
  description = "The S3 bucket name passed from Docker Compose"
}

variable "aws_region" {
  type        = string
  description = "The AWS Region passed from Docker Compose"
}

provider "aws" {
  region = var.aws_region
}

resource "aws_s3_bucket" "datalake" {
  bucket        = var.bucket_name
  force_destroy = true
}