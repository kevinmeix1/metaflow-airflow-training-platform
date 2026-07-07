terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

locals {
  platform_name = "metaflow-airflow-training-platform"
  node_pools    = ["system", "airflow", "training-spot"]
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = local.platform_name
  cluster_version = var.cluster_version

  cluster_endpoint_public_access = true
  enable_irsa                    = true

  cluster_compute_config = {
    enabled    = true
    node_pools = local.node_pools
  }

  tags = {
    Project     = local.platform_name
    Environment = var.environment
  }
}

resource "aws_s3_bucket" "metaflow_datastore" {
  bucket = "${local.platform_name}-${var.environment}-metaflow"
}

resource "aws_s3_bucket_versioning" "metaflow_datastore" {
  bucket = aws_s3_bucket.metaflow_datastore.id
  versioning_configuration {
    status = "Enabled"
  }
}
