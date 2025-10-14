# Main terraform configurations
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.99"
    }
  }
# Back end s3 bucket for tfstate 
  backend "s3" {
    bucket = "dev-gdpr-obfuscate-bucket"
    key    = "tf_state/project_gdpr.tfstate"
    region = "us-east-1"
  }
}

# Configures the AWS provider
provider "aws" {
  region = "us-east-1"
  default_tags {
    tags = {
      project_name = "gdpr-obfuscate-project"
      Environment  = var.environment
    }
  }
}