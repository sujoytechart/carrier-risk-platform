terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # This root deliberately keeps local state: an S3 backend cannot provision
  # the bucket that stores its own state.
}
