terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # State is local through Phase 0. The remote backend cannot create the bucket
  # that holds its own state, so it is bootstrapped separately in Phase 2.
}
