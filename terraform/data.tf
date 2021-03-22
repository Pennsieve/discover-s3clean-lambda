data "aws_region" "current_region" {}

// IMPORT ACCOUNT DATA
data "terraform_remote_state" "account" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/terraform.tfstate"
    region = "us-east-1"
  }
}

// IMPORT VPC DATA
data "terraform_remote_state" "vpc" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/terraform.tfstate"
    region = "us-east-1"
  }
}

/*
// IMPORT DISCOVER SERVICE DATA
data "terraform_remote_state" "discover_service" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/discover-service/terraform.tfstate"
    region = "us-east-1"
  }
}
*/


// IMPORT PLATFORM INFRASTRUCTURE DATA
data "terraform_remote_state" "platform_infrastructure" {
  backend = "s3"

  config = {
    bucket = "${var.aws_account}-terraform-state"
    key    = "aws/${data.aws_region.current_region.name}/${var.vpc_name}/${var.environment_name}/platform-infrastructure/terraform.tfstate"
    region = "us-east-1"
  }
}

// IMPORT LAMBDA S3 BUCKET OBJECT
data "aws_s3_bucket_object" "s3_bucket_object" {
  bucket = var.bucket
  key    = "${var.service_name}-${var.tier}/${var.service_name}-${var.tier}-${var.version_number}.zip"
}
