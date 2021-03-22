variable "aws_region" {}

variable "aws_account" {}

variable "environment_name" {}

variable "version_number" {}

variable "vpc_name" {}

variable "description" {
  default = "Delete published dataset S3 assets"
}

variable "service_name" {}

variable "tier" {}

variable "runtime" {
  default = "python3.7"
}

variable "bucket" {
  default = "pennsieve-cc-lambda-functions-use1"
}

variable "timeout" {
  default = "900"
}

variable "memory_size" {
  default = "192"
}
