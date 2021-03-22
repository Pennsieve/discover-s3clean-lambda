// Create Lambda function
resource "aws_lambda_function" "discover_s3clean_lambda_function" {
  description       = var.description
  function_name     = "${var.environment_name}-${var.service_name}-${var.tier}-lambda-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
  handler           = "main.lambda_handler"
  runtime           = var.runtime
  role              = aws_iam_role.lambda_iam_role.arn
  s3_bucket         = data.aws_s3_bucket_object.s3_bucket_object.bucket
  s3_key            = data.aws_s3_bucket_object.s3_bucket_object.key
  s3_object_version = data.aws_s3_bucket_object.s3_bucket_object.version_id
  timeout           = var.timeout
  memory_size       = var.memory_size

  environment {
    variables = {
      VERSION                   = var.version_number
      ENVIRONMENT               = var.environment_name
      SERVICE_NAME              = var.service_name
      TIER                      = var.tier
      PUBLISH_BUCKET            = data.terraform_remote_state.platform_infrastructure.outputs.discover_publish_bucket_id
      EMBARGO_BUCKET            = data.terraform_remote_state.platform_infrastructure.outputs.discover_embargo_bucket_id
      ASSET_BUCKET              = data.terraform_remote_state.platform_infrastructure.outputs.discover_s3_bucket_id
      DATASET_ASSETS_KEY_PREFIX = data.terraform_remote_state.platform_infrastructure.outputs.discover_bucket_dataset_assets_key_prefix
    }
  }
}
