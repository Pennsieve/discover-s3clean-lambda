# Lambda IAM Role
resource "aws_iam_role" "lambda_iam_role" {
  name = "${var.environment_name}-${var.service_name}-${var.tier}-lambda-role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

# Create IAM Policy
resource "aws_iam_policy" "lambda_iam_policy" {
  name   = "${var.environment_name}-${var.service_name}-${var.tier}-policy-${data.terraform_remote_state.vpc.outputs.aws_region_shortname}"
  path   = "/"
  policy = data.aws_iam_policy_document.lambda_iam_policy_document.json
}

# Attach IAM Policy
resource "aws_iam_role_policy_attachment" "lambda_iam_policy_attachment" {
  role       = aws_iam_role.lambda_iam_role.name
  policy_arn = aws_iam_policy.lambda_iam_policy.arn
}

# Lambda IAM Policy Document
data "aws_iam_policy_document" "lambda_iam_policy_document" {
  statement {
    sid    = "CloudwatchLogPermissions"
    effect = "Allow"

    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutDestination",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams",
    ]

    resources = ["*"]
  }

  statement {
    sid       = "KMSDecryptSSMSecrets"
    effect    = "Allow"
    actions   = ["kms:*"]
    resources = ["arn:aws:kms:${var.aws_region}:${data.terraform_remote_state.account.outputs.aws_account_id}:key/alias/aws/ssm"]
  }

  statement {
    sid    = "S3ListGetDelete"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.discover_publish_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.discover_publish_bucket_arn}/*",
      data.terraform_remote_state.platform_infrastructure.outputs.discover_embargo_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.discover_embargo_bucket_arn}/*",
      data.terraform_remote_state.platform_infrastructure.outputs.discover_s3_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.discover_s3_bucket_arn}/*",
      data.terraform_remote_state.platform_infrastructure.outputs.sparc_publish_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.sparc_publish_bucket_arn}/*",
      data.terraform_remote_state.platform_infrastructure.outputs.sparc_embargo_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.sparc_embargo_bucket_arn}/*",

    ]
  }
}
