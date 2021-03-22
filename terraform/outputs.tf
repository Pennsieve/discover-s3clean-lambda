# OUTPUT LAMBDA ARN
output "lambda_function_arn" {
  value = aws_lambda_function.discover_s3clean_lambda_function.arn
}
