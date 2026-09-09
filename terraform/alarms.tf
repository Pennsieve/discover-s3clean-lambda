# T1 CloudWatch alarms (EPIC 868m2zvjt). Keyed by tier — deployed once per
# publish tier; alarm names must stay unique across them. No alarm_actions
# yet.
module "service_alarms" {
  source = "git@github.com:Pennsieve/terraform-modules.git//service-alarms"

  environment_name = var.environment_name
  service_name     = var.service_name

  lambdas = {
    (var.tier) = {
      function_name   = aws_lambda_function.discover_s3clean_lambda_function.function_name
      timeout_seconds = aws_lambda_function.discover_s3clean_lambda_function.timeout
    }
  }
}
