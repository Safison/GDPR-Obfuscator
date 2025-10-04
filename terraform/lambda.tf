# Create ZIP with source + dependencies
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/../packages/lambda_package.zip"
}


# Lambda Function
resource "aws_lambda_function" "obfuscation_lambda" {
  function_name = "obfuscation_lambda"
  handler       = "obfuscation_lambda.lambda_handler"
  runtime       = var.python_runtime
  role          = aws_iam_role.obfuscate_lambda_role.arn
  filename      = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  memory_size   = 256
  timeout       = var.default_timeout
  # panda layer from external source 
  layers = [
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python311:23"
  ]
  tags      = {
    Name        = "gdpr-obfuscate-${var.environment}"
    Environment = var.environment
  }
}


#s3 bucket for lambda code
resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "gdpr-obfuscate-code-"
  tags      = {
    Name        = "gdpr-obfuscate-${var.environment}"
    Environment = var.environment
  }
}


# s3 object for lambda code
resource "aws_s3_object" "lambda_code" {
  for_each = toset([var.obfuscate_lambda])
  bucket   = aws_s3_bucket.code_bucket.bucket
  key      = "${each.key}/function.zip"
  source   = "${path.module}/../packages/lambda_package.zip"
  #etag     = filemd5("${path.module}/../packages/lambda_package.zip")
  etag     = filemd5("${path.module}/../packages/${each.key}/lambda_package.zip")
}