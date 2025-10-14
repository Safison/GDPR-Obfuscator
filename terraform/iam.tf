# AWS IAM role set up for lambda

# Creates Trust Policies for Lambda 
data "aws_iam_policy_document" "trust_policy_lambda" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}


# Creates IAM Role
resource "aws_iam_role" "obfuscate_lambda_role" {
  name_prefix        = "role-${var.obfuscate_lambda}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy_lambda.json
  tags      = {
    Name        = "gdpr-obfuscate-${var.environment}"
    Environment = var.environment
  }
}


# Creates IAM policy document for s3 buckets access permissions
data "aws_iam_policy_document" "obfuscate_s3_policy" {
  statement {
    sid = "1"

    actions = ["s3:PutObject",
      "s3:Get*",
      "s3:List*",
      "s3:Describe*",
      "s3-object-lambda:Get*",
    "s3-object-lambda:List*"]
    resources = ["*"
    ]
  }
}


# Creates IAM policy
resource "aws_iam_policy" "s3_policy" {
  name_prefix = "s3-policy-${var.obfuscate_lambda}-write"

  policy = data.aws_iam_policy_document.obfuscate_s3_policy.json

}

# Creates policy attachment to the role "obfuscate_lambda_role"
resource "aws_iam_policy_attachment" "lambda_s3_policy_attachment" {
  name       = "lambda-s3-policy-attachment"
  roles      = [aws_iam_role.obfuscate_lambda_role.name]
  policy_arn = aws_iam_policy.s3_policy.arn
}