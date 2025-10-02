#Terraform variable for tagging AWS resources with an environment
variable "environment" {
  description = "The enviroment tag for AWS resources"
  type        = string
  default     = "dev"
}


variable "obfuscate_lambda" {
  type    = string
  default = "obfuscate_lambda"
}


variable "default_timeout" {
  type    = number
  default = 240
}


variable "python_runtime" {
  type    = string
  default = "python3.11"
}