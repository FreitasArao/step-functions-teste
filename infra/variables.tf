variable "aws_region" {
  description = "AWS region for MiniStack"
  type        = string
  default     = "us-east-1"
}

variable "ministack_endpoint" {
  description = "MiniStack local endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}
