terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region                      = var.aws_region
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    sfn = var.ministack_endpoint
    sqs = var.ministack_endpoint
    iam = var.ministack_endpoint
    sts = var.ministack_endpoint
  }
}

resource "aws_sqs_queue" "order_events_dlq" {
  name                      = "order-events-dlq"
  message_retention_seconds = 86400
}

resource "aws_sqs_queue" "order_events_queue" {
  name                       = "order-events-queue"
  visibility_timeout_seconds = 30
  receive_wait_time_seconds  = 5

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.order_events_dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_sqs_queue" "order_saga_dlq" {
  name                      = "order-saga-tasks-dlq"
  message_retention_seconds = 86400
}

resource "aws_sqs_queue" "order_saga_queue" {
  name                       = "order-saga-tasks-queue"
  visibility_timeout_seconds = 30
  receive_wait_time_seconds  = 5

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.order_saga_dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_iam_role" "order_saga_sfn_role" {
  name = "order-saga-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_sfn_state_machine" "order_saga" {
  name     = "order-saga-state-machine"
  role_arn = aws_iam_role.order_saga_sfn_role.arn

  definition = templatefile("${path.module}/statemachine/order-saga.asl.json", {
    queue_url = aws_sqs_queue.order_saga_queue.url
  })
}
