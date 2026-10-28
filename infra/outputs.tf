output "order_events_queue_url" {
  description = "URL of the SQS Queue that receives published Outbox domain events (e.g., ORDER_CREATED)"
  value       = aws_sqs_queue.order_events_queue.url
}

output "sqs_queue_url" {
  description = "URL of the SQS Queue used by Step Functions waitForTaskToken"
  value       = aws_sqs_queue.order_saga_queue.url
}

output "sqs_dlq_url" {
  description = "URL of the Dead Letter Queue"
  value       = aws_sqs_queue.order_saga_dlq.url
}

output "state_machine_arn" {
  description = "ARN of the Order Saga Step Functions State Machine"
  value       = aws_sfn_state_machine.order_saga.arn
}
