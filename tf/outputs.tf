output "queue_url" {
  value = aws_sqs_queue.s3_events_queue.url
}

output "queue_arn" {
  value = aws_sqs_queue.s3_events_queue.arn
}
