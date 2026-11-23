output "vpc_id" {
  value = aws_vpc.main.id
}

output "subnet_id" {
  value = aws_subnet.main.id
}

output "bucket" {
  value = aws_s3_bucket.artifacts.bucket
}

output "queue_url" {
  value = aws_sqs_queue.jobs.url
}

output "topic_arn" {
  value = aws_sns_topic.alerts.arn
}

output "table_name" {
  value = aws_dynamodb_table.items.name
}

output "function_arn" {
  value = aws_lambda_function.handler.arn
}
