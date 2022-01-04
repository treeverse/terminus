# AWS_DEFAULT_REGION=... terraform plan -var bucket=...

data "aws_s3_bucket" "bucket" {
  bucket = var.bucket
}

resource "aws_sqs_queue" "s3_events_queue" {
  name = "terminus-s3-events-queue"

  visibility_timeout_seconds = 3 # Allow other clients to access read but
				 # unacknowledged messages *quickly*

  # ReceiveMessage is a long poll.
  receive_wait_time_seconds = 20
  
  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:*:*:terminus-s3-events-queue",
      "Condition": {
        "ArnEquals": { "aws:SourceArn": "${data.aws_s3_bucket.bucket.arn}" }
      }
    }
  ]
}
POLICY

  tags = {
    service = "terminus"
  }
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = data.aws_s3_bucket.bucket.id

  queue {
    queue_arn     = aws_sqs_queue.s3_events_queue.arn
    events        = ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"]
  }
}
