# AWS_DEFAULT_REGION=eu-central-1 terraform plan -var bucket=treeverse-ariels-test  

data "aws_s3_bucket" "bucket" {
  bucket = var.bucket
}

# This should properly be a FIFO queue, to avoid mixing up Create and Delete
# operations.  However AWS says:
#
# error putting S3 Bucket Notification Configuration: InvalidArgument: FIFO
#     SQS queues are not supported.
resource "aws_sqs_queue" "s3_events_queue" {
  name = "terminus-s3-events-queue"

  visibility_timeout_seconds = 3 # Allow other clients to access read but
				 # unacknowledged messages *quickly*

  # ReceiveMessage is a long poll.
  receive_wait_time_seconds = 20
  
  # Could use a dead-letter queue: no reordering issues are expected for S3
  # events, as all objects must have distinct names to be counted correctly.
  # This may be an issue as a poisoned message blocks processing.  Will need
  # to monitor the queue.
  #
  # If we *do* want to work for objects with distinct names, the ordering of
  # messages becomes critical, and dead-letter queues cannot be used at all.

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
