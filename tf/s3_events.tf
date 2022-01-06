# AWS_DEFAULT_REGION=... terraform plan -var buckets='["bucket1", "bucket2", ...]'

data "aws_s3_bucket" "buckets" {
  for_each = toset(var.buckets)
  bucket = each.key
}

data "aws_iam_policy_document" "s3_events_queue_policy" {
  statement {
    sid = "AllowEventsFromS3Buckets"

    principals {
      type = "*"
      identifiers = ["*"]
    }

    actions = ["sqs:SendMessage"]

    resources = ["arn:aws:sqs:*:*:terminus-s3-events-queue"]

    condition {
      test = "ArnLike"
      variable = "aws:SourceArn"
     values = [for bucket in data.aws_s3_bucket.buckets : bucket.arn]
    }
  }
}

resource "aws_sqs_queue" "s3_events_queue" {
  name = "terminus-s3-events-queue"

  visibility_timeout_seconds = 3 # Allow other clients to access read but
				 # unacknowledged messages *quickly*

  # ReceiveMessage is a long poll.
  receive_wait_time_seconds = 20

  # Allow events from all configured buckets.
  policy = data.aws_iam_policy_document.s3_events_queue_policy.json

  tags = {
    service = "terminus"
  }
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  for_each = data.aws_s3_bucket.buckets
  bucket = "${each.value.id}"

  queue {
    queue_arn     = aws_sqs_queue.s3_events_queue.arn
    events        = ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"]
  }
}
