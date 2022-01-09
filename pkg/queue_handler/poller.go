// Package queue_handler updates quota on a Store from messages arriving on
// a queue.
package queue_handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/treeverse/terminus/pkg/store"
)

const sleepAfterReceiveFailed = 2 * time.Second

// Poll repeatedly long-polls on client, and updates the store s, until ctx
// is cancelled.
func Poll(ctx context.Context, l *log.Logger, client *sqs.SQS, queueUrl string, keyPattern *regexp.Regexp, keyReplace string, s store.Store) {
	for {
		in := &sqs.ReceiveMessageInput{
			// TODO(ariels): Limiting AttributeNames might increase performance.
			MaxNumberOfMessages: aws.Int64(10),
			MessageAttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll),
			},
			QueueUrl:          &queueUrl,
			VisibilityTimeout: aws.Int64(3),
			WaitTimeSeconds:   aws.Int64(10),
		}
		out, err := client.ReceiveMessageWithContext(ctx, in)
		if ctx.Err() != nil {
			l.Printf("DONE: %s\n", ctx.Err())
			return
		}
		if err != nil {
			// TODO(ariels): Replace with a better logger
			l.Printf("ERROR: %s\n", err)
			time.Sleep(sleepAfterReceiveFailed)
			continue
		}
		for i, m := range out.Messages {
			err = UpdateStore(ctx, m, keyPattern, keyReplace, s)
			if err != nil {
				l.Printf("ERROR (%d/%d): %s\n", i, len(out.Messages), err)
				continue // Don't delete, message may be retries or dead-lettered.
			}

			_, err = client.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueUrl),
				ReceiptHandle: m.ReceiptHandle,
			})
			if err != nil {
				l.Printf("ERROR: Ack/delete message handle %s: %s\n", *m.ReceiptHandle, err)
				continue
			}
		}
	}
}

// UpdateStore updates quota on s from an SQS record.
func UpdateStore(ctx context.Context, message *sqs.Message, keyPattern *regexp.Regexp, keyReplace string, s store.Store) error {
	var records struct {
		Records []S3EventRecord `json:"Records"`
	}

	if err := json.Unmarshal([]byte(*message.Body), &records); err != nil {
		// TODO(ariels): Can we output the bad body here?  It might
		// contain PII in S3 object keys... but OTOH we cannot fix
		// what we cannot see.

		id := "[no ID]"
		if message.MessageId != nil {
			id = *message.MessageId
		}
		return fmt.Errorf("JSON parse failed for message %s: %w\n", id, err)
	}

	var merr *multierror.Error
	for i, rec := range records.Records {
		o, err := ComputePathAndSize(&rec)
		if err != nil && !errors.Is(err, ErrNotAChange) {
			merr = multierror.Append(merr, fmt.Errorf("record parse failed for message %s @%d: %w\n", aws.StringValue(message.MessageId), i, err))
		}
		if err != nil {
			continue
		}

		if match := keyPattern.FindStringSubmatchIndex(o.Path); len(match) == 0 {
			continue
		}

		key := keyPattern.ExpandString(nil, keyReplace, o.Path, match)
		err = s.AddSizeBytes(ctx, string(key), o.SizeBytes)
		if errors.Is(err, store.ErrQuotaExceeded) {
			fmt.Printf("[TODO] Quota exceeded, key %s", key)
		} else if err != nil {
			merr = multierror.Append(merr, fmt.Errorf("add %d bytes to key %s: %w", o.SizeBytes, key, err))
			continue
		}
	}
	return merr.ErrorOrNil()
}
