# Queue structure

Terminus receives S3 event notifications onto an SQS topic.  However S3
*cannot* deliver notifications in-order -- trying to configure a bucket
to send notifications to a FIFO SQS queue results in

> Error: error putting S3 Bucket Notification Configuration:
> InvalidArgument: FIFO SQS queues are not supported.

This makes sense at object storage scale _across objects_.  Per-object,
not so much: there is no guarantee that Terminus will observe an object
creation event before a deletion event.  Nonetheless, we work with what
we have.

So Terminus _only adds_ object creation events.  It furthermore ignores
object modifications: these do not occur in lakeFS object storage.
