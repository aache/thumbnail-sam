# src/lambda_function.py
import os
import json
import logging
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")

# required env var: DDB_TABLE
DDB_TABLE = os.environ.get("DDB_TABLE")
if not DDB_TABLE:
    logger.error("Environment variable DDB_TABLE not set")

def iso(dt):
    if isinstance(dt, str):
        return dt
    if isinstance(dt, datetime):
        return dt.astimezone(timezone.utc).isoformat()
    return str(dt)

def lambda_handler(event, context):
    logger.info("Event received: %s", json.dumps(event))
    table = ddb.Table(DDB_TABLE)

    records = event.get("Records", [])
    results = []
    for rec in records:
        try:
            if rec.get("eventSource") != "aws:s3":
                logger.info("Skipping non-s3 record")
                continue

            bucket = rec["s3"]["bucket"]["name"]
            key = rec["s3"]["object"]["key"]

            # Get object metadata (head_object)
            try:
                head = s3.head_object(Bucket=bucket, Key=key)
                size = int(head.get("ContentLength", 0))
                etag = head.get("ETag", "").strip('"')
                content_type = head.get("ContentType", "binary/octet-stream")
                last_modified = head.get("LastModified")  # datetime
            except ClientError as e:
                logger.exception("Failed to head_object s3://%s/%s: %s", bucket, key, e)
                # still attempt to write a minimal record
                size = 0
                etag = None
                content_type = None
                last_modified = None

            # Idempotency: use object ETag as part of sort key if available
            timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
            pk = f"S3#{bucket}#{key}"
            sk = f"METADATA#{etag or timestamp}"

            item = {
                "pk": pk,
                "sk": sk,
                "bucket": bucket,
                "original_key": key,
                "etag": etag or "unknown",
                "size_bytes": size,
                "content_type": content_type or "unknown",
                "s3_last_modified": iso(last_modified) if last_modified else None,
                "recorded_at": timestamp
            }

            # Put item into DynamoDB
            table.put_item(Item=item)
            logger.info("Wrote item to DynamoDB table %s: %s", DDB_TABLE, {"pk": pk, "sk": sk})
            results.append({"bucket": bucket, "key": key, "status": "ok"})

        except Exception as e:
            logger.exception("Error processing record: %s", e)
            results.append({"bucket": rec.get("s3", {}).get("bucket", {}).get("name"),
                            "key": rec.get("s3", {}).get("object", {}).get("key"),
                            "status": "error", "error": str(e)})

    return {
        "statusCode": 200,
        "body": json.dumps(results)
    }