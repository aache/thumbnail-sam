import os
import io
import json
import logging
from datetime import datetime

import boto3
from PIL import Image, ExifTags
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# env vars (from template Globals / Environment)
DDB_TABLE = os.environ.get("DDB_TABLE")
THUMBNAIL_PREFIX = os.environ.get("THUMBNAIL_PREFIX", "thumbnails/")
THUMBNAIL_SIZE = int(os.environ.get("THUMBNAIL_SIZE", "200"))
THUMBNAIL_FORMAT = os.environ.get("THUMBNAIL_FORMAT", "JPEG")
BUCKET_NAME = os.environ.get("BUCKET_NAME")  # from SAM template

def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event))
    table = dynamodb.Table(DDB_TABLE)

    for record in event.get("Records", []):
        try:
            if record.get("eventSource") != "aws:s3":
                logger.info("Skipping non-s3 record")
                continue

            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]
            size = int(record["s3"]["object"].get("size", 0))

            # Skip if it's our thumbnail (avoid infinite loop)
            if key.startswith(THUMBNAIL_PREFIX):
                logger.info("Skipping thumbnail: %s", key)
                continue

            logger.info("Processing s3://%s/%s", bucket, key)

            # Download the object
            resp = s3.get_object(Bucket=bucket, Key=key)
            body = resp["Body"].read()

            # Open image
            img = Image.open(io.BytesIO(body))

            # Handle EXIF orientation if present
            try:
                for orientation in ExifTags.TAGS.keys():
                    if ExifTags.TAGS[orientation] == "Orientation":
                        break
                exif = img._getexif()
                if exif is not None:
                    orientation_value = exif.get(orientation)
                    if orientation_value == 3:
                        img = img.rotate(180, expand=True)
                    elif orientation_value == 6:
                        img = img.rotate(270, expand=True)
                    elif orientation_value == 8:
                        img = img.rotate(90, expand=True)
            except Exception:
                # non-fatal
                pass

            # Create thumbnail (in-place)
            img.thumbnail((THUMBNAIL_SIZE, THUMBNAIL_SIZE))

            # Save thumbnail to bytes
            thumb_buf = io.BytesIO()
            fmt = THUMBNAIL_FORMAT.upper()
            if fmt == "JPEG":
                if img.mode in ("RGBA", "LA"):
                    background = Image.new("RGB", img.size, (255, 255, 255))
                    background.paste(img, mask=img.split()[3])
                    background.save(thumb_buf, format="JPEG", quality=85)
                else:
                    img.save(thumb_buf, format="JPEG", quality=85)
                content_type = "image/jpeg"
            else:
                img.save(thumb_buf, format=fmt)
                content_type = f"image/{fmt.lower()}"

            thumb_buf.seek(0)

            # Choose thumbnail key (put under THUMBNAIL_PREFIX preserving original path)
            thumb_key = f"{THUMBNAIL_PREFIX}{key}"

            # Upload thumbnail
            s3.put_object(
                Bucket=bucket,
                Key=thumb_key,
                Body=thumb_buf,
                ContentType=content_type
            )
            logger.info("Uploaded thumbnail to s3://%s/%s", bucket, thumb_key)

            # Write metadata to DynamoDB
            now = datetime.utcnow().isoformat() + "Z"
            item = {
                "pk": f"S3#{bucket}#{key}",
                "sk": f"METADATA#{now}",
                "bucket": bucket,
                "original_key": key,
                "thumbnail_key": thumb_key,
                "original_size_bytes": size,
                "thumbnail_size_px": THUMBNAIL_SIZE,
                "content_type": content_type,
                "created_at": now
            }
            table.put_item(Item=item)
            logger.info("Wrote metadata to DynamoDB table %s: %s", DDB_TABLE, item)

        except ClientError as e:
            logger.exception("AWS ClientError: %s", e)
        except Exception as e:
            logger.exception("Unhandled exception: %s", e)

    return {"status": "ok"}