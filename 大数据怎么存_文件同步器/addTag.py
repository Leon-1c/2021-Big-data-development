import os
import boto3
import time


def lambda_handler(event, context):
    evtdata = event["Records"][0]["s3"]
    bucket = evtdata["bucket"]["name"]
    endpoint_url = ""

    s3Client = boto3.client("s3",
                            aws_access_key_id="",
                            aws_secret_access_key="",
                            endpoint_url=endpoint_url
                            )

    for e in event["Records"]:
        key = e["s3"]["object"]["key"]
        s3Client.copy_object(Bucket=bucket, Key=key, CopySource=bucket + '/' + key,
                         Metadata={'Lastmodifytime': str(time.time())}, MetadataDirective='REPLACE')
    

    return "{}/{}/{}".format(endpoint_url, bucket, key)
