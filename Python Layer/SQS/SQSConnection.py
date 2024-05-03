import boto3
from dotenv import load_dotenv
import os

load_dotenv()

class SQSConnection:
    def __init__(self) -> None:
        self.sqs_aws_access_key_id = os.getenv("SQS_API_KEY")
        self.sqs_aws_secret_access_key = os.getenv("SQS_API_SECRET")
        self.job_sqs_queue_url = os.getenv("SQS_QUEUE_URL")
        self.inner_sqs_queue_url = os.getenv("SQS_INNER_JOB_QUEUE_URL")
        self.sqs_region = os.getenv("SQS_REGION_NAME")
        self.sqs = boto3.client('sqs', region_name = self.sqs_region,
                    aws_access_key_id = self.sqs_aws_access_key_id,
                    aws_secret_access_key = self.sqs_aws_secret_access_key)
    
    def getConnection(self):
            return self.sqs
    
    def getQueueURL(self, queue_type):
        if queue_type == "FILE_JOB":
            return self.job_sqs_queue_url
        elif queue_type == "INNER_JOB":
            return self.inner_sqs_queue_url