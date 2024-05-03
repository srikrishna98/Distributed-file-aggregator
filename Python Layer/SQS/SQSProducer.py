from SQS.SQSConnection import SQSConnection
import json
from botocore.exceptions import ClientError

class SQSProducer:
    def __init__(self) -> None:
        sqs_connection = SQSConnection()
        self.sqs = sqs_connection.getConnection()
        self.file_queue_url = sqs_connection.getQueueURL("FILE_JOB")
        self.temp_file_queue_url = sqs_connection.getQueueURL("INNER_JOB")

    
    def enqueue_file_job(self, data):
        try:
            self.sqs.send_message(
                QueueUrl = self.file_queue_url,
                MessageBody=json.dumps(data),
                MessageGroupId="LeafJobs"
                
            )
            return True
        except ClientError as e:
            print(f"Error enqueuing job: {e}")
            return False

    def enqueue_temp_job(self, data):
        try:
            self.sqs.send_message(
                QueueUrl = self.temp_file_queue_url,
                MessageBody=json.dumps(data),
                MessageGroupId="InnerJobs"
                
            )
            return True
        except ClientError as e:
            print(f"Error enqueuing job: {e}")
            return False
