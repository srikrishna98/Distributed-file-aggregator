import redis
from SQS.SQSConnection import SQSConnection
from rq import Connection


class SQSConsumer:
    def __init__(self) -> None:        
        self.redis_conn = redis.Redis()        
        sqs_connection = SQSConnection()
        self.sqs = sqs_connection.getConnection()
        self.file_queue_url = sqs_connection.getQueueURL("FILE_JOB")
        self.temp_file_queue_url = sqs_connection.getQueueURL("INNER_JOB")
    
    def getMessage(self, worker_type):
        queue_url = self.file_queue_url
        if worker_type == "TEMP_FILE_WORKER":
            queue_url = self.temp_file_queue_url

        response = self.sqs.receive_message(
                    QueueUrl=queue_url,
                    AttributeNames=["MessageGroupId"],
                    MaxNumberOfMessages=1,  
                    WaitTimeSeconds=3
                )
        return messages[0] if (messages := response.get('Messages', [])) else None

    def deleteMessage(self, worker_type, receiptHandle):
        queue_url = self.file_queue_url
        if worker_type == "TEMP_FILE_WORKER":
            queue_url = self.temp_file_queue_url
        self.sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receiptHandle
                    )

    

