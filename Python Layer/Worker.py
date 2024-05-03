import threading
import time
import redis
import json
from SQS.SQSConsumer import SQSConsumer
from Database import Neo4jConnection
from JobManager import JobManager
from Compute import Compute
from botocore.exceptions import ClientError
from rq import Connection
import logging

class Worker:

    def __init__(self) -> None:
        self.redis_conn = redis.Redis()        
        self.consumer = SQSConsumer()
        self.job_manager = JobManager()

    def _compute_mean(self,values):
        files = values["files"]
        job_id = values["id"]
        root_job_id = values["root_job_id"]
        num_files = values["num_leaves"]
        is_root = job_id == root_job_id
        graphdb_driver = Neo4jConnection.Neo4jConnection()

        if not is_root:
            parent_info = graphdb_driver.getParentInfo(job_id)[0]
            parent_id = parent_info["id"]
        else:
            parent_id = root_job_id
        compute = Compute(files,job_id,is_root,num_files, root_job_id)
        try:
            compute.computeResults()
        except FileNotFoundError:
            # TODO RETRY mechanism, update in db ?
            time.sleep(3)
            compute.computeResults()
        parent_info = graphdb_driver.updateChildCompletion(job_id, parent_id)[0]

        graphdb_driver.close()
        return parent_info
        
    def _update_worker_status(self, worker_name, status):
        self.redis_conn.hset('worker_status', worker_name, status)
    
    def _worker_polling(self, worker_name, worker_type, stop_event):
        print(f"worker {worker_name} of type {worker_type} created\n")
        
        while not stop_event.is_set():
            try:                 
                if message := self.consumer.getMessage(worker_type):  
                    print(f"worker {worker_name} is processing a message.")
                    job_data = json.loads(message['Body'])
                    logging.info(job_data)
                    
                    with Connection(self.redis_conn):
                        self._update_worker_status(worker_name, 'busy')

                    updated_parent = self._compute_mean(job_data) 
                    if updated_parent is not None and len(updated_parent)>0:
                        total_sub_jobs = updated_parent["total_sub_jobs"]
                        sub_job_completed = updated_parent["sub_job_completed"]
                        if total_sub_jobs == sub_job_completed:
                            self.job_manager.queueJob(updated_parent["id"])

                    with Connection(self.redis_conn):
                        self._update_worker_status(worker_name, 'idle')

                    self.consumer.deleteMessage(worker_type, message['ReceiptHandle'])

                else:
                    print(f"{worker_type} Worker {worker_name} is idle...\n")
                    time.sleep(3)  # Optional delay to prevent high CPU usage

            except ClientError as e:
                print(f"Error during polling: {e}")
        print(f"Terminating worker {worker_name} of type {worker_type}")

    # Run multiple workers with unique names
    def run_workers(self, num_workers, stop_event):
        worker_names = [f"Worker{i}" for i in range(1,num_workers+1)] 
        worker_threads = []
        
        for worker_name in worker_names:
            self._update_worker_status(worker_name, "idle")
        
        for worker_name in worker_names[:5]:
            new_thread = threading.Thread(target=self._worker_polling,args=(worker_name, "FILE_WORKER", stop_event))
            worker_threads.append(new_thread)
            
        for worker_name in worker_names[5:]:
            new_thread = threading.Thread(target=self._worker_polling,args=(worker_name, "TEMP_FILE_WORKER", stop_event))
            worker_threads.append(new_thread)
            
        for worker_thread in worker_threads:
            worker_thread.start()
            
        for worker_thread in worker_threads:
            worker_thread.join()
            