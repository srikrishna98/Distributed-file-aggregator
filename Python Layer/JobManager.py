from Database import Neo4jConnection
from SQS.SQSProducer import SQSProducer
from DAGBuilder import DAGBuilder

class JobManager:
    def queueFiles(self, job_id):
        neo4j_connection = Neo4jConnection.Neo4jConnection()
        sqs_producer = SQSProducer()
        leaf_jobs = neo4j_connection.getLeafJobs(job_id)
        neo4j_connection.close()
        for job in leaf_jobs:
            sqs_producer.enqueue_file_job(job)
    
    def queueJob(self, job_id):
        neo4j_connection = Neo4jConnection.Neo4jConnection()
        sqs_producer = SQSProducer()
        leaf_jobs = neo4j_connection.getJob(job_id)
        neo4j_connection.close()
        for job in leaf_jobs:
            sqs_producer.enqueue_temp_job(job)
    
    def generateJobQueue(self, jobId: str):
        job_manager = JobManager()
        job_manager.queueFiles(jobId)


    async def createJobs(self, fileGenerator, job_id):
        dag_builder = DAGBuilder(job_id)
        tree_structure = dag_builder.create_tree_structure(fileGenerator.fileCount)
        neo4jConnection = Neo4jConnection.Neo4jConnection()
        try:
            neo4jConnection.persist_tree(tree_structure)
        finally:
            neo4jConnection.close()
            self.generateJobQueue(job_id)