import logging

from neo4j import GraphDatabase, RoutingControl
from neo4j.exceptions import DriverError, Neo4jError


from dotenv import load_dotenv
import os
import pprint

load_dotenv()


class Neo4jConnection:
    def __init__(self):
        uri = os.getenv("NEO4J_URI")
        user = os.getenv("NEO4J_USER")
        password = os.getenv("NEO4J_PASSWORD")
        self.driver = GraphDatabase.driver(uri, auth=(user, password),database="neo4j")
        
    def close(self):
        self.driver.close()

    def _generate_parameterized_dag_insert_cypher(self, tree_structure):
        # List to hold the generated parameterized Cypher queries
        cypher_queries = []
        # Dictionary to hold the parameters
        parameters = {}

        # Extract nodes and edges from the input JSON structure
        nodes = tree_structure["nodes"]
        edges = tree_structure["edges"]
        root = tree_structure["root"]

        # Create parameterized Cypher queries for nodes with properties
        for node in nodes:
            node_id = node["id"]
            node_label = node["label"]
            node_properties = node.get("properties", {})

            # Construct the properties string with placeholders
            properties_placeholder = ", ".join([f"{key}: ${node_id}_{key}" for key in node_properties])
            cypher_queries.append(f"MERGE (n:{node_label} {{id: ${node_id}_id, {properties_placeholder}}})")

            # Add parameters for this node
            parameters[f"{node_id}_id"] = node_id
            for key, value in node_properties.items():
                parameters[f"{node_id}_{key}"] = value

        # Create parameterized Cypher queries for relationships with properties
        for edge in edges:
            parent = edge["parent"]
            child = edge["child"]
            relationship_properties = edge.get("properties", {})

            if relationship_properties:
                # Construct the properties string with placeholders
                properties_placeholder = ", ".join([f"{key}: ${parent}_{child}_{key}" for key in relationship_properties])
                cypher_queries.append(f"MATCH (p {{id: ${parent}_id}}), (c {{id: ${child}_id}}) MERGE (p)-[:CHILD {{ {properties_placeholder} }}]->(c)")
            else:
                cypher_queries.append(f"MATCH (p {{id: ${parent}_id}}), (c {{id: ${child}_id}}) MERGE (p)-[:CHILD]->(c)")
            # Add parameters for this relationship
            for key, value in relationship_properties.items():
                parameters[f"{parent}_{child}_{key}"] = value

        # Create parameterized Cypher queries for relationships to the root
        for node in nodes:
            node_id = node["id"]
            if node_id != root:
                cypher_queries.append(f"MATCH (r {{id: ${root}_id}}), (n {{id: ${node_id}_id}}) MERGE (n)-[:TO_ROOT]->(r)")

        # Add the root ID parameter
        parameters[f"{root}_id"] = root

        return cypher_queries, parameters

    def _generate_parameterized_leaf_jobs_cypher(self):
        return f'MATCH (n: Job {{root_job_id:$job_id}})-[]->(file:File) WITH COLLECT(file.id) as files, n return n{{.*, files}} as jobs'

    def _generate_parameterized_find_parent_jobs_cypher(self):
        return f'MATCH (n:Job {{id:$job_id}})<-[:CHILD]-(parent:Job) RETURN parent{{.*}} as jobs'

    def _generate_parameterized_update_parent_job_contr_cypher(self):
        return f'MATCH (n:Job {{id:$job_id}})-[c:CHILD]->(:Job)  SET n.sub_job_completed = COALESCE(n.sub_job_completed,0) + 1 WITH n, COUNT(c) as total_sub_jobs RETURN n{{.*, total_sub_jobs: total_sub_jobs}} as jobs'

    def _generate_parameterized_update_child_job_status_cypher(self):
        return f'MATCH (child:Job {{id:$job_id}}) SET child.status = "Completed"'

    def _generate_parameterized_fetch_jobs_cypher(self):
        return f'MATCH (n: Job {{id:$job_id}})-[:CHILD]->(job:Job) WITH COLLECT(job.id) as files,n return n{{.*, files}} as jobs'


    def persist_tree(self, tree_structure):
        queries, parameters = self._generate_parameterized_dag_insert_cypher(tree_structure)
        for query in queries:
            record = self.driver.execute_query(query, parameters)
            pprint.pprint(record)        

    def getLeafJobs(self,job_id):
        query = self._generate_parameterized_leaf_jobs_cypher()
        return self.driver.execute_query(
            query,
            {"job_id": job_id},
            routing_=RoutingControl.READ,
            result_transformer_=lambda r: r.value("jobs"),
        )

    def getJob(self,job_id):
        query = self._generate_parameterized_fetch_jobs_cypher()
        return self.driver.execute_query(
            query,
            {"job_id": job_id},
            routing_=RoutingControl.READ,
            result_transformer_=lambda r: r.value("jobs"),
        )

    def getParentInfo(self, job_id):
        query = self._generate_parameterized_find_parent_jobs_cypher()
        return self.driver.execute_query(
            query,
            {"job_id": job_id},
            routing_=RoutingControl.READ,
            result_transformer_=lambda r: r.value("jobs"),
        )

    def updateChildCompletion(self, job_id, parent_job_id):
        query = self._generate_parameterized_update_child_job_status_cypher()
        self.driver.execute_query(
                    query,
                    {"job_id": job_id},
                    routing_=RoutingControl.WRITE,
                    result_transformer_=lambda r: r.value("jobs"),
                )
        if parent_job_id is not job_id:
            query = self._generate_parameterized_update_parent_job_contr_cypher()
            print(f"Updating parent {parent_job_id}")
            return self.driver.execute_query(
                query,
                {"job_id": parent_job_id},
                routing_=RoutingControl.WRITE,
                result_transformer_=lambda r: r.value("jobs"),
                )
        return None

# neo4jConnection = Neo4jConnection()

# try:
# #     neo4jConnection.persist_tree(tempTree)
#     pprint.pprint(neo4jConnection.getLeafJobs("9eafe362aa5042a291f206f7cdb0e18e"))
# finally:
#     neo4jConnection.close()