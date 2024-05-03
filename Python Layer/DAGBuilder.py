import math
from collections import deque

class DAGBuilder:
    def __init__(self, jobId):
        self.jobId = jobId
    

    def create_tree_structure(self, num_leaves, max_children=5):
        edges = []

        nodes = [
            {
                "id": f"{self.jobId}_file_{i}",
                "label": "File",
                "properties": {"status": "File created",
                               "root_job_id": self.jobId
                },
            }
            for i in range(1, num_leaves + 1)
        ]
        # A list of current nodes at the lowest level
        current_level = [f"{self.jobId}_file_{i}" for i in range(1, num_leaves + 1)]

        # Work upwards to create parent levels until only one root remains
        parent_level = 1
        while len(current_level) > 1:
            # Determine the number of parents needed at this level
            num_parents = math.ceil(len(current_level) / max_children)

            # Create the parent nodes for this level
            parent_nodes = []

            for i in range(1, num_parents + 1):
                parent_id = f"{self.jobId}_temp_{parent_level}_{i}"
                if num_parents == 1:
                    parent_id = f"{self.jobId}"
                nodes.append({"id": parent_id, "label": "Job",  "properties": {"status": "DAG Created", "root_job_id": self.jobId, "num_leaves":num_leaves}})
                parent_nodes.append(parent_id)

            # Assign children to these parent nodes
            for i, parent_id in enumerate(parent_nodes):
                start_idx = i * max_children
                end_idx = min((i + 1) * max_children, len(current_level))
                children = current_level[start_idx:end_idx]

                edges.extend({"parent": parent_id, "child": child_id} for child_id in children)
            # Move up a level and prepare the new current_level
            current_level = parent_nodes
            parent_level += 1

        return {"nodes": nodes, "edges": edges, "root": f"{self.jobId}"}