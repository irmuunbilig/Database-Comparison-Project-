from neo4j import GraphDatabase
from py2neo import Graph
import time

# Database connection details
graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))

# Query to fetch the professors' names who teach at 'Papardo' in Spring
query = '''
    MATCH (p:Professor3)-[:TEACHES]->(c:Course3)-[:HAS_SCHEDULE]->(s:Schedule3)
    WHERE c.campus = 'Papardo' AND s.semester STARTS WITH 'Spring'
    RETURN DISTINCT p.name AS professor_name
'''

start_time = time.time()

results = graph.run(query).data()

for result in results:
    print(result)

# Visualization query
cypher_query = '''
    MATCH (p:Professor3)-[:TEACHES]->(c:Course3)-[:HAS_SCHEDULE]->(s:Schedule3)
    WHERE c.campus = 'Papardo' AND s.semester STARTS WITH 'Spring'
    RETURN p, c, s
'''

visualization_results = graph.run(cypher_query).data()

# Print the visualization results
for result in visualization_results:
    print(result)

# Calculate and print the execution time
execution_time = time.time() - start_time
print("Execution time:", execution_time, "seconds")
