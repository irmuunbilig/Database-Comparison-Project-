from neo4j import GraphDatabase
import time
from py2neo import Graph

# Database connection details
graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))

# Query to fetch the distinct names of professors who teach at 'Papardo'
query = '''
    MATCH (p:Professor)-[:TEACHES]->(c:Course)
    WHERE c.campus = 'Papardo'
    RETURN DISTINCT p.name AS professor_name
'''

start_time = time.time()

results = graph.run(query).data()

for result in results:
    print(result)

# Visualization query
cypher_query = '''
    MATCH (p:Professor)-[:TEACHES]->(c:Course)
    WHERE c.campus = 'Papardo'
    RETURN p, c
'''

visualization_results = graph.run(cypher_query).data()

# Print the visualization results
for result in visualization_results:
    print(result)

# Calculate and print the execution time
execution_time = time.time() - start_time
print("Execution time:", execution_time, "seconds")
