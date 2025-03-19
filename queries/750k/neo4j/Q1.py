from neo4j import GraphDatabase
import time
from py2neo import Graph

# Database connection details
graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))

# Fetch all the courses offered in Spring
query = '''
    MATCH (s:Schedule3)
    WHERE s.semester STARTS WITH 'Spring'
    RETURN s
'''

start_time = time.time()

results = graph.run(query).data()

for result in results:
    print(result)

# Calculate and print the execution time
execution_time = time.time() - start_time
print("Execution time:", execution_time, "seconds")

# Visualization query
cypher_query = '''
    MATCH (s:Schedule3)
    WHERE s.semester STARTS WITH 'Spring'
    RETURN s
'''

visualization_results = graph.run(cypher_query).data()
