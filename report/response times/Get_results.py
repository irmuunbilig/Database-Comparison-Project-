import os
import subprocess
import csv
import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor, as_completed

# Base paths
query_base_path = '/Users/irmuunbilig/Desktop/DB_pro/queries'
output_base_path = '/Users/irmuunbilig/Desktop/DB_pro/output'

# Dataset sizes and database names
dataset_sizes = ['250k', '500k', '750k']
databases = ['cassandra', 'mongoDb', 'mysql', 'neo4j', 'redis']
query_files = ['Q1.py', 'Q2.py', 'Q3.py']

# Function to execute a query file and measure its execution time
def execute_query(query_file, iteration):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            print(f"Starting execution of {query_file} (attempt {attempt + 1}/{max_retries}), iteration {iteration + 1}/31")
            start_time = time.time()
            result = subprocess.run(['python', query_file], capture_output=True, text=True, timeout=300)
            end_time = time.time()
            if result.returncode != 0:
                print(f"Error running query {query_file}: {result.stderr}")
                continue
            execution_time = end_time - start_time
            print(f"Execution of {query_file} (iteration {iteration + 1}/31) completed in {execution_time:.2f} seconds")
            return execution_time
        except subprocess.TimeoutExpired:
            print(f"Query {query_file} timed out. Retrying {attempt + 1}/{max_retries}...")
        except Exception as e:
            print(f"Exception running query {query_file}: {e}. Retrying {attempt + 1}/{max_retries}...")
    print(f"Failed to execute {query_file} after {max_retries} attempts.")
    return None

# Function to perform queries and collect execution times
def perform_queries(dataset_size, db_name):
    results = {query: [] for query in query_files}
    query_path = os.path.join(query_base_path, dataset_size, db_name)

    def run_queries_for_file(query_file):
        query_file_path = os.path.join(query_path, query_file)
        if not os.path.exists(query_file_path):
            print(f"Query file {query_file_path} does not exist.")
            return None, []

        print(f"Executing {query_file_path} for {db_name} on {dataset_size} dataset")
        
        query_results = []
        for i in range(31):
            execution_time = execute_query(query_file_path, i)
            if execution_time is not None:
                query_results.append(execution_time)
            else:
                print(f"Failed to execute {query_file_path} at iteration {i + 1}")
        return query_file, query_results

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(run_queries_for_file, query_file) for query_file in query_files]
        for future in as_completed(futures):
            query_file, query_results = future.result()
            if query_file is not None:
                results[query_file] = query_results

    return results

# Function to save results to CSV
def save_results_to_csv(results, dataset_size):
    output_file = os.path.join(output_base_path, f"{dataset_size}.csv")
    headers = [''] + [f'{db}_{q}' for db in databases for q in ['Q1', 'Q2', 'Q3']]
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        max_len = max(len(v) for res in results.values() for v in res.values())
        for i in range(max_len):
            row = [f'R{i + 1}']
            for db in databases:
                for query in query_files:
                    row.append(results[db][query][i] if i < len(results[db][query]) else '')
            writer.writerow(row)
        
        writer.writerow(['MIN'] + [min(results[db][query]) for db in databases for query in query_files if results[db][query]])
        writer.writerow(['MAX'] + [max(results[db][query]) for db in databases for query in query_files if results[db][query]])
        writer.writerow(['MEAN'] + [np.mean(results[db][query]) for db in databases for query in query_files if results[db][query]])


# Ensure output directory exists
os.makedirs(output_base_path, exist_ok=True)

# Main loop to process all datasets and databases
for dataset_size in dataset_sizes:
    print(f"Processing dataset size: {dataset_size}")
    all_results = {db: perform_queries(dataset_size, db) for db in databases}
    save_results_to_csv(all_results, dataset_size)
    
print("All tasks completed successfully.")
