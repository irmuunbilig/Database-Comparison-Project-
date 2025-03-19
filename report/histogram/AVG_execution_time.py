import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

# Load the CSV files
df_250k = pd.read_csv('/Users/irmuunbilig/Desktop/DB_pro/output/250k.csv')
df_500k = pd.read_csv('/Users/irmuunbilig/Desktop/DB_pro/output/500k.csv')
df_750k = pd.read_csv('/Users/irmuunbilig/Desktop/DB_pro/output/750k.csv')

# Extract the MEAN row from each dataframe
mean_250k = df_250k[df_250k['Unnamed: 0'] == 'MEAN']
mean_500k = df_500k[df_500k['Unnamed: 0'] == 'MEAN']
mean_750k = df_750k[df_750k['Unnamed: 0'] == 'MEAN']

# Combine the MEAN rows into a single dataframe for easy plotting
mean_combined = pd.concat([mean_250k, mean_500k, mean_750k], ignore_index=True)
mean_combined['Size'] = ['250k', '500k', '750k']

# Set the index to Size for easier plotting
mean_combined.set_index('Size', inplace=True)

# Create a PDF file to save the plots
pdf_filename = 'average_execution_times.pdf'
pdf_pages = PdfPages(pdf_filename)

# Define a function to plot and save each figure
def plot_and_save(query_set, title):
    plt.figure(figsize=(10, 5))
    plt.bar(['MySQL', 'MongoDB', 'Cassandra', 'Redis', 'Neo4j'], query_set, color='blue')
    plt.title(title)
    plt.xlabel('MEAN')
    plt.ylabel('TIME(SECOND)')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    pdf_pages.savefig()
    plt.close()

# Define the queries for each Q set
queries_q1 = mean_combined.loc['250k', ['mysql_Q1', 'mongoDb_Q1', 'cassandra_Q1', 'redis_Q1', 'neo4j_Q1']]
queries_q2 = mean_combined.loc['500k', ['mysql_Q2', 'mongoDb_Q2', 'cassandra_Q2', 'redis_Q2', 'neo4j_Q2']]
queries_q3 = mean_combined.loc['750k', ['mysql_Q3', 'mongoDb_Q3', 'cassandra_Q3', 'redis_Q3', 'neo4j_Q3']]

# Plot and save each figure
plot_and_save(queries_q1, 'Average Execution Time of Each Database for Q1')
plot_and_save(queries_q2, 'Average Execution Time of Each Database for Q2')
plot_and_save(queries_q3, 'Average Execution Time of Each Database for Q3')

# Close the PDF file
pdf_pages.close()