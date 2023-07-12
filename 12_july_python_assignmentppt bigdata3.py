#!/usr/bin/env python
# coding: utf-8

# 1. Write a Python program to read a Hadoop configuration file and display the core components of Hadoop.
# 2. Implement a Python function that calculates the total file size in a Hadoop Distributed File System (HDFS) directory.
# 3. Create a Python program that extracts and displays the top N most frequent words from a large text file using the MapReduce approach.
# 4. Write a Python script that checks the health status of the NameNode and DataNodes in a Hadoop cluster using Hadoop's REST API.
# 5. Develop a Python program that lists all the files and directories in a specific HDFS path.
# 6. Implement a Python program that analyzes the storage utilization of DataNodes in a Hadoop cluster and identifies the nodes with the highest and lowest storage capacities.
# 7. Create a Python script that interacts with YARN's ResourceManager API to submit a Hadoop job, monitor its progress, and retrieve the final output.
# 8. Create a Python script that interacts with YARN's ResourceManager API to submit a Hadoop job, set resource requirements, and track resource usage during job execution.
# 9. Write a Python program that compares the performance of a MapReduce job with different input split sizes, showcasing the impact on overall job execution time.
# 
# 
# 

# In[1]:


#1ANS
import configparser

def display_core_components():
    # Create a ConfigParser object
    config = configparser.ConfigParser()

    # Read the Hadoop configuration file
    config.read('/path/to/hadoop/conf/hadoop-core.xml')

    # Get the core section from the configuration file
    core_section = config['core']

    # Get the property values for the core components
    fs_default_name = core_section.get('fs.default.name')
    hadoop_http_address = core_section.get('hadoop.http.address')
    mapreduce_framework_name = core_section.get('mapreduce.framework.name')

    # Display the core components
    print('Core Components of Hadoop:')
    print('-------------------------')
    print(f'Filesystem Default Name: {fs_default_name}')
    print(f'Hadoop HTTP Address: {hadoop_http_address}')
    print(f'MapReduce Framework Name: {mapreduce_framework_name}')

# Call the function to display the core components
display_core_components()


# In[3]:


pip install hdfs


# In[4]:


#2ANS

from hdfs import InsecureClient

def calculate_total_file_size(hdfs_url, directory):
    # Create an HDFS client
    client = InsecureClient(hdfs_url)

    # Get the file status for the directory
    directory_status = client.status(directory)

    # Initialize the total file size
    total_size = 0

    # Traverse the directory and its subdirectories
    def traverse_directory(path):
        nonlocal total_size

        # Get the file status for the current path
        status = client.status(path)

        # If it's a directory, traverse its contents
        if status['type'] == 'DIRECTORY':
            subdirectories, files = client.list(path)

            for subdir in subdirectories:
                traverse_directory(f"{path}/{subdir}")

            for file in files:
                file_path = f"{path}/{file}"
                file_status = client.status(file_path)
                total_size += file_status['length']

        # If it's a file, add its size to the total
        elif status['type'] == 'FILE':
            total_size += status['length']

    # Start traversing the directory
    traverse_directory(directory)

    # Return the total file size
    return total_size

# Example usage:
hdfs_url = 'http://localhost:50070'  # Replace with your HDFS namenode URL
directory = '/path/to/hdfs/directory'  # Replace with your HDFS directory
total_size = calculate_total_file_size(hdfs_url, directory)
print(f"Total File Size: {total_size} bytes")


# In[7]:


pip install mrjob


# In[3]:


#3ANS
from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq

class TopNWords(MRJob):

    def configure_args(self):
        super(TopNWords, self).configure_args()
        self.add_passthru_arg('--top-n', type=int, help='Number of top words to display')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_top_n)
        ]

    def mapper_get_words(self, _, line):
        words = line.strip().lower().split()
        for word in words:
            yield word, 1

    def combiner_count_words(self, word, counts):
        yield word, sum(counts)

    def reducer_count_words(self, word, counts):
        yield None, (sum(counts), word)

    def reducer_find_top_n(self, _, word_counts):
        top_n = self.options.top_n
        top_n_words = heapq.nlargest(top_n, word_counts)
        for count, word in top_n_words:
            yield word, count

if __name__ == '__main__':
    TopNWords.run()
 


# In[4]:


#4ANS
import requests

# Set the Hadoop cluster URL
hadoop_url = 'http://localhost:9870'

def check_namenode_health():
    # Build the URL for the NameNode health endpoint
    namenode_health_url = f'{hadoop_url}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus'

    # Send a GET request to the NameNode health endpoint
    response = requests.get(namenode_health_url)

    # Parse the JSON response
    data = response.json()

    # Extract the NameNode health status
    namenode_status = data['beans'][0]['State']

    # Print the NameNode health status
    print(f"NameNode Health Status: {namenode_status}")

def check_datanode_health():
    # Build the URL for the DataNodes health endpoint
    datanode_health_url = f'{hadoop_url}/jmx?qry=Hadoop:service=DataNode,name=DataNodeInfo'

    # Send a GET request to the DataNodes health endpoint
    response = requests.get(datanode_health_url)

    # Parse the JSON response
    data = response.json()

    # Extract the DataNodes health status
    datanode_info = data['beans']
    datanode_status = all(d['DatanodeState'] == 'NORMAL' for d in datanode_info)

    # Print the DataNodes health status
    print(f"DataNodes Health Status: {'Healthy' if datanode_status else 'Unhealthy'}")

# Check the health status of the NameNode and DataNodes
check_namenode_health()
check_datanode_health()


# In[5]:


#5ANS
from hdfs import InsecureClient

def list_hdfs_path(hdfs_url, path):
    # Create an HDFS client
    client = InsecureClient(hdfs_url)

    # List files and directories in the given path
    files = client.list(path, status=True)

    # Display the files and directories
    print(f"Files and Directories in {path}:")
    print("-----------------------------")
    for file in files:
        file_type = "Directory" if file['type'] == 'DIRECTORY' else "File"
        print(f"{file_type}: {file['path']}")

# Example usage:
hdfs_url = 'http://localhost:50070'  # Replace with your HDFS namenode URL
path = '/path/to/hdfs/directory'  # Replace with the desired HDFS path
list_hdfs_path(hdfs_url, path)


# In[6]:


#6ANS
import requests

# Set the Hadoop cluster URL
hadoop_url = 'http://localhost:9870'

def analyze_storage_utilization():
    # Build the URL for the DataNodes information endpoint
    datanode_info_url = f'{hadoop_url}/jmx?qry=Hadoop:service=DataNode,name=DataNodeInfo'

    # Send a GET request to the DataNodes information endpoint
    response = requests.get(datanode_info_url)

    # Parse the JSON response
    data = response.json()

    # Extract the DataNodes information
    datanode_info = data['beans']

    # Sort the DataNodes by their storage capacities
    sorted_datanodes = sorted(datanode_info, key=lambda d: d['Capacity'], reverse=True)

    # Print the DataNodes and their storage capacities
    print("DataNodes Storage Utilization:")
    print("-----------------------------")
    for datanode in sorted_datanodes:
        datanode_id = datanode['DatanodeHostName']
        storage_capacity = datanode['Capacity']
        print(f"Datanode: {datanode_id} - Storage Capacity: {storage_capacity}")

    # Find the DataNode with the highest storage capacity
    highest_capacity_datanode = sorted_datanodes[0]
    highest_capacity = highest_capacity_datanode['Capacity']
    highest_capacity_datanode_id = highest_capacity_datanode['DatanodeHostName']

    # Find the DataNode with the lowest storage capacity
    lowest_capacity_datanode = sorted_datanodes[-1]
    lowest_capacity = lowest_capacity_datanode['Capacity']
    lowest_capacity_datanode_id = lowest_capacity_datanode['DatanodeHostName']

    # Print the DataNode with the highest storage capacity
    print(f"\nDatanode with Highest Storage Capacity:")
    print(f"Datanode: {highest_capacity_datanode_id} - Storage Capacity: {highest_capacity}")

    # Print the DataNode with the lowest storage capacity
    print(f"\nDatanode with Lowest Storage Capacity:")
    print(f"Datanode: {lowest_capacity_datanode_id} - Storage Capacity: {lowest_capacity}")

# Analyze the storage utilization of DataNodes
analyze_storage_utilization()


# In[7]:


#7ANS
import requests
import time

# Set the YARN ResourceManager URL
resourcemanager_url = 'http://localhost:8088'

def submit_hadoop_job(jar_path, class_name, input_path, output_path):
    # Build the URL for the job submission endpoint
    submit_url = f'{resourcemanager_url}/ws/v1/cluster/apps/new-application'

    # Send a POST request to the job submission endpoint
    response = requests.post(submit_url)
    data = response.json()
    application_id = data['application-id']

    # Build the URL for the job submission API endpoint
    submit_api_url = f'{resourcemanager_url}/ws/v1/cluster/apps'

    # Define the job submission payload
    payload = {
        "application-id": application_id,
        "application-name": "My Hadoop Job",
        "am-container-spec": {
            "commands": {
                "command": f"yarn jar {jar_path} {class_name} {input_path} {output_path}"
            }
        },
        "priority": 0,
        "queue": "default"
    }

    # Send a PUT request to the job submission API endpoint
    response = requests.put(submit_api_url, json=payload)
    data = response.json()
    job_id = data['app']['id']

    print(f"Hadoop Job submitted. Job ID: {job_id}")

    return job_id

def monitor_job_progress(job_id):
    # Build the URL for the job status API endpoint
    status_url = f'{resourcemanager_url}/ws/v1/cluster/apps/{job_id}'

    while True:
        # Send a GET request to the job status API endpoint
        response = requests.get(status_url)
        data = response.json()
        state = data['app']['state']
        progress = data['app']['progress']

        print(f"Job ID: {job_id} - State: {state} - Progress: {progress}%")

        # Break the loop if the job is completed
        if state == 'FINISHED':
            break

        # Wait for 5 seconds before checking the job status again
        time.sleep(5)

def retrieve_output(output_path):
    # Build the URL for the output retrieval API endpoint
    output_url = f'{resourcemanager_url}/webhdfs/v1{output_path}?op=OPEN'

    # Send a GET request to retrieve the output
    response = requests.get(output_url)
    data = response.content

    print(f"\nJob Output:")
    print(data)

# Example usage:
jar_path = '/path/to/hadoop/job.jar'  # Replace with the path to your Hadoop job JAR file
class_name = 'com.example.hadoop.JobClass'  # Replace with the class name of your Hadoop job
input_path = '/input/path'  # Replace with the input path for your Hadoop job
output_path = '/output/path'  # Replace with the output path for your Hadoop job

# Submit the Hadoop job
job_id = submit_hadoop_job(jar_path, class_name, input_path, output_path)

# Monitor the progress of the Hadoop job
monitor_job_progress(job_id)

# Retrieve the output of the Hadoop job
retrieve_output(output_path)


# In[8]:


#8ANS
import requests
import time

# Set the YARN ResourceManager URL
resourcemanager_url = 'http://localhost:8088'

def submit_hadoop_job(jar_path, class_name, input_path, output_path, num_executors, executor_memory, executor_cores):
    # Build the URL for the job submission endpoint
    submit_url = f'{resourcemanager_url}/ws/v1/cluster/apps/new-application'

    # Send a POST request to the job submission endpoint
    response = requests.post(submit_url)
    data = response.json()
    application_id = data['application-id']

    # Build the URL for the job submission API endpoint
    submit_api_url = f'{resourcemanager_url}/ws/v1/cluster/apps'

    # Define the job submission payload
    payload = {
        "application-id": application_id,
        "application-name": "My Hadoop Job",
        "am-container-spec": {
            "commands": {
                "command": f"yarn jar {jar_path} {class_name} {input_path} {output_path}"
            }
        },
        "priority": 0,
        "queue": "default",
        "resource": {
            "vCores": executor_cores,
            "memory": executor_memory,
            "instances": num_executors
        }
    }

    # Send a PUT request to the job submission API endpoint
    response = requests.put(submit_api_url, json=payload)
    data = response.json()
    job_id = data['app']['id']

    print(f"Hadoop Job submitted. Job ID: {job_id}")

    return job_id

def track_resource_usage(job_id):
    # Build the URL for the job status API endpoint
    status_url = f'{resourcemanager_url}/ws/v1/cluster/apps/{job_id}'

    while True:
        # Send a GET request to the job status API endpoint
        response = requests.get(status_url)
        data = response.json()
        state = data['app']['state']
        progress = data['app']['progress']

        if 'currentResourceUsage' in data['app']:
            current_usage = data['app']['currentResourceUsage']
            allocated_memory = current_usage['allocatedMB']
            allocated_vcores = current_usage['allocatedVCores']

            print(f"Job ID: {job_id} - State: {state} - Progress: {progress}%")
            print(f"Allocated Memory: {allocated_memory} MB - Allocated vCores: {allocated_vcores}")

        # Break the loop if the job is completed
        if state == 'FINISHED':
            break

        # Wait for 5 seconds before checking the job status again
        time.sleep(5)

# Example usage:
jar_path = '/path/to/hadoop/job.jar'  # Replace with the path to your Hadoop job JAR file
class_name = 'com.example.hadoop.JobClass'  # Replace with the class name of your Hadoop job
input_path = '/input/path'  # Replace with the input path for your Hadoop job
output_path = '/output/path'  # Replace with the output path for your Hadoop job
num_executors = 2  # Number of executors for the job
executor_memory = 1024  # Memory per executor in MB
executor_cores = 2  # Number of cores per executor

# Submit the Hadoop job with resource requirements
job_id = submit_hadoop_job(jar_path, class_name, input_path, output_path, num_executors, executor_memory, executor_cores)

# Track the resource usage of the Hadoop job
track_resource_usage(job_id)


# In[9]:


#9ANS
from mrjob.job import MRJob
from mrjob.step import MRStep
import time

class MapReduceJob(MRJob):

    def configure_args(self):
        super(MapReduceJob, self).configure_args()
        self.add_passthru_arg('--split-size', type=int, help='Input split size')

    def mapper(self, _, line):
        # Your mapper logic here
        yield None, line

    def reducer(self, key, values):
        # Your reducer logic here
        for value in values:
            yield key, value

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

    def run_job(self):
        start_time = time.time()

        # Run the MapReduce job
        self.run()

        end_time = time.time()
        execution_time = end_time - start_time

        print(f"Execution Time (Split Size: {self.options.split_size}): {execution_time} seconds")

if __name__ == '__main__':
    job = MapReduceJob()

    # Define the input file and split sizes to compare
    input_file = '/path/to/input/file.txt'
    split_sizes = [100, 1000, 10000]  # Modify with desired split sizes

    # Run the MapReduce job with different split sizes
    for split_size in split_sizes:
        job.args = [input_file, '--split-size', str(split_size)]
        job.run_job()


# In[ ]:




