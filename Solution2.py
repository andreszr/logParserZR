from pyspark import SparkContext
from datetime import datetime, timedelta
import os
import shutil
import time  

# Get the current time
current_time = datetime(2012, 4, 24, 11, 3, 59, 342380).timestamp()
# Get the time one hour ago
time_one_hour_ago = current_time - 3600

# Create a function to map the CSV data to a tuple of (epoch time, host1, host2)
def map_csv_data(line):
    parts = line.split(" ") # to split each row in the file
    epoch_time = int(parts[0]) # retrieve first field
    host1 = parts[1] # retrieve second field
    host2 = parts[2] # retrieve third field
    return (epoch_time, host1, host2)

# Create a function to get the list of hostnames connected to or from the given parameter host during the last hour
def get_hosts_connections(rdd, host, type='to'):
    # Filter the RDD to include only connections made in the last hour
    rdd = rdd.filter(lambda x: x[0] >= time_one_hour_ago).cache()
    # Conditional to get data about connection to or from the host
    if type == 'to':
        # Filter the RDD to include only connections involving the given host
        rdd = rdd.filter(lambda x: x[2] == host).cache()
        # Map the RDD to a tuple of (host1)
        rdd = rdd.map(lambda x: (x[1]))
    elif type == 'from':
        # Filter the RDD to include only connections involving the given host
        rdd = rdd.filter(lambda x: x[1] == host).cache()
        # Map the RDD to a tuple of (host2)
        rdd = rdd.map(lambda x: (x[2]))

    # Create a set of unique hostnames
    host_set = set(rdd.collect())
    # Return the list of hostnames
    return list(host_set)


# Create a function to get the hostname that generated most connections in the last hour
def get_most_connected_host(rdd):
    # Filter the RDD to include only connections made in the last hour
    rdd = rdd.filter(lambda x: x[0] >= time_one_hour_ago).cache()
    # Map the RDD to a tuple of (host1, host2)
    rdd = rdd.map(lambda x: (x[1], x[2]))
    # Flatten the RDD to a list of hostnames
    host_list = rdd.flatMap(lambda x: x).collect()
    # Create a dictionary to count the occurrences of each hostname
    host_counts = {}
    for host in host_list:
        if host in host_counts:
            host_counts[host] += 1
        else:
            host_counts[host] = 1
    # Find the hostname with the most connections
    most_connected_host = max(host_counts, key=host_counts.get)
    # Return the hostname
    return most_connected_host


if __name__ == "__main__":
    # Create Spark context
    sc = SparkContext("local", "CSV Files")
    while True:        
        start_time = time.time()
        # List files in the logs folder
        files = os.listdir("logs/")

        if files == []:
            print("There are not new logs")
        else:
            # Load the CSV files as an RDD
            rdd = sc.textFile("logs/*.csv")
            # Apply the mapping function to the RDD
            rdd = rdd.map(map_csv_data)
            
            # User input to check the connections in the logs
            host = input("Enter the host: ")

            # Return a list of hostnames connected to the given parameter host during the last hour
            connected_to_hosts = get_hosts_connections(rdd, host, 'to')
            print(f"List of hostnames connected to {host} in the last hour: {connected_to_hosts}")

            # Return a list of hostnames that received connections from the given parameter host during the last hour
            connected_from_hosts = get_hosts_connections(rdd, host, 'from')
            print(f"List of hostnames that received connections from {host} in the last hour: {connected_from_hosts}")

            # Return host that generated most connections in the last hour
            most_connected_host = get_most_connected_host(rdd)
            print(f"The hostname that generated the most connections in the last hour is {most_connected_host}")

            # Move the log files from logs folder to processed_logs folder
            for file in files:
                shutil.move('logs/' + file, 'processed_logs/')

            rdd.unpersist()
            end_time = time.time()
            print("Total runtime:", end_time - start_time)

        # Sleep for one hour
        time.sleep(3600)

