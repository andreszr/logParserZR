import datetime
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import time
import shutil
import os

# Create a SparkSession
spark = SparkSession.builder.appName("CSV Log Parser").getOrCreate()

schema = StructType([
    StructField("timestamp", IntegerType()),
    StructField("host_from", StringType()),
    StructField("host_to", StringType())
])

def get_last_hour_records(df):
    now = datetime.datetime(2013, 4, 24, 11, 3, 59, 342380)
    # now = datetime.datetime.now()
    one_hour_ago = now - datetime.timedelta(hours=1,minutes=5)
    filtered_df = df.filter((col("timestamp") >= one_hour_ago.timestamp()) & (col("timestamp") <= now.timestamp()) ).cache()
    return filtered_df

def parse_log(host, path='logs/'):
  try:
    # Read the log file as a DataFrame
    df = spark.read.csv(path, header=False, sep = ' ', schema=schema)

    # Filter the DataFrame for connections in the last hour
    filtered_df = get_last_hour_records(df)

    # Get the list of hostnames connected to the parameter host
    filtered_df_1 = filtered_df.filter(filtered_df["host_to"] == host).cache()
    received_hosts = filtered_df_1.select("host_from").distinct().rdd.map(lambda row: row[0]).collect()
    print(f"List of hostnames connected to {host} in the last hour: {received_hosts}")

    # Get the list of hostnames that received connections from the parameter host
    filtered_df_2 = filtered_df.filter(filtered_df["host_from"] == host).cache()
    connected_hosts = filtered_df_2.select("host_to").distinct().rdd.map(lambda row: row[0]).collect()
    print(f"List of hostnames that received connections from {host} in the last hour: {connected_hosts}")

    # Get the hostname that generated the most connections in the last hour
    most_active_host = filtered_df.groupBy("host_from").count().sort(desc("count")).first()
    if (most_active_host == None):
      print(f"there were not connections in the last hours to any hostname")
    else:
      print(f"The hostname that generated the most connections in the last hour is {most_active_host[0]}")
  
  except FileNotFoundError as e:
      print(f"FileNotFoundError: {str(e)}")
  except Exception as e:
      print(f"An exception of type {type(e).__name__} occurred with message: {str(e)}")
    

if __name__ == "__main__":
    while True:
        start_time = time.time()

        # List files in the logs folder
        files = os.listdir("logs/")

        if files == []:
            print("There are not new logs")
        else:
            host = input("Enter the host: ")    
            parse_log(host)
            
            # Move the log files read from the directory to another directory
            for file in files:
                shutil.move('logs/' + file, 'processed_logs/')

            spark.catalog.clearCache()
            end_time = time.time()
            print("Total runtime:", end_time - start_time)
    
        # Sleep for one hour
        time.sleep(3600)

        