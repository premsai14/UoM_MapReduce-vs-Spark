from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import time

def main():
    spark = SparkSession.builder.appName("AirlineDelayAnalysis").getOrCreate()

    # Load the dataset from S3
    df = spark.read.option("header", "true").csv("s3://sparkhivesqlbucketlab/Spark_sparkSQL/input/DelayedFlights-updated.csv")
    df.createOrReplaceTempView("delayed_flights")

    # Define the queries for each type of delay
    queries = {
        "carrier_delay": "SELECT Year, SUM(CarrierDelay) AS total_carrier_delay FROM delayed_flights GROUP BY Year ORDER BY Year",
        "nas_delay": "SELECT Year, SUM(NASDelay) AS total_nas_delay FROM delayed_flights GROUP BY Year ORDER BY Year",
        "weather_delay": "SELECT Year, SUM(WeatherDelay) AS total_weather_delay FROM delayed_flights GROUP BY Year ORDER BY Year",
        "late_aircraft_delay": "SELECT Year, SUM(LateAircraftDelay) AS total_late_aircraft_delay FROM delayed_flights GROUP BY Year ORDER BY Year",
        "security_delay": "SELECT Year, SUM(SecurityDelay) AS total_security_delay FROM delayed_flights GROUP BY Year ORDER BY Year"
    }

    # Initialize an empty DataFrame to store query execution times
    time_df = spark.createDataFrame([], schema="query_name STRING, execution_time FLOAT")

    # Execute each query and measure the time taken
    for delay_type, query in queries.items():
        start_time = time.time()
        result = spark.sql(query)
        end_time = time.time()
        
        # Calculate the execution time
        execution_time = end_time - start_time
        
        # Append the execution time to the DataFrame
        new_row = spark.createDataFrame([(delay_type, execution_time)], schema="query_name STRING, execution_time FLOAT")
        time_df = time_df.union(new_row)
        
        # Save the query results to S3
        output_path = f"s3://sparkhivesqlbucketlab/Spark_sparkSQL/output/{delay_type}_per_year"
        result.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

    # Save the execution times to a CSV file in S3
    time_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("s3://sparkhivesqlbucketlab/Spark_sparkSQL/output/query_execution_times")

    spark.stop()

if __name__ == "__main__":
    main()
