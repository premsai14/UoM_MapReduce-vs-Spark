DROP TABLE IF EXISTS delayed_flights;
CREATE EXTERNAL TABLE  delayed_flights (
    Year INT,
    Month INT,
    DayofMonth INT,
    DayOfWeek INT,
    DepTime INT,
    CRSDepTime INT,
    ArrTime INT,
    CRSArrTime INT,
    UniqueCarrier STRING,
    FlightNum STRING,
    TailNum STRING,
    ActualElapsedTime INT,
    CRSElapsedTime INT,
    AirTime INT,
    ArrDelay INT,
    DepDelay INT,
    Origin STRING,
    Dest STRING,
    Distance INT,
    TaxiIn INT,
    TaxiOut INT,
    Cancelled INT,
    CancellationCode STRING,
    Diverted INT,
    CarrierDelay INT,
    WeatherDelay INT,
    NASDelay INT,
    SecurityDelay INT,
    LateAircraftDelay INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://sparkhivesqlbucketlab/Hadoop_hiveQL/input/'
TBLPROPERTIES ("skip.header.line.count"="1");

SET hive.cli.print.header=true;

INSERT OVERWRITE LOCAL DIRECTORY 's3://sparkhivesqlbucketlab/Hadoop_hiveQL/output/year_wise_carrier_delay'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT Year, SUM(COALESCE(CAST(CarrierDelay AS INT), 0)) AS total_carrier_delay
FROM delayed_flights
GROUP BY Year
ORDER BY Year;


INSERT OVERWRITE LOCAL DIRECTORY 's3://sparkhivesqlbucketlab/Hadoop_hiveQL/output/year_wise_nas_delay'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT Year, SUM(COALESCE(CAST(NASDelay AS INT), 0)) AS total_nas_delay
FROM delayed_flights
GROUP BY Year
ORDER BY Year;


INSERT OVERWRITE LOCAL DIRECTORY 's3://sparkhivesqlbucketlab/Hadoop_hiveQL/output/year_wise_weather_delay'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT Year, SUM(COALESCE(CAST(WeatherDelay AS INT), 0)) AS total_weather_delay
FROM delayed_flights
GROUP BY Year
ORDER BY Year;


INSERT OVERWRITE LOCAL DIRECTORY 's3://sparkhivesqlbucketlab/Hadoop_hiveQL/output/year_wise_late_aircraft_delay'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT Year, SUM(COALESCE(CAST(LateAircraftDelay AS INT), 0)) AS total_late_aircraft_delay
FROM delayed_flights
GROUP BY Year
ORDER BY Year;


INSERT OVERWRITE LOCAL DIRECTORY 's3://sparkhivesqlbucketlab/Hadoop_hiveQL/output/year_wise_security_delay'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT Year, SUM(COALESCE(CAST(SecurityDelay AS INT), 0)) AS total_security_delay
FROM delayed_flights
GROUP BY Year
ORDER BY Year;
