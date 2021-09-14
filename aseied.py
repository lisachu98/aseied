from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, avg
import matplotlib.pyplot as plt

if __name__ == "__main__":
    sesja = SparkSession.builder.appName("Taxi").getOrCreate()
    sc = sesja.sparkContext
    sc.setLogLevel('ERROR')

    dataFrameReader = sesja.read
    przejazdy19G = dataFrameReader.option("header","true").option("delimiter",",").option("inferschema","true").csv("s3n://testaseied/green_tripdata_2019-05.csv")
    przejazdy19Y = dataFrameReader.option("header","true").option("delimiter",",").option("inferschema","true").csv("s3n://testaseied/yellow_tripdata_2019-05.csv")
    przejazdy20G = dataFrameReader.option("header","true").option("delimiter",",").option("inferschema","true").csv("s3n://testaseied/green_tripdata_2020-05.csv")
    przejazdy20Y = dataFrameReader.option("header","true").option("delimiter",",").option("inferschema","true").csv("s3n://testaseied/yellow_tripdata_2020-05.csv")
    
    przejazdy19G = przejazdy19G.select("lpep_pickup_datetime", "lpep_dropoff_datetime", "trip_distance")
    przejazdy19G = przejazdy19G.withColumnRenamed("lpep_pickup_datetime","pickup_datetime").withColumnRenamed("lpep_dropoff_datetime","dropoff_datetime")
    przejazdy19G = przejazdy19G.withColumn("drive_time", (to_timestamp(col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss").cast("long") - to_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss").cast("long"))/3600)
    przejazdy19G = przejazdy19G.withColumn("avg speed", col("trip_distance")/col("drive_time"))

    przejazdy19Y = przejazdy19Y.select("tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance")
    przejazdy19Y = przejazdy19Y.withColumnRenamed("tpep_pickup_datetime","pickup_datetime").withColumnRenamed("tpep_dropoff_datetime","dropoff_datetime")
    przejazdy19Y = przejazdy19Y.withColumn("drive_time", (to_timestamp(col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss").cast("long") - to_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss").cast("long"))/3600)
    przejazdy19Y = przejazdy19Y.withColumn("avg speed", col("trip_distance")/col("drive_time"))

    przejazdy20G = przejazdy20G.select("lpep_pickup_datetime", "lpep_dropoff_datetime", "trip_distance")
    przejazdy20G = przejazdy20G.withColumnRenamed("lpep_pickup_datetime","pickup_datetime").withColumnRenamed("lpep_dropoff_datetime","dropoff_datetime")
    przejazdy20G = przejazdy20G.withColumn("drive_time", (to_timestamp(col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss").cast("long") - to_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss").cast("long"))/3600)
    przejazdy20G = przejazdy20G.withColumn("avg speed", col("trip_distance")/col("drive_time"))

    przejazdy20Y = przejazdy20Y.select("tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance")
    przejazdy20Y = przejazdy20Y.withColumnRenamed("tpep_pickup_datetime","pickup_datetime").withColumnRenamed("tpep_dropoff_datetime","dropoff_datetime")
    przejazdy20Y = przejazdy20Y.withColumn("drive_time", (to_timestamp(col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss").cast("long") - to_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss").cast("long"))/3600)
    przejazdy20Y = przejazdy20Y.withColumn("avg speed", col("trip_distance")/col("drive_time"))

    przejazdy19 = przejazdy19G.unionAll(przejazdy19Y)
    przejazdy20 = przejazdy20G.unionAll(przejazdy20Y)

    przejazdy19.show(5)

    przejazdy19.groupBy().avg("avg speed").show()
    przejazdy20.groupBy().avg("avg speed").show()

    speed19 = przejazdy19.groupBy().avg("avg speed").first()['avg(avg speed)']
    speed20 = przejazdy20.groupBy().avg("avg speed").first()['avg(avg speed)']

    plt.bar(["2019", "2020"], [speed19, speed20])
    plt.xlabel("Year")
    plt.ylabel("Average speed [mph]")
    plt.title("Pandemic impact on average speed of taxis in New York")
    plt.savefig('result.png')
