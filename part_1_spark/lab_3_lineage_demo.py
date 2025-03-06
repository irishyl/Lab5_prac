from pyspark.sql import SparkSession

def main(spark):
    df = spark.read.csv("data.csv") 
    filtered_df = df.filter(df.age > 30)
    grouped_df = filtered_df.groupBy("city").count()

    grouped_df.show()


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('demo').getOrCreate()

    main(spark)
