import random

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("Salting Demo")
             .master("local[3]")
             .getOrCreate())

    # Create a list with skewed data
    skewed_data = ['A'] * 10000 + ['B'] * 100 + ['C'] * 10 + ['D']

    # Create a DataFrame with skewed data
    data = [(key,) for key in skewed_data]
    df = spark.createDataFrame(data, ["key"])
    # Show the initial distribution of the 'key' column
    print("Initial Distribution:")
    df.groupBy("key").count().show()

    # Identify the skewed key or if you know the key, just mention the value here
    skewed_key = df.groupBy('key').count().orderBy('count', ascending=False).first()[0]


    # We want to break the skewed key into 5 partitions
    def rand():
        return random.randint(0, 4)
    rand_udf = udf(rand)

    # Add a random salt to the skewed key
    df = df.withColumn('salted_key',
                       F.when(F.col('key') == skewed_key, F.concat(F.col('key'), F.lit('_'), rand_udf())).otherwise(
                           F.col('key')))

    df.groupBy("salted_key").count().show()

    # Now the dimension table
    dimension_data = [("A", "letter A"),
                    ("B", "letter B"),
                    ("C", "letter C"),
                    ("D", "letter D")]
    dimension_df = spark.createDataFrame(dimension_data).toDF("key", "val")
    salt_df = spark.range(0, 5)
    salt_df.show()

    salted_dimension_df= dimension_df.join(salt_df, how="cross").withColumn("salted_key",
                                                                 F.when(
                                                                     F.col('key') == skewed_key,F.concat("key", F.lit("_"), "id")
                                                                 ).otherwise(
                                                                    F.col('key'))
                                                                 ).drop("id").distinct()
    salted_dimension_df.show()

    # Now join on salted_key
    joined_df = df.join(salted_dimension_df, how="inner", on=df['salted_key']==salted_dimension_df['salted_key'])
    joined_df.groupBy('val').count().show()

