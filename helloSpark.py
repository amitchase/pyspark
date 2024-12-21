from pyspark.sql import *
if __name__ == '__main__':
    print("hello world")

    spark = SparkSession \
        .builder \
        .appName("HelloSpark") \
        .master("local[2]") \
        .getOrCreate()

    data_list = [("imdadul", 34),
                 ("amit", 34),
                 ("praveen",32)]

    df = spark.createDataFrame(data_list).toDF("Name", "Age")
    df.show()