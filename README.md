# IlkProje
Yeni başlıyorum
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("İstanbul BTK YAz Okulu MovieData EDA").getOrCreate()
rawDF = spark.read.option("Header",True).option("inferSchema",True).csv('/FileStore/tables/book_ratings.csv')
rawDF.printSchema()
rawDF.show()
rawDF.count()
movieByuserDF = rawDF.groupBy("userId").count()
movieByuserDF.show()
movieBymovieDF = rawDF.groupBy("movieId").count()
movieBymovieDF.count()
movieByuserDF.orderBy("count",ascending=False).show(5)
display(movieByuserDF.orderBy("count",ascending=False))
avgRatingDF = rawDF.filter("movieId=429 or movieId=431").groupBy("movieId").avg("rating").withColumnRenamed("avg(rating)","avgByRating")
avgRatingDF.withColumn("fixAvgRating",round(col('avgByRating'),2)).show()
