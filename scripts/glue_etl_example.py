from pyspark import SparkContext 
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

sc = SparkContext.getOrCreate()
gc = GlueContext(sc)
spark = gc.spark_session

plist = '[{"Name":"Jose","Idade":"25"},{"Name":"Maria","Idade":"32"},{"Name":"Joao","Idade":"47"}]'

spark_df = spark.read.json(sc.parallelize([plist]))
glue_df = DynamicFrame.fromDF(spark_df, gc, "glue_df")

glue_df.printSchema()
glue_df.toDF().show()