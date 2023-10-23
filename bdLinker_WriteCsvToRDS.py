import boto3
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("connection").getOrCreate()

df = spark.read.option("header",True).option("inferSchema",True).csv('s3://*ARN to your file*')
df.printSchema()


url = "jdbc:postgresql://**bdd endpoint**:5432/database-1-testdblinker"
table = "*your csv name*"
driver = "org.postgresql.Driver"
user = "decodeur"
password = "XXXXXXX"

df.write.format("jdbc").option("driver",driver).option("url",url).option("dbtable",table).option("user",user).option("password",password).save()

