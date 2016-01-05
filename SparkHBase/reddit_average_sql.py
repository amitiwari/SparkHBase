from pyspark.sql import SQLContext
from pyspark.sql import DataFrameWriter
from pyspark import SparkContext 
from pyspark import SparkConf
import sys, operator
import re, string
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType



inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('reddit averages sql')
sc = SparkContext()

sqlContext = SQLContext(sc)
 
schema = StructType([
    StructField('subreddit', StringType(), False),
    StructField('score', IntegerType(), False)
]) 

comments = sqlContext.read.json(inputs, schema=schema)
#averages = comments.select('subreddit', 'score').groupby('subreddit').avg()

comments.registerTempTable('comments')
averages = sqlContext.sql("""
    SELECT subreddit, AVG(score)
    FROM comments
    GROUP BY subreddit
""")

averages.coalesce(1).write.save(output, format='json', mode='overwrite')
#averages.show()