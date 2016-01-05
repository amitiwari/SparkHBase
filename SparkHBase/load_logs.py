from pyspark.sql import SQLContext
from pyspark import SparkContext 
from pyspark import SparkConf
import sys, operator
import re, string
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row
import datetime

linere = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")
def find_match(line):
    matcher= linere.match(line)    
    if(matcher):
        hostname=matcher.group(1)
        datestring=matcher.group(2)
        date_string=datetime.datetime.strptime(datestring, '%d/%b/%Y:%H:%M:%S')
        path=matcher.group(3)
        numBytes=int(matcher.group(4))               
        return (hostname,date_string,path,numBytes)

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('load logs')
sc = SparkContext()
sqlContext = SQLContext(sc)

text = sc.textFile(inputs)

rdd1=text.map(lambda line: find_match(line)).filter(lambda x: x is not None)

rdd_rowobjects=rdd1.map(lambda p: Row(
    host=p[0],
    date=p[1],
    path=p[2],
    numbytes=p[3],    
    )
) 

logs_sql=sqlContext.createDataFrame(rdd_rowobjects)

logs_sql.write.format('parquet').save(output)

readparquet=sqlContext.read.parquet(output)

totalbytes=readparquet.select('host', 'numbytes').groupby('host').sum()

totalbytes.show();