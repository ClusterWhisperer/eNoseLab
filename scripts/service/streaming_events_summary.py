from __future__ import print_function

import sys
import re
import json

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import OffsetRange
from operator import add

if __name__ == "__main__":

	sc = SparkContext(appName="eNoseServiceCheck")
	ssc = StreamingContext(sc, 2)
	sqlContext = SQLContext(sc)
	
	kvs = KafkaUtils.createDirectStream(ssc, ['lab2'], {"metadata.broker.list": 'kafka1:9092'})
	jsonDStream = kvs.map(lambda (key, value): value)

	# jsonDStream.pprint(5)
	
	def process_window(time, rdd):
		try:
			json = sqlContext.read.json(rdd)	
			json.registerTempTable("events")
			# print("JSON Schema\n=====")
			# json.printSchema()
			sqlContext.sql(" select guid, count(1) count, avg(payload.data.R1) avg_r1, avg(payload.data.humid) avg_humid,avg(payload.data.temp) avg_temp from events group by guid ").show(10)
			sqlContext.dropTempTable("events")	
		except:
			print ("****** ERROR ***********")
			pass
			

	jsonDStream.foreachRDD(process_window)
	ssc.start()
	ssc.awaitTermination()