from __future__ import print_function

import sys
import re
import json
import random

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import OffsetRange
from operator import add

if __name__ == "__main__":

	sc = SparkContext(appName="eNoseServiceCheck")
	ssc = StreamingContext(sc, 5)
	sqlContext = SQLContext(sc)
	#ssc.checkpoint("enose_state_change_checkpoint")
	
	kvs = KafkaUtils.createDirectStream(ssc, ['lab2'], {"metadata.broker.list": 'kafka1:9092'})
	jsonDStream = kvs.map(lambda (key, value): value)

	# jsonDStream.pprint(5)

	def process_readings(row_list):
		return random.choice(['banana', 'wine', 'normal'])

	def process_process_guid_records(k, list):
		curr_label = process_readings(list)
		print ('*** Classified guid:%s as label:%s \n' % (k, curr_label))
		#return (k, curr_label)

	
	def process_window(time, rdd):
		try:
			json = sqlContext.read.json(rdd)	
			json.registerTempTable("events")
			
			# getting average readings
			sqlContext.sql(" select guid, count(1) count, avg(payload.data.R1) avg_r1, " + \
				"avg(payload.data.R2) avg_r2, avg(payload.data.R1) avg_r3, avg(payload.data.R4) avg_r4, " + \
				" avg(payload.data.R5) avg_r5, avg(payload.data.R6) avg_r6, avg(payload.data.R7) avg_r7, " + \
				" avg(payload.data.R8) avg_r8, avg(payload.data.humid) avg_humid,avg(payload.data.temp) avg_temp " + \
				"from events group by guid ").show(10)


			df_all_rows = sqlContext.sql(" select * from events ")

			# for each guid;  get classification
			rows = df_all_rows.rdd
			rows.map(lambda r: (r['guid'], r)).groupByKey().foreach(lambda (k,v): process_process_guid_records(k,v))
			
			#writing to datastore 

			df_all_rows.write.parquet("file:///output/enose_events")

			sqlContext.dropTempTable("events")	
		except:
			# print ("****** ERROR ***********")
			pass
			

	jsonDStream.foreachRDD(process_window)
	ssc.start()
	ssc.awaitTermination()