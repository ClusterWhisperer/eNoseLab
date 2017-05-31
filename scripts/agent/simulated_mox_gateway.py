#! /usr/bin/python
import json
import datetime
from optparse import OptionParser
from kafka import KafkaProducer

'''
simulated_mox_gateway.py -  reads the HT_Sensor_dataset.dat file and HT_Sensor_metadata.dat file 
and builds json.

Ex:
id time	      R1         R2         R3         R4         R5         R6         R7        R8        Temp.      Humidity
0  -0.999750  12.862100  10.368300  10.438300  11.669900  13.493100  13.342300  8.041690  8.739010  26.225700  59.052800

simulator would read the static file (@param -f) and converts each row to json object of following format:

{
  "guid": "0-ZZZ123456787B",
  "eventTime": "2017-05-18T23:37:32.618488Z",
  "payload": {
     "format": "urn:enose:gateway:summary",
     "data": {
       "R1": 12.862100,
       "R2": 10.368300,
       "R3": 10.438300,
       "R4": 11.669900,
       "R5": 13.342300,
       "R6": 13.342300,
       "R7": 8.041690,
       "R8": 8.739010,
       "temp": 26.225700,
       "humid": 59.052800
     }
   }
}


Usage:
./simulated_mox_sensor.py -i 38412f32-e708-447c-8842-6d65357aac89 -f ../data/HT_Sensor_dataset.dat

'''


# metadata inmem lookup (only 100 inductions)
metadata_lookup = {}


# metadata file column offset
METADATA_ID = 0
METADATA_DATE = 1
METADATA_CLASS = 2
METADATA_INDUCTION_START = 3
METADATA_INDUCTION_DURATION = 4


# data file column offset
DATA_OFFSET_ID = 0 
DATA_OFFSET_TIME = 1
DATA_OFFSET_R1 = 2
DATA_OFFSET_R2 = 3
DATA_OFFSET_R3 = 4
DATA_OFFSET_R4 = 5
DATA_OFFSET_R5 = 6
DATA_OFFSET_R6 = 7
DATA_OFFSET_R7 = 8
DATA_OFFSET_R8 = 9 
DATA_OFFSET_TEMP = 10
DATA_OFFSET_HUMID = 11

def load_metadata(file):
	with open(file) as f:
		next(f)
		for line in f:
			words = line.split()
			metadata_lookup[words[DATA_OFFSET_ID]] = words

def get_date_iso_format(ind_id, event_offset):
	induction_metadata=metadata_lookup.get(ind_id)
	induction_date = induction_metadata[METADATA_DATE]
	induction_time=float(event_offset)*60 # convert to mins. 
	return (datetime.datetime.strptime(induction_date,'%m-%d-%y') + datetime.timedelta(minutes = induction_time)).isoformat()


def build_json_from_data_line(line, guid):
	words = line.split()
	l = {}
	l['guid'] = guid
	l['eventTime'] = get_date_iso_format(words[DATA_OFFSET_ID], words[DATA_OFFSET_TIME])
	p = {}
	p['format'] = 'urn:enose:gateway:summary'
	d = {}
	d['R1'] = float(words[DATA_OFFSET_R1])
	d['R2'] = float(words[DATA_OFFSET_R2])
	d['R3'] = float(words[DATA_OFFSET_R3])
	d['R4'] = float(words[DATA_OFFSET_R4])
	d['R5'] = float(words[DATA_OFFSET_R5])
	d['R6'] = float(words[DATA_OFFSET_R6])
	d['R7'] = float(words[DATA_OFFSET_R7])
	d['R8'] = float(words[DATA_OFFSET_R8])
	d['temp'] = float(words[DATA_OFFSET_TEMP])
	d['humid'] = float(words[DATA_OFFSET_HUMID])
	p["data"] = d
	l["payload"] = p
	return json.dumps(l)

#block till single message is sent
def write_to_kafka(producer, topic, msg):
	future = producer.send(topic, msg)
	result = future.get(timeout=60)


def main():
	parser = OptionParser()
	parser.add_option("-d", "--data_file", dest="data_file", help="input data file")
	parser.add_option("-m", "--meta_data_file", dest="metadata_file", help="input metadata file")
	parser.add_option("-i", "--gateway_id", dest="guid", help="GUID")
	parser.add_option("-s", "--kafka_server", dest="server", help="kafka server list")
	parser.add_option("-t", "--topic", dest="topic", help="kafka topic")
	(options, args) = parser.parse_args()

	# json_data=[]

	producer = KafkaProducer(bootstrap_servers=options.server)

	print 'Loading metadata file:{} in memory.'.format(options.metadata_file)
	load_metadata(options.metadata_file)

	print 'Processing data file:{}.\n Sending to kafka server:{}\n Kafka Topic:{}\n '.format(options.data_file, options.server, options.topic)
	with open(options.data_file) as f:
		next(f) #skip the header line
		for line in f:
			write_to_kafka(producer, options.topic, build_json_from_data_line(line, options.guid))
			print '.'

			
	# print json_data[-100:]		

	

if __name__ == '__main__':
	main()