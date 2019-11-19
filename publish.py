#!/usr/bin/python3
# -*- coding: utf-8 -*-
import paho.mqtt.client as mqtt
import configparser
import time
import random
import zlib
import binascii
import json
import uuid
import sys

#### Configuration #####
connflag = False
config = configparser.ConfigParser()
config.read(sys.path[0]+'/configuracion.ini')
#broker_host	= str(config['MQTT']['broker_host'])
broker_host	= str(config['MQTT']['broker_host'])
broker_port	= int(config['MQTT']['broker_port'])
#username	= str(config['MQTT']['mqtt_user'])
#password	= str(config['MQTT']['mqtt_pass'])
mqtt_qos	= int(config['MQTT']['mqtt_qos'])
keep_alive	= int(config['MQTT']['mqtt_keep_alive'])
debug		= bool(config['DEV']['debug'])
client_id = "mqtt-publisher-{0}".format(uuid.uuid4().hex[:8])
#client_id = 'mqtt-publisher-' + str(uuid.uuid4())
#### Configuration #####

def on_connect(client, userdata, flags, rc):
	global connflag
	if rc==0:
		connflag = True
		print("connection OK rc={}".format(str(rc)))
		# subscribe to topic
		client.subscribe("hermes/temp/out", qos=mqtt_qos)
	else:
		print("Bad connection Returned code={}".format(str(rc)))


def on_disconnect(client, userdata, rc):
	global connflag
	if rc != 0:
		connflag = False
		print("Unexpected MQTT disconnection. Will auto-reconnect 5 seg...")
		time.sleep(5)


def on_message(client, userdata, msg):
	# decompress data
	hex_msg = msg.payload.decode('utf-8')

	# debug
	if debug==True:
		print("Recv: {}".format(str(hex_msg)))
	
	bin_data = binascii.unhexlify(hex_msg)
	json_data = zlib.decompress(bin_data)
	parsed_json = json.loads(json_data)
	
	print("Recv:{} {} ret:{} qos:{}".format(str(msg.topic), str(parsed_json), str(msg.retain), str(msg.qos)))
	
	#if msg.retain==1:
	#	print("This is a retained message")


def on_publish(client, obj, mid):
    #print("mid: " + str(mid))
    pass


def on_subscribe(client, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


def on_log(client, obj, level, string):
    print(string)


#### Connection #####
client = mqtt.Client(client_id=client_id, clean_session=False)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message
client.on_publish = on_publish
client.on_subscribe = on_subscribe
#client.on_log = on_log
#client.username_pw_set(username, password)
client.connect(broker_host, broker_port, keep_alive)
client.loop_start()
#### Connection #####

# main
while True:
	time.sleep(1)
	if connflag == True:
		tempreading = round(random.uniform(20.0,25.0))
		json_data = {'temperature': tempreading}
		data_string = json.dumps(json_data)
		
		# compress data
		compressed = zlib.compress(data_string.encode("utf-8"))
		hex_data = binascii.hexlify(compressed)
		
		# send data to topic
		client.publish("hermes/temp/in", hex_data, qos=mqtt_qos, retain=True)
		
		# debug
		if debug==True:
			#print("Sent: Temp " + "%.2f" % tempreading )
			print("Sent: Temp {} ".format(str(tempreading)))
		
		#client.publish("hermes/hotword/default/detected","1",qos=1)
		#client.publish("hermes/intent/INTENT_NAME","2",qos=1)
	else:
		print("waiting for connection...")
# main
