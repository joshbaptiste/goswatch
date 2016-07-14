#!/opt/python/current2/bin/python
import sys
import thread
import Queue
import time
from cStringIO import StringIO
from gzip import GzipFile
from base64 import b64decode
from xml.etree import ElementTree as ET
from xml.etree.ElementTree import ParseError
import redis
from redis.exceptions import ConnectionError, InvalidResponse, PubSubError, RedisError
from logwatch import log
from processTR import process_TR
from processWO import process_WO
from processPTR import process_PTR
from processRRU import process_RRU
from processIR import process_IR
from dbwatch import get_site_flag
import datetime
import time, os
import config
#goswatch.py

# Python based script watches redis queue for  Sev3 or lower IR/PTR and TR adds to GOS FU as soon as hits queue.
# Removes from GOS FU as soon as leaves queue or is closed...
# Changes it to Info section if TR is implemented
# Uses user goswatch in trends

REC_TYPES = ["TR", "WO", "PTR", "RRU", "IR"]
dataQueue = Queue.Queue()
thread_lock = thread.allocate_lock()
MSG_COUNTER = 0

def get_record_type():
    txt_xml_record = consumer_queue()
    try:
        xml_record = ET.fromstring(txt_xml_record)
        rec_type = xml_record.attrib['typeName']
    except (KeyError,ParseError):
        log.error('Error Parsing String XML .. Skipping pls check stdout file')
        print txt_xml_record
        return [False, False]

    return [rec_type, xml_record]
 
def process_record(redis_handle):   
    record_type, xml_record = get_record_type()
    if record_type in REC_TYPES:
        if record_type == "TR": 
            process_TR(xml_record, redis_handle)
        if record_type == "WO": 
            process_WO(xml_record, redis_handle)
        if record_type == "PTR": 
            process_PTR(xml_record, redis_handle)
        if get_site_flag(datetime.datetime.now().hour) == 'M':
            if record_type == "RRU": 
                process_RRU(xml_record, redis_handle)
        if record_type == "IR": 
            process_IR(xml_record, redis_handle)
    else:
        return None
            
def producer_queue(txt_xml_output):
    global MSG_COUNTER
    MSG_COUNTER+=1
    log.debug("Adding msg %d to Queue" % MSG_COUNTER)
    dataQueue.put(txt_xml_output)

def consumer_queue():
    txt_xml_output = dataQueue.get(block=True)
    log.debug("Retrieving msg %d from Queue" % MSG_COUNTER)
    return txt_xml_output

def connect_to_redis():
    while True:
        try:
            redis_handle = redis.Redis(config.redis_host)
            pubsub_handle = redis_handle.pubsub()
            pubsub_handle.subscribe(config.redis_channel)
            break
        except ConnectionError:
            log.error("Connection issue with Redis! %s" % str(sys.exc_info()))
            log.info("Connecting in 10 seconds")
            time.sleep(10)

    return [pubsub_handle,redis_handle]

def main():
    pubsub_handle,redis_handle = connect_to_redis()
    print 'monitoring channel ' + config.phase 
    for msg in pubsub_handle.listen():
        if msg['type'] == 'message':
            file_output = GzipFile(mode='r',fileobj=StringIO(b64decode(msg['data'])))
            txt_xml_output = file_output.read()
#            print txt_xml_output
            thread.start_new_thread(producer_queue, (txt_xml_output,))
            thread.start_new_thread(process_record, (redis_handle,))
                
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print "Exiting !!"
        sys.exit(1)
