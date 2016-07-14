import re
import sys
import datetime
from logwatch import log
from dbwatch import trends
from config import GROUPS
#processPTR.py

# Python based script watches redis queue for
# PTRs adds to GOS FU as soon as hits queue.
# Removes from GOS FU as soon as leaves queue or is closed...
# Uses user goswatch in trends
RECORD_TYPE="PTR"

def is_record_tracked(assignee_code, record_num, redis_handler):
    try:
        if redis_handler.hexists(assignee_code, record_num):
            return record_num
        else:
            return False
    except:
        log.error("{} {} Could not query redis {}".format\
                  (RECORD_TYPE, record_num, str(sys.exc_info()) ))

def update_cache_timestamp(record_num, redis_handler):
    try:
        redis_handler.set('{}:time'.format\
        (record_num) , datetime.datetime.now().utcnow().strftime('%H') )
    except:
        log.error("{} {} Could not update cache timestamp {}".format\
                (RECORD_TYPE, record_num, str(sys.exc_info()) ))

def check_cache_timestamp(record_num, redis_handler):
    try:
        timestamp = redis_handler.get('{}:time'.format(record_num))
        if timestamp:
            #if timestamp in MIAMI_SHIFT:
                return True
    except:
        log.error("{} {} Could not query redis {}".format\
            (RECORD_TYPE, record_num, str(sys.exc_info()) ))


def add_redis_record(assignee_code, record_num, status, redis_handler):
    try:
        log.info("{} {} Adding key value to redis".format\
        (RECORD_TYPE, record_num))
        redis_handler.hset(assignee_code, record_num, status)
        redis_handler.set('{}:time'.format(record_num), datetime.datetime.now().utcnow().strftime('%H'))
    except:
        log.error("{} {} Could not add to redis {}".format\
        (RECORD_TYPE, record_num, str(sys.exc_info()) ))


def delete_redis_record(assignee_code, record_num, redis_handler):
    try:
        log.info("{} {} Removing from redis".format(RECORD_TYPE, record_num))
        redis_handler.hdel(assignee_code, record_num)
    except:
        log.error("{} {} Could not delete from redis {}".format\
        (RECORD_TYPE, record_num, str(sys.exc_info()) ))

def track_record(aproach_fields, redis_handler):
    record_num, title, assignee_code, assignee_name, status,\
    severity, urgency, phase = aproach_fields

    if not assignee_name:
        add_redis_record(assignee_code, record_num, status, redis_handler)
        comment = "{} PTR {}[{}]".format(phase, severity, urgency)
        follow_up_record(record_num, title, comment)
    else:
        log.info("{} {} is assigned to {}, ignoring".format(RECORD_TYPE, record_num, assignee_name))


def follow_up_record(record_num, title, comment):
    kwargs={'action': 'update_to_follow','record_num': record_num, \
    'title': title, 'comment': comment, 'rectype': RECORD_TYPE}
    query_result = trends(**kwargs)
    # update record...(will fail if record not in DB)
    if not query_result:
        # if record does not exist insert it
        log.info("%s %s UPDATE affected %s rows. Executing INSERT" % (RECORD_TYPE, record_num, query_result))
        kwargs={'action': 'follow-up','record_num': record_num, \
        'title': title, 'comment': comment, 'rectype': RECORD_TYPE}
        trends(**kwargs)


def delete_trends_record(record_num, phase, cached_status, status):
    """ still need to check phase, if PRD move to info, else delete.
    can probably use status
    """
    # if PRD PTR and status has changed...
    if cached_status != status:
        # move to info section
        kwargs={'action': 'moveto', 'record_num': record_num, 'section': 4, 'status': status, 'rectype': RECORD_TYPE}
    else:
        # otherwise move to trash
        kwargs={'action': 'moveto','record_num': record_num,'section': 0, 'status': '', 'rectype': RECORD_TYPE}
    trends(**kwargs)


def extract_xml_data(xml_record):
    title, assignee_code, assignee_name, status, severity, urgency, phase  = \
    "","", "", "", "", "", ""
    record_num = xml_record.attrib['id']
    for node in xml_record.iter('field'):
        if node.attrib.get('xmlname') == 'Title':
            title =  node.text.lstrip()
        if node.attrib.get('xmlname') == "AssigneeGroup":
            assignee_code = node.attrib.get('code')
        if node.attrib.get('xmlname') == "AssigneeName":
            assignee_name = node.text
        if node.attrib.get('xmlname') == "Status":
            status = node.text
        if node.attrib.get('xmlname') == "Severity":
            severity = node.text
        if node.attrib.get('xmlname') == "UrgencyCode":
            urgency = node.text
        if node.attrib.get('xmlname') == "AsysCategory":
            phase = node.text

    return [record_num, title, assignee_code, assignee_name, status,\
            severity, urgency, phase]


def process_not_in_groups(aproach_fields, redis_handler):
    cached_status = None
    record_num, title, assignee_code, assignee_name, status,\
    severity, urgency, phase = aproach_fields

    # record is being tracked, but assigned to another GROUP..
    for assignee in GROUPS:
        if is_record_tracked(assignee, record_num,redis_handler):
            # retrieve the cached Aproach status in redis
            cached_status = redis_handler.hget(assignee, record_num)
            # delete (or move to info) record from trends
            # stop tracking record...
            delete_redis_record (assignee,record_num,redis_handler)
            delete_trends_record (record_num, phase, cached_status, status)
            break
    # Record was not being tracked and not with GROUP, so can ignore
    if not cached_status:
        log.info("{} {} not our record, for group {}".format\
        (RECORD_TYPE, record_num, assignee_code))


def process_in_groups(aproach_fields, redis_handler):
    record_num, title, assignee_code, assignee_name, status,\
    severity, urgency, phase = aproach_fields

    closed_status = re.search(r'Closed|Solved', status, re.I)
    # record already tracked and assigned to GROUP
    if is_record_tracked(assignee_code, record_num,redis_handler):
        # PTR back to logger for closure.
        if closed_status:
            # delete (or move to info) record from trends
            # stop tracking record...
            # retrieve the cached Aproach status in redis
            cached_status = redis_handler.hget(assignee_code, record_num)
            delete_redis_record (assignee_code,record_num,redis_handler)
            delete_trends_record (record_num, phase, cached_status, status)
        else:
            if check_cache_timestamp(record_num, redis_handler):
                log.info("{} {} already being tracked".format\
                (RECORD_TYPE, record_num))
            else:
                update_cache_timestamp(record_num, redis_handler)
                if assignee_name:
                    track_record(aproach_fields, redis_handler)
    # record not being tracked (first time in GROUP Queue )...
    else:
        # track it...
        #if record is Closed or Solved no need to track
        if closed_status:
            log.info("{} {} already is in {} status no need to track".format\
            (RECORD_TYPE, record_num, status))
        else:
            if assignee_name:
                track_record(aproach_fields, redis_handler)

def process_PTR(xml_record, redis_handler):
    aproach_fields = extract_xml_data(xml_record)
    # record assigned to GROUP...
    # aproach_fields[2] = assignee_code
    if aproach_fields[2] in GROUPS:
        process_in_groups(aproach_fields, redis_handler)
    # record is not assigned to GROUP
    else:
        process_not_in_groups(aproach_fields, redis_handler)
