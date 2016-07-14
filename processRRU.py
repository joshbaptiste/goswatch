import re
import xml.etree.ElementTree as ET
from logwatch import log
from dbwatch import trends
from config import GROUPS
#processRRU.py

# Python based script watches redis queue for  Sev3 or lower IR/RRU and RRU adds to GOS FU as soon as hits queue.
# Removes from GOS FU as soon as leaves queue or is closed...
# Changes it to Info section if RRU is implemented
# Uses user goswatch in trends
record_type="RRU"

def is_record_tracked(assignee_code, record_num, redis_handler):
    try:
        if redis_handler.hexists(assignee_code, record_num):
            return record_num
        else:
            return False
    except:
        log.error("Error: could not query redis for %s %s." % (record_type, record_num))

def add_redis_record(assignee_code, record_num, status, redis_handler):
    try:
        log.info("%s %s Adding key value to redis" % (record_type, record_num))
        redis_handler.hset(assignee_code, record_num, status)
    except:
        log.error("Error: could not add %s %s to redis." % (record_type, record_num))

def delete_redis_record(assignee_code, record_num, redis_handler):
    try:
        log.info("%s %s Removing from redis" % (record_type, record_num))
        redis_handler.hdel(assignee_code, record_num)
    except:
        log.error("Error: could not delete %s %s from redis." % (record_type, record_num))

def follow_up_record(record_num, title, app, description, comment):
    kwargs={'action': 'update_to_follow','record_num': record_num, \
    'title': title, 'app': app, 'description':description, 'comment': comment, 'rectype': record_type}
    query_result = trends(**kwargs)
    # update record...(will fail if record not in DB)
    if not query_result:
        # if record does not exist insert it
        log.info("%s %s UPDATE affected %s rows. Executing INSERT" % (record_type, record_num, query_result))
        kwargs={'action': 'follow-up','record_num': record_num, \
        'title': title, 'app': app, 'description':description, 'comment': comment, 'rectype': record_type}
        trends(**kwargs)

def schedule_record(record_num, title, start_date, start_time, comment):
    kwargs = {'action': 'update_to_schedule', 'record_num': record_num,
              'title': title, 'planned_date': planned_date,
              'planned_time': planned_time, 'comment': comment,
              'rectype': record_type}

    query_result = trends(**kwargs)
    # update record...(will fail if record not in DB)
    if not query_result:
        # if record does not exist insert it
        log.info("%s %s UPDATE affected %s rows. Executing INSERT" %
                 (record_type, record_num, query_result))
        kwargs = {'action': 'schedule', 'record_num': record_num,
                  'title': title, 'start_date': start_date,
                  'start_time': start_time, 'comment': comment,
                  'rectype': record_type}
        trends(**kwargs)


def delete_trends_record (record_num, status):
    # All RRUs should go to info section
    kwargs={'action': 'moveto', 'record_num': record_num, 'section': 4, 'status': status, 'rectype': record_type}
    trends(**kwargs)

def extract_xml_data(xml_record):
    title, assignee_code, status, target, targetsystems, priority, planned_date, planned_time, risk, qatteststatus, frtteststatus, pptteststatus, \
    application = "", "", "", "", "", "", "", "", "", "", "", "",""
    record_num = xml_record.attrib['id']
    for node in xml_record.iter('field'):
        if node.attrib.get('xmlname') == 'Title':
            title =  node.text.lstrip()
        if node.attrib.get('xmlname') == "AssigneeGroup":
            assignee_code = node.attrib.get('code')
        if node.attrib.get('xmlname') == "Status":
            status = node.text
        if node.attrib.get('xmlname') == "Target":
            target = node.text
        if node.attrib.get('xmlname') == "TargetSystems":
            targetsystems = node.text
        if node.attrib.get('xmlname') == "Priority":
            priority = node.text
        if node.attrib.get('xmlname') == "PlannedDate":
            planned_date = node.text
        if node.attrib.get('xmlname') == "PlannedTime":
            planned_time = node.text
        if node.attrib.get('xmlname') == "Risk":
            risk = node.text
        if node.attrib.get('xmlname') == "QATTestStatus":
            qatteststatus = node.text
        if node.attrib.get('xmlname') == "FRTTestStatus":
            frtteststatus = node.text
        if node.attrib.get('xmlname') == "PPTTestStatus":
            pptteststatus = node.text
        if node.attrib.get('xmlname') == "Application":
            application = node.text

    if target == "Production Systems":
        target = "PRD"
    elif target == "Test Systems":
        target = "TST"

    if qatteststatus == "Not Applicable":
        qatteststatus = "N/A"
    elif qatteststatus == "Passed":
        qatteststatus = "OK"

    if frtteststatus == "Not Applicable":
        frtteststatus = "N/A"
    elif frtteststatus == "Passed":
        frtteststatus = "OK"

    if pptteststatus == "Not Applicable":
        pptteststatus = "N/A"
    elif pptteststatus == "Passed":
        pptteststatus = "OK"

    if priority != "Pri/A":
        priority = ""

    return [record_num, title, assignee_code, status, target, targetsystems, \
            priority, planned_date, planned_time, risk, qatteststatus, frtteststatus, pptteststatus, application]

def process_not_in_groups(aproach_fields, redis_handler):
    cached_status = None
    record_num, title, assignee_code, status, target, targetsystems, priority, planned_date, \
    planned_time, risk, qatteststatus, frtteststatus, pptteststatus = aproach_fields
    # record is being tracked, but assigned to another GROUP..
    for assignee in GROUPS:
        if is_record_tracked(assignee, record_num,redis_handler):
            # retrieve the cached Aproach status in redis
            cached_status = redis_handler.hget(assignee, record_num)
            # delete (or move to info) record from trends
            # stop tracking record...
            delete_redis_record (assignee,record_num,redis_handler)
            delete_trends_record (record_num, status)
            break
    # Record was not being tracked and not with GROUP, so can ignore
    if not cached_status:
        log.info("%s %s not our record, for group %s" % (record_type, record_num, assignee_code))

def process_in_groups(aproach_fields, redis_handler):
    record_num, title, assignee_code, status, target, targetsystems, priority, planned_date, \
    planned_time, risk, qatteststatus, frtteststatus, pptteststatus, application = aproach_fields
    # record already tracked and assigned to GROUP
    if is_record_tracked(assignee_code, record_num,redis_handler):
        # retrieve the cached Aproach status in redis
        cached_status = redis_handler.hget(assignee_code, record_num)
        # RRU back to logger for closure.
        if cached_status != status:
            # delete (or move to info) record from trends
            # stop tracking record...
            delete_redis_record (assignee_code,record_num,redis_handler)
            delete_trends_record (record_num, phase, cached_status, status)
        else:
            log.info("%s %s already being tracked" % (record_type, record_num))
    # record not being tracked (first time in GROUP Queue )...
    else:
        # track it...
        closed_status = re.search(r'Closed', status, re.I)
        #if record is Closed or Solved no need to track
        if closed_status:
            log.info("%s %s already is in %s status no need to track" % (record_type, record_num, status))
        else:
            add_redis_record(assignee_code, record_num, status, redis_handler)
            if target == "PRD":
                comment = "%s RRU %s [%s] \n Sign-Offs: QAT: %s FRT: %s PPT: %s" % (target, priority, status, qatteststatus, frtteststatus, pptteststatus)
            else:
                comment = "%s RRU %s [%s]" % (target, priority, status)
            appacronym = re.search(r"[A-Z]{3,}", application)
            appdescription = re.match(r"[^\(]+", application)
            app = appacronym.group()
            description = appdescription.group()
            follow_up_record(record_num, title, app, description, comment)


def process_RRU(xml_record, redis_handler):
    #print ET.tostring(xml_record)
    #return
    aproach_fields = extract_xml_data(xml_record)
    print aproach_fields
    # record assigned to GROUP...
    # aproach_fields[2] = assignee_code
    if aproach_fields[2] in GROUPS:
        process_in_groups(aproach_fields, redis_handler)
    # record is not assigned to GROUP
    else:
        process_not_in_groups(aproach_fields, redis_handler)
