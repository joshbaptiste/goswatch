import re
from logwatch import log
from dbwatch import trends
from config import GROUPS
#processTR.py

# Python based script watches redis queue for  Sev3 or lower IR/PTR and TR adds
# to GOS FU as soon as hits queue.  Removes from GOS FU as soon as leaves queue
# or is closed... Changes it to Info section if TR is implemented
# Uses user goswatch in trends

record_type = "TR"


def is_record_tracked(assignee_code, record_num, redis_handler):
    try:
        if redis_handler.hexists(assignee_code, record_num):
            log.info("%s %s Found key value in redis" % (record_type, record_num))
            return record_num
        else:
            log.info("%s %s Did not find key value in redis" % (record_type, record_num))
            return False
    except:
        log.error("Error: could not query redis for %s %s." %
                  (record_type, record_num))


def add_redis_record(assignee_code, record_num, status, redis_handler):
    try:
        log.info("%s %s Adding key value to redis" % (record_type, record_num))
        redis_handler.hset(assignee_code, record_num, status)
    except:
        log.error("Error: could not add %s %s to redis." %
                  (record_type, record_num))


def delete_redis_record(assignee_code, record_num, redis_handler):
    try:
        log.info("%s %s Removing from redis" % (record_type, record_num))
        redis_handler.hdel(assignee_code, record_num)
    except:
        log.error("Error: could not delete %s %s from redis." %
                  (record_type, record_num))


def schedule_record(record_num, title, start_date, start_time, comment):
    kwargs = {'action': 'update_to_schedule', 'record_num': record_num,
              'title': title, 'start_date': start_date,
              'start_time': start_time, 'comment': comment,
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


def follow_up_record(record_num, title, comment):
    kwargs = {'action': 'update_to_follow', 'record_num': record_num,
              'title': title, 'comment': comment, 'rectype': record_type}

    query_result = trends(**kwargs)
    # update record...(will fail if record not in DB)
    if not query_result:
        # if record does not exist insert it
        log.info("%s %s UPDATE affected %s rows. Executing INSERT" %
                 (record_type, record_num, query_result))
        kwargs = {'action': 'follow-up', 'record_num': record_num,
                  'title': title, 'comment': comment, 'rectype': record_type}
        trends(**kwargs)


# still need to check phase, if PRD move to info, else delete.
# can probably use status
def delete_trends_record(record_num, phase, cached_status, status):
    # if PRD TR and status has changed...
    if cached_status != status:
        # move to info section
        kwargs = {'action': 'moveto', 'record_num': record_num, 'section': 4,
                  'status': status, 'rectype': record_type}
    else:
        # otherwise move to trash
        kwargs = {'action': 'moveto', 'record_num': record_num, 'section': 0,
                  'status': '', 'rectype': record_type}
    trends(**kwargs)


def extract_xml_data(xml_record):
    category, title, assignee_code, logger_code, phase, status, start_date,\
        start_time = "", "", "", "", "", "", "", ""
    record_num = xml_record.attrib['id']
    for node in xml_record.iter('field'):
        if node.attrib.get('xmlname') == "AssigneeGroup":
            assignee_code = node.attrib.get('code')
        if node.attrib.get('xmlname') == "LoggerGroup":
            logger_code = node.attrib.get('code')
        if node.attrib.get('xmlname') == 'Title':
            title = node.text
        if node.attrib.get('xmlname') == "SystemCategory":
            phase = node.text
        if node.attrib.get('xmlname') == "Status":
            status = node.text
        if node.attrib.get('xmlname') == "StartDate":
            start_date = node.text
        if node.attrib.get('xmlname') == "StartTime":
            start_time = node.text
        if node.attrib.get('xmlname') == "StandardChange":
            category = node.text
    if not category:
        category = 'Not Categorized'

    return [category, title, assignee_code, phase, status, start_date,
            start_time, record_num, logger_code]


def process_not_in_groups(aproach_fields, redis_handler):
    cached_status = None
    category, title, assignee_code, phase, status, start_date, start_time,\
        record_num, logger_code = aproach_fields

    # record is being tracked, but assigned to another GROUP..
    for assignee in GROUPS:
        if is_record_tracked(assignee, record_num, redis_handler):
            if status == 'Pending CMG Scheduling' or status == 'Approved by ECAB':
                log.info("%s %s Do not touch, non-standard that has been elevated by a greater force" % (record_type, record_num))
                break
            # retrieve the cached Aproach status in redis
            cached_status = redis_handler.hget(assignee, record_num)
            # delete(or move to info) record from trends
            # stop tracking record...
            delete_redis_record(assignee, record_num, redis_handler)
            delete_trends_record(record_num, phase, cached_status, status)
            break

    # Record was not being tracked and not with GROUP, so can ignore
    if not cached_status:
        log.info("%s %s not our record, for group %s" %
                 (record_type, record_num, assignee_code))


def process_in_groups(aproach_fields, redis_handler):
    category, title, assignee_code, phase, status, start_date, start_time,\
        record_num, logger_code = aproach_fields

    # record not being tracked (first time in GROUP Queue )...
    if not is_record_tracked(assignee_code, record_num, redis_handler):
        # if record is closed already no need to track
        closed_status = re.search(r'Closed', status, re.I)
        if closed_status and logger_code in GROUPS:
            log.info("%s %s already is in '%s' status no need to Track" %
                     (record_type, record_num, status))
            return
        # track it...
        add_redis_record(assignee_code, record_num, status, redis_handler)
        # and add it to trends
        # only PRD changes are added to SA
        if status == 'Scheduled' and phase == 'Production':
            schedule_record(record_num, title, start_date, start_time, category)
        else:
            follow_up_record(record_num, title, category)
    # record already tracked and assigned to GROUP, so nothing to do...
    else:
        # TR 7830041 ex.. closed automatically still assigned to GROUP
        closed_status = re.search(r'Closed|Solved', status, re.I)
        #if record is Closed or Solved no need to track
        if closed_status:
            log.info("%s %s is in '%s' status, Removing from Trends and Cache"
                     % (record_type, record_num, status))
            cached_status = redis_handler.hget(assignee_code, record_num)
            delete_redis_record(assignee_code, record_num, redis_handler)
            delete_trends_record(record_num, phase, cached_status, status)
        else:
            log.info("%s %s already being tracked and assigned to group" % (record_type, record_num))


def process_TR(xml_record, redis_handler):
    aproach_fields = extract_xml_data(xml_record)
    # record assigned to GROUP...
    # aproach_fields[2] = assignee_code
    if aproach_fields[2] in GROUPS:
        process_in_groups(aproach_fields, redis_handler)
    # record is not assigned to GROUP
    else:
        process_not_in_groups(aproach_fields, redis_handler)
