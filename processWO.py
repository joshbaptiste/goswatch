import re
import sys
import json
import urllib2
from datetime import datetime, timedelta
from logwatch import log
from dbwatch import trends
from config import GROUPS, MCA, MCR
import socket
#processWO.py

# Python based script watches redis queue for  Sev3 or lower IR/PTR and WO adds to GOS FU as soon as hits queue.
# Removes from GOS FU as soon as leaves queue or is closed...
# Changes it to Info section if WO is implemented
# Uses user goswatch in trends
RECORD_TYPE = "WO"


def set_cache_ttl(record_num, redis_handler, osr_type, ttl):
    try:
        log.info("%s %s setting %s osr cache" % (RECORD_TYPE, record_num, osr_type))
        if redis_handler.expire("%s_osr_cache" % osr_type, ttl):
            return
        else:
            redis_handler.set("%s_osr_cache" % osr_type, "True")
            redis_handler.expire("%s_osr_cache" % osr_type, ttl)
    except:
        log.error("%s %s could not set %s osr cache." % (RECORD_TYPE, record_num, osr_type))


def get_cache_ttl(record_num, osr_type, redis_handler):
    try:
        log.info("%s %s Checking TTL of %s osr cache" % (RECORD_TYPE, record_num, osr_type))
        return redis_handler.ttl("%s_osr_cache" % osr_type)
    except:
        log.error("%s %s Could not check TTL for %s osr cache." % (RECORD_TYPE, record_num, osr_type))


def cache_oneosr_schedule(record_num, assignee_code, osr_type, redis_handler):
    # connect to oneosr API and get raw json file
    try:
        oneosr_handler = urllib2.urlopen(DBOSR_URL, None, 20)
    except (urllib2.URLError):
        log.error("%s %s ONEOSR : ERROR - %s" % (RECORD_TYPE, record_num, str(sys.exc_info())))
        return None
    except (socket.timeout):
        log.error("%s %s ONEOSR : ERROR - %s" % (RECORD_TYPE, record_num, str(sys.exc_info())))
        return None
    # read json file
    oneosr_string = oneosr_handler.read()
    oneosr_json = json.loads(oneosr_string)
    # store only OSR info
    try:
        scheduled_osrs = oneosr_json['OSRRequest']
    except (LookupError):
        log.error("%s %s ONEOSR : ERROR - %s" % (RECORD_TYPE, record_num, str(sys.exc_info())))
        return None
    # add OSR info to redis
    try:
        log.info("%s %s %s Adding %s scheduled osrs to redis" % (RECORD_TYPE, record_num, assignee_code, osr_type))
        redis_handler.hset(assignee_code, osr_type, oneosr_string)
        return scheduled_osrs
    except:
        log.error("%s %s %s Error could not add  %s scheduled osrs to redis" % (RECORD_TYPE, record_num, assignee_code, osr_type))


def get_oneosr_schedule(record_num, assignee_code, osr_type, redis_handler):
    try:
        log.info("%s %s %s Retrieving %s scheduled osrs from redis" % (RECORD_TYPE, record_num, assignee_code, osr_type))
        oneosr_json = json.loads(redis_handler.hget(assignee_code, osr_type))
        return oneosr_json['OSRRequest']
    except:
        log.error("%s %s %s Error could not retrieve %s scheduled osrs from redis" % (RECORD_TYPE, record_num, assignee_code, osr_type))


def is_oneosr_scheduled(record_num, assignee_code, osr_type, scheduled_osrs):
    try:
        for osr in scheduled_osrs:
            if record_num == str(osr.get('winaproach_record')):
                return osr
        return None
    except (TypeError):
        log.error("%s %s %s Error cannot check if OSR is scheduled due to issue with null %s ONEOSR data - %s" % (RECORD_TYPE, record_num, assignee_code, osr_type, str(sys.exc_info())))


def is_record_tracked(assignee_code, record_num, redis_handler):
    try:
        if redis_handler.hexists(assignee_code, record_num):
            return record_num
        else:
            return False
    except:
        log.error("Error: could not query redis for %s %s." % (RECORD_TYPE, record_num))


def add_redis_record(assignee_code, record_num, status, redis_handler):
    try:
        log.info("%s %s Adding key value to redis" % (RECORD_TYPE, record_num))
        redis_handler.hset(assignee_code, record_num, status)
    except:
        log.error("Error: could not add %s %s to redis." % (RECORD_TYPE, record_num))


def delete_redis_record(assignee_code, record_num, redis_handler):
    try:
        log.info("%s %s Removing from redis" % (RECORD_TYPE, record_num))
        redis_handler.hdel(assignee_code, record_num)
    except:
        log.error("Error: could not delete %s %s from redis." % (RECORD_TYPE, record_num))


def is_record_in_trends(record_num):
    kwargs = {'action': 'check', 'record_num': record_num, 'rectype': RECORD_TYPE}
    if trends(**kwargs):
        log.info("%s %s Record found in trends." % (RECORD_TYPE, record_num))
        return True
    else:
        log.info("%s %s Record not found in trends." % (RECORD_TYPE, record_num))
        return False


def schedule_record(record_num, title, start_date, start_time, comment):
    kwargs = {'action': 'update_to_schedule', 'record_num': record_num,
    'title': title, 'start_date': start_date, 'start_time': start_time,
    'comment': comment, 'rectype': RECORD_TYPE}
    query_result = trends(**kwargs)
    # update record...(will fail if record not in DB)
    if not query_result:
        # if record does not exist insert it
        log.info("%s %s UPDATE affected %s rows. Executing INSERT" % (RECORD_TYPE, record_num, query_result))
        kwargs = {'action': 'schedule', 'record_num': record_num,
        'title': title, 'start_date': start_date, 'start_time': start_time,
        'comment': comment, 'rectype': RECORD_TYPE}
        return trends(**kwargs)
    else:
        return query_result


def follow_up_record(record_num, title, comment):
    kwargs = {'action': 'update_to_follow', 'record_num': record_num,
    'title': title, 'comment': comment, 'rectype': RECORD_TYPE}
    query_result = trends(**kwargs)
    # update record...(will fail if record not in DB)
    if not query_result:
        # if record does not exist insert it
        log.info("%s %s UPDATE affected %s rows. Executing INSERT" % (RECORD_TYPE, record_num, query_result))
        kwargs = {'action': 'follow-up', 'record_num': record_num,
        'title': title, 'comment': comment, 'rectype': RECORD_TYPE}
        return trends(**kwargs)
    else:
        return query_result

# still need to check phase, if PRD move to info, else delete.
# can probably use status
def delete_trends_record(record_num, phase, cached_status, status):
    # if PRD WO and status has changed...
    if cached_status != status:
        # move to info section
        kwargs = {'action': 'moveto', 'record_num': record_num, 'section': 4, 'status': status, 'rectype': RECORD_TYPE}
    else:
        # otherwise move to trash
        kwargs = {'action': 'moveto', 'record_num': record_num, 'section': 0, 'status': '', 'rectype': RECORD_TYPE}
    return trends(**kwargs)


def extract_xml_data(xml_record):
    category, title, assignee_code, phase, status, start_date, start_time, severity = \
        "", "", "", "", "", "", "", ""
    record_num = xml_record.attrib['id']
    for node in xml_record.iter('field'):
        if node.attrib.get('xmlname') == 'Title':
            title = node.text.lstrip()
        if node.attrib.get('xmlname') == "AssigneeGroup":
            assignee_code = node.attrib.get('code')
        if node.attrib.get('xmlname') == "Status":
            status = node.text
        if node.attrib.get('xmlname') == "Severity":
            severity = node.text
        if node.attrib.get('xmlname') == "System":
            phase = node.text
        if node.attrib.get('xmlname') == "WhishDate":
            start_date = node.text
        if node.attrib.get('xmlname') == "WhishTime":
            start_time = node.text
        if node.attrib.get('xmlname') == "WorkorderType":
            category = node.text
    if not category:
        category = 'Normal'

    return [record_num, title, assignee_code, status, severity, phase, start_date,
            start_time, category]


# 0 = Monday, 1 = Tuesday, 2 = Wednesday, ... , Sunday = 6
def getdate_next_weekday(day_of_week):
    today = datetime.now()
    days_ahead = day_of_week - today.weekday()
    # if day of week has already past, increment days_ahead by 7 days
    # for example, if today is Wednesday, then today.weekday() = 2
    # if day_of_week is 0 (Monday), then days_ahead = 0 - 2 = -2
    # In this case -2 means that Monday was 2 days prior to today, and
    # that the next Monday would be 5 days from today. if we process
    # days_ahead += 7 we do get 5. If the today is the day of the week
    # that was requsted, then today is returned.
    if days_ahead < 0:
        days_ahead += 7
    return (today + timedelta(days_ahead)).strftime("%d%b%y")


def process_not_in_groups(aproach_fields, redis_handler):
    cached_status = None
    record_num, title, assignee_code, status, severity, phase, start_date, \
        start_time, category = aproach_fields
    # record is being tracked, but assigned to another GROUP..
    for assignee in GROUPS:
        if is_record_tracked(assignee, record_num, redis_handler):
            # retrieve the cached Aproach status in redis
            cached_status = redis_handler.hget(assignee, record_num)
            # delete (or move to info) record from trends
            # stop tracking record...
            delete_redis_record(assignee, record_num, redis_handler)
            delete_trends_record(record_num, phase, cached_status, status)
            break
    # Record was not being tracked and not with GROUP, so can ignore
    if not cached_status:
        log.info("%s %s not our record, for group %s" % (RECORD_TYPE, record_num, assignee_code))


def process_in_groups(aproach_fields, redis_handler):
    first_time = False
    record_num, title, assignee_code, status, severity, phase, start_date, \
        start_time, category = aproach_fields
    dbosr = re.compile('.*DB[A-Z].OSR.*')
    siosr = re.compile('.*SIOSR.*')
    # record already tracked and assigned to GROUP...
    if is_record_tracked(assignee_code, record_num, redis_handler):
        closed = re.compile('^closed.*', re.IGNORECASE)
        # ... and status is closed
        # to avoid lingering WO's that were orginally logged by group in GROUPS
        if closed.match(status):
            cached_status = redis_handler.hget(assignee_code, record_num)
            delete_redis_record(assignee_code, record_num, redis_handler)
            delete_trends_record(record_num, phase, cached_status, status)
            return None
        #...nothing to do
        else:
            log.info("%s %s already being tracked and assigned to group" % (RECORD_TYPE, record_num))
    # record not being tracked (first time in GROUP Queue )...
    else:
        # track it...
        add_redis_record(assignee_code, record_num, status, redis_handler)
        first_time = True

    # WO is a DB OSR
    if dbosr.match(title):
        process_DBOSR(aproach_fields, redis_handler)
    # WO is a SI OSR
    elif siosr.match(title):
        process_SIOSR(aproach_fields)
    # Regular WO that isnt already tracked to prvent duplicates
    else:
        if first_time:
            comment = category
            follow_up_record(record_num, title, comment)
            first_time = False


def create_dbosr_trends_comment(category, phase, app):
    today = datetime.now()
    if app in ['ETK', 'CDB']:
        comment = category + ', ' + phase + ' ETK DB OSR. Suggested window: ' \
                + getdate_next_weekday(0) + '/' + getdate_next_weekday(3) + ' @ 18:00GMT'
    elif app == 'APL':
        comment = category + ', ' + phase + ' APL Weekly DB OSR. Suggested window: ' \
                + getdate_next_weekday(0) + ' @ 15:30GMT'
    elif app == 'MDS':
        comment = category + ', ' + phase + ' MDS DB OSR. Suggested window: ' \
                + getdate_next_weekday(3) + ' @ 1:30GMT'
    elif app in MCA:
        comment = category + ', ' + phase + ' MCA DB OSR. Suggested window: ' \
                + today.strftime("%d%b%y") + ' @ 15:30GMT'
    elif app in MCR:
        comment = category + ', ' + phase + ' MCR DB OSR. Suggested window: ' \
                + today.strftime("%d%b%y") + ' @ 18:00GMT'
    else:
        comment = category + ', ' + phase + ' Anytime DB OSR. Suggested window: ' \
                + today.strftime("%d%b%y") + ' @ 20:00GMT'
    return comment


def get_scheduling_info(window, special_date=""):
    today = datetime.now()
    date = today.date().strftime("%d%b%y")
    # check if OSR is specially scheduled.
    if window == 'OTHER':
        # only split on special_date if not NULL/NoneType
        if special_date:
            _date,time = special_date.split()
            date = datetime.strptime(_date, "%Y-%m-%d") # create an date object from the _date string
            date = date.strftime("%d%b%y") # create a date string from the date object with the format %d%b%y, i.e. 31Aug14
        else:
            date, time = None, None # special_date, which is equal to value of time_range_lo key in json object, set to null
        return (date, time)
    elif window == 'MCA weekly':
        return [date, '15:00GMT']
    elif window == 'APL weekly':
        return [getdate_next_weekday(0), '15:00:00']
    elif window == 'ETK-TSR weekly':
        return [getdate_next_weekday(0), '18:00:00']
    elif window == 'ETK-ETS weekly':
        return [getdate_next_weekday(3), '18:00:00']
    elif window == 'PPP weekly':
        return [date, '18:00:00']
    elif window == 'SBR weekly':
        return [date, '18:00:00']
    elif window == 'MDS weekly':
        return [getdate_next_weekday(3), '01:00:00']
    # Anytime
    else:
        return [date, '20:00:00']


def add_to_sched_actions(schedule_info):
    record_num = schedule_info.get('winaproach_record')
    title = schedule_info.get('name')
    #comment = load_info.get('comment') + ". " +\
    #               schedule_info.get('load_instructions')
    #disabled above due to exceptions if load_instructions=null
    load_info = schedule_info.get('loadwindow')
    comment = load_info.get('comment')
    #start_date, start_time = get_scheduling_info(load_info.get('type'), load_info.get('time_range_lo'))
    window_category = load_info.get('type')
    window_datetime = load_info.get('time_range_lo') # expected json time_range_lo format: "yyyy-mm-dd HH:MM:SS" or NULL
    start_date, start_time = get_scheduling_info(window_category, window_datetime);
    if start_date: # if start_date is specified then schedule
            return schedule_record(record_num, title, start_date, start_time, comment)
    # otherwise add to follow-up with updated comment.
    comment += ". Special Schedule with no date/time specified."
    return  follow_up_record(record_num, title, comment)
    #return schedule_record(record_num, title, start_date, start_time, comment)


def schedule_osr(aproach_fields, redis_handler):
    record_num, title, assignee_code, status, severity, phase, start_date, \
    start_time, category = aproach_fields
    # check if oneosr cache is set, and active
    if get_cache_ttl(record_num, 'db', redis_handler):
        oneosr_schedule = get_oneosr_schedule(record_num, assignee_code, 'db', redis_handler)
        # osr is scheduled via oneosr
        schedule_info = is_oneosr_scheduled(record_num, assignee_code, 'db', oneosr_schedule)
        if schedule_info:
            # ADD TO SCHEDULED ACTIONS
            return add_to_sched_actions(schedule_info)
        # update the oneosr schedule cache and recheck for record
        else:
            # retrieve and cache oneosr schedule
            oneosr_schedule = cache_oneosr_schedule(record_num, assignee_code, 'db', redis_handler)
            # set oneosr cache time-to-live
            set_cache_ttl(record_num, redis_handler, 'db', 5)
            # osr is scheduled via oneosr
            schedule_info = is_oneosr_scheduled(record_num, assignee_code, 'db', oneosr_schedule)
            if schedule_info:
                # ADD TO SCHEDULED ACTIONS
                return add_to_sched_actions(schedule_info)
    # osr_schedule not cached or cold
    else:
        # retrieve and cache oneosr schedule
        oneosr_schedule = cache_oneosr_schedule(record_num, assignee_code, 'db', redis_handler)
        # set oneosr cache time-to-live
        set_cache_ttl(record_num, redis_handler, 'db', 5)
        # osr is scheduled via oneosr
        schedule_info = is_oneosr_scheduled(record_num, assignee_code, 'db', oneosr_schedule)
        if schedule_info:
            # ADD TO SCHEDULED ACTIONS
            return add_to_sched_actions(schedule_info)
    return None


def process_DBOSR(aproach_fields, redis_handler):
    record_num, title, assignee_code, status, severity, phase, start_date,\
    start_time, category = aproach_fields
    prdskl = re.compile('(^PRD|^SKL).*')
    log.info("%s %s Attempting to process DB OSR" % (RECORD_TYPE, record_num))
    # SKL or PRD OSRs to trends
    if prdskl.match(title):
        # grab portion of title that has app, i.e. ETK-DBX-OSR
        result = re.search(r'[A-Z]{3}-[A-Z]{3}-[A-Z]{3}', title)
        # could not parse title..
        if not result:
            log.info("%s %s Malformed DBOSR title '%s' adding to follow_up" % (RECORD_TYPE, record_num, title))
            follow_up_record(record_num, title, 'Please Follow up WO')
            return
        # schedule osr based on ONEOSR
        if schedule_osr(aproach_fields, redis_handler):
            return
        # OSR is not scheduled via ONEOSR, so add to follow-up
        else:
            # grab only app
            app = result.group().split('-')[0]
            comment = create_dbosr_trends_comment(category, title[:3], app)
            # and add it to trends
            follow_up_record(record_num, title, comment)
    # Not a PRD/SKL OSR
    else:
        # grab portion of title that has app, i.e. ETK-DBX-OSR
        result = re.search(r'[A-Z]{3}-[A-Z]{3}-[A-Z]{3}', title)
        # could not parse title..
        if not result:
            log.info("{} {} Malformed DBOSR title '{}' adding to follow_up"\
                     .format(RECORD_TYPE, record_num, title))
            follow_up_record(record_num, title, 'Please Follow up WO')
            return
        # and add it to trends
        follow_up_record(record_num, title, 'TST DBOSR, please follow up')
        #log.info("%s %s Not an PRD/SKL record. Processing halted." % (RECORD_TYPE, record_num))


def process_SIOSR(aproach_fields):
    record_num, title, assignee_code, status, severity, phase, start_date, \
    start_time, category = aproach_fields
    prd = re.compile('.*-PRD.*')
    pdt = re.compile('.*-PDT.*')
    today = datetime.now()
    log.info("%s %s Attempting to process SI OSR" % (RECORD_TYPE, record_num))
    # PRD to follow-up
    if prd.match(title):
        if not is_record_in_trends(record_num):
            # grab portion of title that has app, i.e. REV-SIOSR-PRD
            result = re.search(r'[A-Z]{3}-SIOSR-[A-Z]{3}', title)
            #If for some reason
            if not result:
                log.info("%s %s Malformed SIOSR title '%s' adding to follow_up" % (RECORD_TYPE, record_num, title))
                follow_up_record(record_num, title, 'Please Follow up WO')
                return
            # grab only app
            app = result.group().split('-')[0]
            phase = result.group().split('-')[2]
            if app in MCA:
                comment = category + ', ' + phase + ' MCA SI OSR. Suggested window: ' \
                        + today.strftime("%d%b%y") + ' @ 15:30GMT'
            elif app in MCR:
                comment = category + ', ' + phase + ' MCR SI OSR. Suggested window: ' \
                        + today.strftime("%d%b%y") + ' @ 18:00GMT'
            else:
                comment = category + ', ' + phase + ' Anytime SI OSR. Suggested window: ' \
                        + today.strftime("%d%b%y") + ' @ 20:00GMT'
            # and add it to trends
            follow_up_record(record_num, title, comment)
    # TST to scheduled actions for SYD
    elif pdt.match(title):
        if not is_record_in_trends(record_num):
            # grab portion of title that has app, i.e. REV-SIOSR-PRD
            result = re.search(r'[A-Z]{3}-SIOSR-[A-Z]{3}', title)
            #If for some reason
            if not result:
                log.info("%s %s Malformed SIOSR title '%s' adding to follow_up" % (RECORD_TYPE, record_num, title))
                follow_up_record(record_num, title, 'Please Follow up WO')
                return
            # grab only app
            app = result.group().split('-')[0]
            phase = result.group().split('-')[2]
            comment = category + ', ' + phase + ' SI OSR'
            # if OSR is submitted on Sat,Sun schedule for Sun SYD time (Monday)
            if today.weekday() in range(5, 6):
                # Monday is 0 and Sunday is 6
                start_date = getdate_next_weekday(0)
                schedule_record(record_num, title, start_date, '00:00:00', comment)
            # otherwise schedule for today
            else:
                start_date = today.strftime("%d%b%y")
                schedule_record(record_num, title, start_date, today.strftime('%H:%M:%S'), comment)
    else:
        log.info("%s %s Not an PRD/PDT record. Processing halted." % (RECORD_TYPE, record_num))


def process_WO(xml_record, redis_handler):
    aproach_fields = extract_xml_data(xml_record)
    # record assigned to GROUP...
    # aproach_fields[2] = assignee_code
    if aproach_fields[2] in GROUPS:
        process_in_groups(aproach_fields, redis_handler)
    # record is not assigned to GROUP
    else:
        process_not_in_groups(aproach_fields, redis_handler)
