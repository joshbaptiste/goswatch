import sys
import datetime
import mysql.connector
from mysql.connector.constants import ClientFlag
import config
from logwatch import log
import time, os
import random

class DBConnection:
    _dbconn = None

    @staticmethod
    def get_instance():
        if not DBConnection._dbconn:
            DBConnection._dbconn = DBConnection()
        return DBConnection._dbconn

    def __init__(self):
        self.connection = None

    def connect(self):
        if not self.connection:
            self.connection = mysql.connector.connect(**config.CONFIG)

    def get_cursor(self):
        retries = 2
        while retries > 0:
            try:
                self.connect()
                cursor = self.connection.cursor(buffered=True)
                return cursor
            except mysql.connector.errors.InterfaceError, iErr:
                log.error("%s: Connection failed. Retrying. " % iErr)
                self.connection = None
                retries -= 1
                if retries == 0:
                    raise

    def execute(self, query, params=None):
        cursor = self.get_cursor()
        cursor.execute(query, params)
        #dbdebugoutput = cursor.fetchwarnings()
        #log.error("DEBUG LOG: %s" % dbdebugoutput)
        return cursor.rowcount

def get_site_flag(current_hour):

    os.environ["TZ"]="Europe/Berlin"
    isERDdst = time.localtime().tm_isdst

    if (isERDdst == 0):
        MIAMI_SHIFT = range(15,23)
        ERDING_SHIFT = range(7,15)
        SYDNEY_SHIFT = range(23,24) + range(0,7)
    else:
        MIAMI_SHIFT = range(14,22)
        ERDING_SHIFT = range(6,14)
        SYDNEY_SHIFT = range(22,24) + range(0,6)

    del os.environ["TZ"]

    try:
        current_hour = int(current_hour)
    except ValueError:
        current_hour = -1

    if current_hour in MIAMI_SHIFT:
        return 'M'
    elif current_hour in ERDING_SHIFT:
        return 'E'
    elif current_hour in SYDNEY_SHIFT:
        return 'S'
    else:
        return ''


def trends(**kwargs):
    dml = None
    values = None
    rows = None
    # only process trends during Miami Shift / SYD approved as of Nov30, to test
    # if get_site_flag(datetime.datetime.now().hour) != 'M':
    if get_site_flag(datetime.datetime.now().hour) == 'E':
    #   log.info("%s %s Trends action '%s' disabled. Currently operating outside of Miami hours" \
        log.info("%s %s Trends action '%s' disabled. Currently operating in Erding hours" \
        % (kwargs['rectype'], kwargs['record_num'], kwargs['action']))
        return None

    if kwargs['action'] == "check":
        dml = "SELECT `log_type` FROM `register` WHERE `record_num` = \
        %s AND `log_type` IN ('1', '3')" % kwargs['record_num']
        log.info("%s %s Checking if record exists in F/U or SA of trends: %s" % (kwargs['rectype'], kwargs['record_num'], dml))

    if kwargs['action'] == "moveto":
        # Section 1:f/u 2:problem 3:s/a 4:info 0:trash
        sections = ['Trash','Follow up','Problem','Scheduled Actions','Information']
        # if moving to trash no need to update comment
        if kwargs['section'] == 0:
            dml = "UPDATE `register` SET `log_type` = %s WHERE `record_num` = %s"
            values = (kwargs['section'], kwargs['record_num'])
        else:
            dml = "UPDATE `register` SET `log_type` = %s, `comments` = %s WHERE `record_num` = %s"
            values = (kwargs['section'], kwargs['status'], kwargs['record_num'])
        log.info("%s %s Moving to section '%s' %d: %s" %\
        (kwargs['rectype'], kwargs['record_num'], sections[kwargs['section']], kwargs['section'], (dml % values) ))

    if kwargs['action'] == "schedule":
        yyyymmdd = datetime.datetime.strptime(kwargs['start_date'], '%d%b%y').strftime('%Y-%m-%d')
        start_hour = kwargs['start_time'][0:2]
        dml = "INSERT INTO `register` (`record_num`,`title`,`sch_date`, \
        `sch_time`,`rectype`,`log_type`,`user_id`,`entry`,`site`,`comments`,`category`) VALUES \
        (%s, %s, %s, %s, %s, 3, 'GOSWATCH', 2, %s, %s, 63)"
        values = (kwargs['record_num'], kwargs['title'], yyyymmdd, kwargs['start_time'], kwargs['rectype'], get_site_flag(start_hour), kwargs['comment'])
        log.info("%s %s Adding to scheduled actions up for review: %s" % (kwargs['rectype'], kwargs['record_num'], (dml % values) ))

    if kwargs['action'] == "update_to_schedule":
        yyyymmdd = datetime.datetime.strptime(kwargs['start_date'], '%d%b%y').strftime('%Y-%m-%d')
        start_hour = kwargs['start_time'][0:2]
        dml = "UPDATE `register` SET `title` = %s,`sch_date` = %s, \
        `sch_time` = %s,`rectype` = %s,`log_type` = '3',`user_id` = 'GOSWATCH', \
        `entry` = '2',`site` = %s WHERE `record_num` = %s LIMIT 1"
        values = (kwargs['title'], yyyymmdd, kwargs['start_time'], kwargs['rectype'], get_site_flag(start_hour), kwargs['record_num'])
        log.info("%s %s Updating to scheduled actions for review: %s" % (kwargs['rectype'], kwargs['record_num'], (dml % values) ))

    if kwargs['action'] == "update_to_follow":
        dml = "UPDATE `register` SET `title` = %s,`rectype` = %s, \
        `log_type` = '1',`user_id` = 'GOSWATCH', `entry` = '2', \
        `site` = 'M' WHERE `record_num` = %s LIMIT 1"
        #Note the site is hardcoded above, need to check this...
        values = (kwargs['title'], kwargs['rectype'], kwargs['record_num'])
        log.info("%s %s Updating to follow up for review: %s" % (kwargs['rectype'], kwargs['record_num'], (dml % values) ))

    if kwargs['action'] == "follow-up":
        dml = "INSERT INTO `register` (`record_num`,`title`,`sch_date`, \
        `rectype`,`log_type`,`user_id`,`entry`,`site`,`comments`,`category`) VALUES \
        (%s, %s, NOW(), %s, 1, 'GOSWATCH', 2, 'M',%s, 63)"
        #Note the site is hardcoded above, need to check this...
        values = (kwargs['record_num'], kwargs['title'], kwargs['rectype'], kwargs['comment'] + ', Please follow-up')
        log.info("%s %s Adding to follow up for review: %s" % (kwargs['rectype'], kwargs['record_num'], (dml % values) ))

    try:
        if config.TEST_MODE:
            print 'DB in TEST mode will connect but all updates to stdout only!'
        else:
            sleepytime = random.uniform(1,5)
            time.sleep(sleepytime)
            log.info("%s %s Sleeping for %s seconds to avoid concurrent updates which cause freezes" % (kwargs['rectype'], kwargs['record_num'], sleepytime))
            connection = DBConnection.get_instance()
            log.info("%s %s Connected and attempting EXECUTE" % (kwargs['rectype'], kwargs['record_num']))
            rows = connection.execute(dml, values)
            log.info("%s %s EXECUTE affected %s rows." % (kwargs['rectype'], kwargs['record_num'], rows))
    except mysql.connector.Error as err:
        log.error("Error: could not execute %s on trends db for %s %s.,%s" % ((dml % values), kwargs['rectype'], kwargs['record_num'], err))
        log.info("%s %s EXECUTE affected %s rows - Checking for rows affected after exception" % (kwargs['rectype'], kwargs['record_num'], rows))
    return rows

