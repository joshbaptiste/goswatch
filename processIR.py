from logwatch import log
from config import GROUPS
#processIR.py
# Script to log IR's with new status, and assignee code in GROUPS.
RECORD_TYPE = "IR"


def extract_xml_data(xml_record):
    title, assignee_code, status, severity =\
        "", "", "", ""
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
        #This results in more than 1 value being returned for some reason
        #if node.attrib.get('xmlname') == "UrgencyCode":
        #    urgency = node.text.lstrip()
    return [record_num, title, assignee_code, status, severity]


def process_in_groups(aproach_fields, redis_handler):
    record_num, title, assignee_code, status, severity =\
        aproach_fields
    log.info("{} {} {} {} {}".format(RECORD_TYPE, record_num, title,
             severity, assignee_code))


def process_IR(xml_record, redis_handler):
    aproach_fields = extract_xml_data(xml_record)
    # record assigned to GROUP...
    # aproach_fields[2] = assignee_code
    if aproach_fields[2] in GROUPS:
        process_in_groups(aproach_fields, redis_handler)
