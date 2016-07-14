from mysql.connector.constants import ClientFlag
TEST_MODE = False
redis_host = 'localhost'
phase = 'PRD'
redis_channel = 'records'
LOG_FILENAME = '/ama/gmstst/goswatch/log/goswatch.log'
log_name = "goswatch"
flags = [ClientFlag.FOUND_ROWS]
CONFIG = {'user': 'ho_user',
          #'user': 'root',
          'password': '4m4d3u5',
          'database': 'trends',
          'host': 'localhost',
          'port': '3307',
          #'unix_socket': '/ama/gmstst/data/ALL/database/mysql/mysql.sock',
          'client_flags' : flags, #Added due to duplicates
          #'get_warnings' : True
          }

#GROUPS = ['OIAOPS', 'OOPSZR6']
#GROUPS = ['OIAOPS', 'OCMOFGOP', 'OCMOFGOT']
#Test team disabled as per MIA TSS request
GROUPS = ['OIAOPS', 'OCMOFGOP', 'OIMOIFS']
MCA = ['CML', 'FML', 'TDS', 'LSS', 'RFD', 'AML', 'JFS', 'MSS', 'SIT', 'SES',
       'APL', 'ESS', 'MSG', 'NCM', 'NFM', 'TSW', 'DAS']
MCR = ['RES', 'IIP', 'PAP', 'ROC', 'DIR', 'SEL', 'MID', 'NOX', 'PPP', 'APA',
       'PFX', 'NGI', 'APE', 'APS', 'RDI', 'ETK', 'CDB', 'MDS', 'POR', 'WBA',
			 'ETS', 'CTS', 'PQR', 'ATT', 'DEC', 'WBS', 'ECO', 'PUB', 'LFS', 'FQI', 'ADL']
