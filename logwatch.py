import logging
import logging.handlers
import config

log = logging.getLogger(config.log_name)
log.setLevel(logging.INFO)
# create console handler and set level to debug
handler = logging.handlers.RotatingFileHandler(config.LOG_FILENAME, maxBytes=50000000, backupCount=20)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
handler.setFormatter(formatter)
log.addHandler(handler)

if __name__ == "__main__":
    pass
