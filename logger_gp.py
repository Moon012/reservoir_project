import logging
from logging.handlers import RotatingFileHandler
import datetime

logger = logging.getLogger()

#로그생성
def logger_defination():
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(u'%(asctime)s [%(levelname)8s] %(message)s')

    ##디렉토리가 형성 되어있어야 함
    file_path = 'D:\\pythonProject\\python_logs\\logfile_{:%Y%m%d}.log'.format(datetime.datetime.now())

    # StreamHandler
    streamingHandler = logging.StreamHandler()
    streamingHandler.setFormatter(formatter)

    # RotatingFileHandler
    log_max_size = 10 * 1024 * 1024  ## 10MB
    log_file_count = 20
    rotatingFileHandler = logging.handlers.RotatingFileHandler(
        filename=file_path,
        maxBytes=log_max_size,
        backupCount=log_file_count,
        encoding='utf-8'
    )
    rotatingFileHandler.setFormatter(formatter)

    logger.addHandler(streamingHandler)
    logger.addHandler(rotatingFileHandler)

# logger instnace로 log 찍기

# logger.setLevel(level=logging.DEBUG)
# logger.debug('my DEBUG log')
#간단히 문제를 진단하고 싶을 때 필요한 자세한 정보를 기록함
# logger.info('my INFO log')
#계획대로 작동하고 있음을 알리는 확인 메시지
# logger.warning('my WARNING log')
#소프트웨어가 작동은 하고 있지만, 예상치 못한 일이 발생했거나 할 것으로 예측된다는 것을 알림
# logger.error('my ERROR log')
# 중대한 문제로 인해 소프트웨어가 몇몇 기능들을 수행하지 못함을 알림
# logger.critical('my CRITICAL log')
# 작동이 불가능한 수준의 심각한 에러가 발생함을 알림    

logger_defination()
