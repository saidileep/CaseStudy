import logging

def log(alert_level, message):
        """Load data from input csv files and deduplicates the data
        :param alert_level: type of log to classify actions based on it
        :param message: String messages which needs to be logged
        :return: None
        """
        logs_object = {
                       'alertLevelCd': alert_level,
                       'logMessage': message,
                       'eventDtTime': str(datetime.datetime.now())}

        if alert_level == 'INFO':
            print(logs_object)
            logger.info(logs_object)
        elif alert_level == 'ERROR':
            print(logs_object)
            logger.error(logs_object)
            #Send email in case of errors

        # Code can be added to log things into database for log analytics.