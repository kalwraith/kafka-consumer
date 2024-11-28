import logging


class BaseConsumer():
    def __init__(self, group_id):
        self.BOOTSTRAP_SERVERS = 'kafka01:9092,kafka02:9092,kafka03:9092'
        self.group_id = group_id
        self.logger = self.get_logger(group_id)

    def get_logger(self, group_id):
        logger = logging.getLogger(group_id)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
        logger.addHandler(handler)

        return logger

    def callback_on_assign(self, consumer, partition):
        self.logger.info(f'consumer:{consumer}. assigned partition: {partition}')
