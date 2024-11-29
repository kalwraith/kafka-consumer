from confluent_kafka import Consumer
from confluent_kafka import KafkaError, KafkaException
from consumers.base_consumer import BaseConsumer
import pandas as pd
import sys
import json
import time

class ConsumeConsumer(BaseConsumer):
    def __init__(self, group_id):
        super().__init__(group_id)
        self.topics = ['apis.seouldata.rt-bicycle']

        conf = {'bootstrap.servers': self.BOOTSTRAP_SERVERS,
                'group.id': self.group_id,
                'auto.offset.reset': 'latest',
                'enable.auto.commit': 'true',
                'auto.commit.interval.ms': '60000'         # 기본 값: 5000 (5초)
                }

        self.consumer = Consumer(conf)
        self.consumer.subscribe(self.topics, on_assign=self.callback_on_assign)


    def poll(self):
        try:
            while True:
                msg_lst = self.consumer.consume(num_messages=100)
                if msg_lst is None or len(msg_lst) == 0: continue

                self.logger.info(f'message count:{msg_lst.count()}')
                for msg in msg_lst:
                    if msg.error():
                        if msg_lst.error().code() == KafkaError._PARTITION_EOF:
                            # End of partition event
                            sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                             (msg.topic(), msg.partition(), msg.offset()))
                        elif msg.error():
                            raise KafkaException(msg.error())

                # 로직 처리 부분
                # Kafka 레코드에 대한 전처리, Target Sink 등 수행
                self.logger.info(f'message 처리 로직 시작')
                msg_val_lst = [json.loads(msg.value().decode('utf-8')) for msg in msg_lst]
                df = pd.DataFrame(msg_val_lst)
                print(df)

        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()


if __name__ == '__main__':
    consume_consumer= ConsumeConsumer('auto_commit_consumer')
    consume_consumer.poll()