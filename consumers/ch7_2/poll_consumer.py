from confluent_kafka import Consumer
from confluent_kafka import KafkaError, KafkaException
from consumers.base_consumer import BaseConsumer
import pandas as pd
import sys
import json
import time


class PollConsumer(BaseConsumer):
    def __init__(self, group_id):
        super().__init__(group_id)
        self.topics = ['apis.seouldata.rt-bicycle']
        self.MIN_COMMIT_COUNT = 100
        conf = {'bootstrap.servers': self.BOOTSTRAP_SERVERS,
                'group.id': self.group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': 'false'}

        self.consumer = Consumer(conf)
        self.consumer.subscribe(self.topics, on_assign=self.callback_on_assign)


    def poll(self):
        msg_cnt = 0
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None: continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())


                # 로직 처리 부분
                # Kafka 레코드에 대한 전처리, Target Sink 등 수행
                df = pd.DataFrame([json.loads(msg.value().decode('utf-8'))])
                print(df)
                msg_cnt += 1

                # 로직 처리 완료 후 Async Commit 수행 후 2초 대기
                # 커밋 구간 사이에서 Consumer Program Down & 재시작하는 경우 메시지 중복처리가 될 수 있음
                if msg_cnt % self.MIN_COMMIT_COUNT == 0:
                    self.consumer.commit(asynchronous=True)
                    self.logger.info(f'Commit 완료, partition: {msg.partition()}, offset: {msg.offset()}')
                    time.sleep(2)

        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()


if __name__ == '__main__':
    consume_consumer = PollConsumer('poll_consumer')
    consume_consumer.poll()
