from kafka_class import KafkaConsumerClass
from kafka.errors import KafkaError


def msg_process(msg):
    print(msg)


def consumer(topic):
    _consumer = KafkaConsumerClass(topic_name=topic)
    _consumer.consumer.subscribe(topics=[topic])

    try:
        for msg in _consumer.consumer:
            msg_process(msg)
    except Exception as e:
        print(str(e))


if __name__ == '__main__':
    consumer('chatbot_queue')
