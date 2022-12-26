from kafka_class import KafkaPublisher


def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)


def on_send_error(excp):
    print('I am an errback', exc_info=excp)


def publish(topic, data):
    publisher = KafkaPublisher(topic_name=topic)
    if topic is None:
        publisher.send_data_to_topic(data=data, onsuccess_fn=on_send_success, onfailure_fn=on_send_error)
    else:
        publisher.send_data_to_topic(topic_name=topic, data=data)


if __name__ == '__main__':
    publish(None, {'name': 'giray', 'last_name': 'Akkaya'})
