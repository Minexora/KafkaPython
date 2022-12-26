import os
import msgpack
from dotenv import load_dotenv
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer

load_dotenv()


class KafkaClass:
    admin_client = None

    def __init__(self):
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=f'{os.getenv("KafkaQueueHost")}:{os.getenv("KafkaQueuePort")}',
                client_id=os.getenv('KafkaQueueClientId')
            )
        except Exception as e:
            print('Kafkaya bağlanılamadı. Oluşan Hata: {}'.format(str(e)))

    def get_topic_list(self):
        return self.admin_client.list_topics()

    def create_topic(self, topic_name="chatbot_queue"):
        if self.admin_client:
            try:
                topics = self.get_topic_list()
                if topic_name not in topics:
                    print(f"{topic_name} isimli topic oluşturuluyor.")
                    topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
                    self.admin_client.create_topics(new_topics=[topic],  validate_only=False)
                else:
                    print(f"Oluşturulmak istenen {topic_name} adlı topic daha önceden oluşturulmuştur.")
                return True
            except Exception as e:
                print(f"{topic_name} adlı topic oluşturulurken hata oluştu! Hata: {str(e)}")
                return False
        return False

    def delete_topic(self, topic_name):
        try:
            print(f"{topic_name} adlı topic siliniyor.")
            topics = self.get_topic_list()
            if (topic_name in topics):
                self.admin_client.delete_topics(topics=[topic_name])
                print(f"{topic_name} adlı topic silindi.")
                return True
            else:
                print(f"{topic_name} adlı topic daha önce oluşturulmamıştır.")
                return False
        except Exception as e:
            print(f"{topic_name} adlı topic silinirken hata ile karşılaşıldı! Hata: {str(e)}")
            return False


class KafkaPublisher:
    __instance = None

    def __init__(self, topic_name=None):
        self.producer = None
        self.topic_name = topic_name
        if topic_name is not None:
            res = KafkaClass().create_topic(topic_name)
        else:
            res = KafkaClass().create_topic()

        if (res):
            self.connect()
        else:
            print('Topic oluşturulamaması nedeniyle publisher çalıştırılamıyor.')

        if KafkaPublisher.__instance is not None:
            raise Exception("KafkaPublisher exists already!")
        else:
            KafkaPublisher.__instance = self

    def connect(self):
        try:
            print("Kafka bağlantısı oluşturuluyor.")
            server = f'{os.getenv("KafkaQueueHost")}:{os.getenv("KafkaQueuePort")}'
            self.producer = KafkaProducer(bootstrap_servers=[server], client_id=os.getenv('KafkaQueueClient'), value_serializer=msgpack.packb)
            print("Kafka publisher bağlantısı oluşturuldu.")
        except Exception as e:
            print('Kafkaya bağlanılamadı. Oluşan Hata: {}'.format(str(e)))

    def send_data_to_topic(self, topic_name='chatbot_queue', data={}, onsuccess_fn=None, onfailure_fn=None):
        if (self.producer is None):
            self.connect()
        try:
            if onsuccess_fn and onfailure_fn:
                res = self.producer.send(topic=topic_name, value=data).add_callback(onsuccess_fn).add_errback(onfailure_fn)
            else:
                res = self.producer.send(topic=topic_name, value=data)
            print('res:')
            print(res.value)
        except Exception as e:
            print(f"Topic'e data gönderilemedi! Hata: {str(e)}")

    @staticmethod
    def getInstance():
        return KafkaPublisher.__instance


class KafkaConsumerClass:
    __instance = None

    def __init__(self, topic_name=None):
        self.consumer = None
        self.topic_name = topic_name
        if topic_name is not None:
            res = KafkaClass().create_topic(topic_name)
        else:
            res = KafkaClass().create_topic()

        if (res):
            self.connect(topic_name)
        else:
            print('Topic oluşturulamaması nedeniyle publisher çalıştırılamıyor.')

        if KafkaConsumerClass.__instance is not None:
            raise Exception("KafkaPublisher exists already!")
        else:
            KafkaConsumerClass.__instance = self

    def connect(self, topic):
        try:
            print("Kafka bağlantısı oluşturuluyor.")
            server = f'{os.getenv("KafkaQueueHost")}:{os.getenv("KafkaQueuePort")}'
            self.consumer = KafkaConsumer(
                bootstrap_servers=[server],
                client_id=os.getenv('KafkaQueueClient'),
                value_deserializer=msgpack.unpackb,
                auto_offset_reset='latest',

            )
            print("Kafka consumer bağlantısı oluşturuldu.")
        except Exception as e:
            print('Kafkaya bağlanılamadı. Oluşan Hata: {}'.format(str(e)))

    @staticmethod
    def getInstance():
        return KafkaConsumerClass.__instance
