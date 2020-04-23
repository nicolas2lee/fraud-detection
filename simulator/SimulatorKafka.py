import pandas as pd
from kafka import KafkaProducer

class SimulatorKafka:
    def send(self):
        KAFKA_DEFAULT_CONFIG = {
            'bootstrap_servers': ['broker-2-jjpdp4qxrj0ztgsh.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093','broker-5-jjpdp4qxrj0ztgsh.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093','broker-0-jjpdp4qxrj0ztgsh.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093','broker-3-jjpdp4qxrj0ztgsh.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093','broker-4-jjpdp4qxrj0ztgsh.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093','broker-1-jjpdp4qxrj0ztgsh.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093'],
            #'client_id': None,
            #'key_serializer': None,
            #'value_serializer': None,
            'acks': 'all',
            #'compression_type': None,
            'sasl.jaas.config': 'org.apache.kafka.common.security.plain.PlainLoginModule required username = token password = "<api_key>";',
            'security_protocol': 'SASL_SSL',
            'ssl.protocol': 'TLSv1.2',
            'sasl_mechanism': 'PLAIN',
        }
        topic = 'test'
        producer = KafkaProducer(bootstrap_servers=['broker-2-jjpdp4qxrj0ztgsh.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093','broker-5-jjpdp4qxrj0ztgsh.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093','broker-0-jjpdp4qxrj0ztgsh.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093','broker-3-jjpdp4qxrj0ztgsh.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093','broker-4-jjpdp4qxrj0ztgsh.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093','broker-1-jjpdp4qxrj0ztgsh.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093'], \
                                 acks= 'all', \
                                 security_protocol = 'SASL_SSL', \
                                 sasl_mechanism = 'PLAIN', \
                                 sasl_plain_username = 'token', \
                                 sasl_plain_password= 'tAYDSKjd48gmLD48Xt2xn9CSzWSx9_EgxS6WxTIDum9g'
            )

        FILE_PREFIX = "./resource/"
        test_transaction = "test_transaction.csv"
        test_transaction_df = pd.read_csv("{}/{}".format(FILE_PREFIX, test_transaction))
        for e in test_transaction_df:
            print(e.to_csv)
            producer.send(topic, e.to_csv)

if __name__ == '__main__':
    s = SimulatorKafka()
    s.send()