import pika
import os
from producer_interface import mqProducerInterface

class mqProducer(mqProducerInterface):

    def __init__(self, routing_key: str, exchange_name: str) -> None:
    
        self.channel = None
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        self.connection = pika.BlockingConnection(parameters=conParams)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange_name)
    
        # Establish Channel



    def publishOrder(self, message: str) -> None:
        # Basic Publish to Exchange
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=message,
        )
        # Close Channel
        # Close Connection
        self.channel.close()
        self.connection.close()
