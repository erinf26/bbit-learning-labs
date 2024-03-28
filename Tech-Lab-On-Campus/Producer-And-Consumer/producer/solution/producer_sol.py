from producer_interface import mqProducerInterface

import pika
import os


class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        # body of constructor
        # str ?

        #set up parametes
        self.routing_key = routing_key

        self.exchange_name = exchange_name

        #call connection
        self.setupRMQConnection()


    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service

        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        self.channel = self.connection.channel()

        # Create the exchange if not already present
        self.channel.exchange_declare(exchange=self.exchange_name)

        pass

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
    
        pass