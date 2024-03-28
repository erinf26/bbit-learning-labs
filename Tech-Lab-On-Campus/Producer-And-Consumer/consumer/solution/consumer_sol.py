import pika
import os
from consumer_interface import mqConsumerInterface

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        # Save parameters to class variables
        self.__binding_key = binding_key
        self.__exchange_name = exchange_name
        self.__queue_name = queue_name
        # Call setupRMQConnection
        self.setupRMQConnection()
    
    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        
        # Establish channel
        self.channel = self.connection.channel()
        
        # Create Queue if not already present
        self.channel.queue_declare(queue=self.__queue_name)

        # Create the exchange if not already present
        self.channel.exchange_declare(exchange=self.__exchange_name)

        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(
            queue= self.__queue_name,
            routing_key= self.__binding_key,
            exchange= self.__exchange_name,
        )

        # Set-up Callback function for receiving messages
        self.channel.basic_consume(self.__queue_name, self.on_message_callback, auto_ack=False)

        print("Setup check")
    
    def on_message_callback(self, channel, method_frame, header_frame, body) -> None:
        print("here")
        # Acknowledge message
        self.channel.basic_ack(method_frame.delivery_tag, False)
    
        #Print message (The message is contained in the body parameter variable)
        print(body)
        print("Message callback")
        
    def startConsuming(self) -> None:
        # Start consuming messages
        self.channel.start_consuming()
        print("Start consume check")


    def __del__(self) -> None:
        print("del called")
        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")
        # Close channel
        # Close Connection
        self.channel.close()
        self.connection.close()
        print("del complete")
        