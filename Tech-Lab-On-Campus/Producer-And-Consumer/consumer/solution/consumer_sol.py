import pika
import os

class mqConsumer(mqConsumerInterface):
     def __init__(self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        # Save parameters to class variables
        self.__binding_key = binding_key
        self.__exchange_name = exchange_name
        self.__queue_name = queue_name
        # Call setupRMQConnection
        setupRMQConnection()

    def __setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)
        
        # Establish Channel
        channel = connection.channel()
        
        # Create Queue if not already present
        channel.queue_declare(queue="Queue Name")

        # Create the exchange if not already present
        exchange = channel.exchange_declare(exchange="Exchange Name")

        # Bind Binding Key to Queue on the exchange
        channel.queue_bind(
            queue= "Queue Name",
            routing_key= "Routing Key",
            exchange="Exchange Name",
        )

        # Set-up Callback function for receiving messages
        channel.basic_consume(
            "Queue Name", Function Name, auto_ack=False
        )
        

    def on_message_callback(self, channel, method_frame, header_frame, body) -> None:
        # Acknowledge message
        channel.basic_ack(method_frame.delivery_tag, False)
    
        #Print message (The message is contained in the body parameter variable)
        print(body)


    def startConsuming(self) -> None:
        # Start consuming messages
        channel.start_consuming()


    def __del__(self) -> None:
        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")
        # Close Channel
        # Close Connection
        channel.close()
        connection.close()
        