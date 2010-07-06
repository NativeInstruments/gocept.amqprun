import amqplib.client_0_8 as amqp
import sys

with amqp.Connection(host='localhost') as connection:
    with connection.channel() as channel:
        channel.basic_publish(
            amqp.Message(sys.argv[2]), 'amq.topic', routing_key=sys.argv[1])
