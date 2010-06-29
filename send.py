import amqplib.client_0_8 as amqp
import sys

connection = amqp.Connection(host='localhost')
channel = connection.channel()
channel.basic_publish(amqp.Message(sys.argv[1]), 'amq.fanout')
