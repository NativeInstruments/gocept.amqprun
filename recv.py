import amqplib.client_0_8 as amqp
import sys

with amqp.Connection(host='localhost') as connection:
    with connection.channel() as channel:
        channel.queue_declare(queue='test.receive', auto_delete=False)
        channel.queue_bind('test.receive', 'amq.topic',
                           routing_key='licensedproduct.data')
        while True:
            message = channel.basic_get('test.receive')
            if message:
                print message.body
                channel.basic_ack(message.delivery_tag)
            else:
                break
