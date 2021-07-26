import json
import os
import pika
import sqlite3
import sys
import requests

from contextvars import ContextVar
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize


SQLITE_DATABASE = 'database.db'

porter = PorterStemmer()
amqp_connection = ContextVar('amqp_connection')


def main():
    amqp_connection.set(
        pika.BlockingConnection(pika.ConnectionParameters('localhost')))
    
    connection = amqp_connection.get()
    channel = connection.channel()

    channel.queue_declare(queue='requests', durable=True)

    def callback(channel, method, properties, body):
        body = json.loads(body)

        print(" [x] Received %r" % body)

        if body['method'] == 'process_chunk':
            _handle_process_chunk(
                request_id=body['param']['request_id'],
                line_id=body['param']['line_id'])
        elif body['method'] == 'process_webhook':
            _handle_process_webhook(
                request_id=body['param']['request_id'])

        channel.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue='requests', on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


def _handle_process_chunk(request_id, line_id):
    with sqlite3.connect(SQLITE_DATABASE) as sqlite_connection:
        request_line = sqlite_connection.execute(f'''
            SELECT * FROM
                request_line
            WHERE
                request_id = "{request_id}" AND
                line_id = {line_id}
            ''').fetchone()
    
    sentence = ''
    if request_line:
        sentence = str(request_line[2])

    stemmed_sentence = _stem_sentence(sentence) 

    with sqlite3.connect(SQLITE_DATABASE) as sqlite_connection:
        sqlite_connection.execute(f'''
            INSERT OR REPLACE INTO 
                result_line (request_id, line_id, value) 
            VALUES (
                "{request_id}", "{line_id}", "{stemmed_sentence}"
            );
            ''')
        sqlite_connection.execute(f'''
            DELETE FROM
                request_line
            WHERE
                request_id = "{request_id}" AND
                line_id = {line_id}
            ''')
        count_result = sqlite_connection.execute(f'''
            SELECT 
                COUNT(*)
            FROM
                request_line
            WHERE
                request_id = "{request_id}" 
            ''').fetchone()
        if count_result is None:
            count = 0
        else:
            count = count_result[0]

    if count == 0:
        connection = amqp_connection.get()
        channel = connection.channel()
        payload = json.dumps({
            'method': 'process_webhook',
            'param': {
                'request_id': request_id
            }
        })
        channel.basic_publish(
            exchange='',
            routing_key='requests',
            body=payload,
            properties=pika.BasicProperties(
                delivery_mode=2
            ))


def _handle_process_webhook(request_id):
    with sqlite3.connect(SQLITE_DATABASE) as sqlite_connection:
        request_result = sqlite_connection.execute(f'''
            SELECT * FROM
                request
            WHERE
                id = "{request_id}"
           ''').fetchone()
        webhook_url = request_result[2]

        request_line = sqlite_connection.execute(f'''
            SELECT * FROM
                result_line
            WHERE
                request_id = "{request_id}"
            ORDER BY
                line_id ASC
           ''').fetchall()
        content = "".join([x[2] for x in request_line])
    if webhook_url is not None:
        requests.post(webhook_url, data=content)


def _stem_sentence(sentence):
    token_words = word_tokenize(sentence)
    stem_sentence = []
    for word in token_words:
        stem_sentence.append(porter.stem(word))
        stem_sentence.append(" ")
    return "".join(stem_sentence)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

