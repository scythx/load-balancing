import json
import pika
import sqlite3

from flask import Flask, request
from flask_restful import Resource, Api
from uuid import uuid4


SQLITE_DATABASE = 'database.db'
MAX_CHUNK_SIZE = 32

app = Flask(__name__)
api = Api(app)


class Stem(Resource):
    def get(self):
        return _enqueue_request(
            request.stream, request.args.get('webhook')), 202


class StemResult(Resource):
    def get(self, id):
        connection = sqlite3.connect(SQLITE_DATABASE)
        with connection:
            rows = connection.execute(f'''
                SELECT 
                    * 
                FROM 
                    request
                WHERE
                    id = "{id}"
                ''').fetchall()

            if not len(rows):
                return '', 404

            return str(rows[0]), 200


def _enqueue_request(message, webhook=None) -> str:
    correlation_id = str(uuid4())

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='requests', durable=True)

    with sqlite3.connect(SQLITE_DATABASE) as sqlite_connection:
        sqlite_connection.execute(f'''
            INSERT INTO 
                request (id, webhook_url) 
            VALUES (
                "{correlation_id}", ?
            );
            ''', (webhook,))

        for id, value in enumerate(message.readlines()):
            value = value.decode("utf-8")
            sqlite_connection.execute(f'''
                INSERT OR REPLACE INTO 
                   request_line (request_id, line_id, value) 
                VALUES (
                    "{correlation_id}", "{id}", ?
                );
                ''', (value,))
            payload = json.dumps({
                'method': 'process_chunk',
                'param': {
                    'request_id': correlation_id,
                    'line_id': id
                }
            })
            channel.basic_publish(
                exchange='',
                routing_key='requests',
                body=payload,
                properties=pika.BasicProperties(
                    delivery_mode=2
                ))

    connection.close()

    return correlation_id


def _prepare_database():
    connection = sqlite3.connect(SQLITE_DATABASE)
    with connection:
        connection.execute('''
            CREATE TABLE IF NOT EXISTS request (
    	        id TEXT PRIMARY KEY,
   	            result TEXT NULL,
                webhook_url TEXT NULL
            );
            ''')
        connection.execute('''
            CREATE TABLE IF NOT EXISTS request_line (
    	        request_id TEXT NOT NULL,
                line_id INTEGER NOT NULL,
   	            value TEXT NULL,
                PRIMARY KEY (request_id, line_id)
            );
            ''')
        connection.execute('''
            CREATE TABLE IF NOT EXISTS result_line (
    	        request_id TEXT NOT NULL,
                line_id INTEGER NOT NULL,
   	            value TEXT NULL,
                PRIMARY KEY (request_id, line_id)
            );
            ''')



_prepare_database()

api.add_resource(Stem, '/')
api.add_resource(StemResult, '/results/<id>')