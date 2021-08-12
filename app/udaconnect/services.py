import logging
import threading
import json
import grpc

from datetime import datetime, timedelta
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor

import app.udaconnect.proto.person_pb2 as per_pb2
import app.udaconnect.proto.person_pb2_grpc as per_pb2_grpc

from app import db, app
from app.udaconnect.models import Person
from app.udaconnect.schemas import PersonSchema
from geoalchemy2.functions import ST_AsText, ST_Point
from sqlalchemy.sql import text

from kafka import KafkaConsumer

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("udaconnect-api")

class PersonService:
    @staticmethod
    def create_from_kafka(person: Dict) -> Person:
        validation_results: Dict = PersonSchema().validate(person)
        if validation_results:
            logger.warning(f"Unexpected data format in payload: {validation_results}")
            raise Exception(f"Invalid payload: {validation_results}")

        new_person = Person()
        new_person.first_name = person["first_name"]
        new_person.last_name = person["last_name"]
        new_person.company_name = person["company_name"]

        with app.app_context():
            db.session.add(new_person)
            db.session.commit()
            logger.info("Added new person to database")

        return new_person

    @staticmethod
    def retrieve(person_id: int) -> Person:
        with app.app_context():
            person = db.session.query(Person).get(person_id)
        return per_pb2.Person(
                id=person["id"],
                first_name=person["first_name"],
                last_name=person["last_name"],
                company_name=person["company_name"]
                )

    @staticmethod
    def retrieve_all() -> List[Person]:
        with app.app_context():
            persons = db.session.query(Person).all()
        return per_pb2.PersonList(
                persons=persons
                )

    # gRPC functions
    def Get(self, request, context):
        return self.retrieve(request.id)

    def GetAll(self, request, context):
        return self.retrieve_all()

def setup_grpc_server(server_port):
    server = grpc.server(ThreadPoolExecutor(max_workers=5))
    per_pb2_grpc.add_PersonServiceServicer_to_server(PersonService(), server)
    server.add_insecure_port(server_port)

    def serve(server):
        server.start()
        logger.debug('gRPC server added')
        server.wait_for_termination()

    serve_thread = threading.Thread(target=serve, args=(server,))
    serve_thread.start()

def setup_kafka_consumer(topic_name, server, port):
    consuming = True
    kafka_server = ':'.join([server, port])
    logger.debug('Connecting to kafka server {}'.format(kafka_server))
    consumer = KafkaConsumer(
            topic_name, 
            bootstrap_servers=[kafka_server], 
            api_version=(2, 8, 0),
            value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    def consume(consumer):
        while consuming:
            for message in consumer:
                message = message.value
                PersonService().create_from_kafka(message)

    # Thread does not stop
    consume_thread = threading.Thread(target=consume, args=(consumer,))
    consume_thread.start()
    logger.debug('Kafka consumer thread started')

