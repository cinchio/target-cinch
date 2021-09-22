from target_cinch.service import Service
import singer
from singer import logger

BATCH_SIZE = 500

DEPENDENCIES = {
    "schedule": ["customer_ref"],
    "transaction": [
        "customer_ref",
        "location",
    ],
    "transaction_detail": ["transaction"],
}


class Processor:
    config = None
    batch_queues = None

    def __init__(self, args):
        self.config = args.config

        self.service = Service(self.config['email'], self.config['password'], self.config.get('environment'))

        self.batch_queues = {
            "location": [],
            "customer_ref": [],
            "schedule": [],
            "transaction": [],
            "transaction_detail": [],
            "engagement": [],
        }

    def post_batch(self, model):
        if not self.batch_queues[model]:
            return

        if model in DEPENDENCIES:
            # Make sure all queued-dependent models are posted first.
            # Warning: this can caues a circular dependency issue if not set up correctly
            for dependency in DEPENDENCIES[model]:
                self.post_batch(dependency)

        if model == "location":
            self.service.post_locations(self.batch_queues[model])
        elif model == "customer_ref":
            self.service.post_customer_refs(self.batch_queues[model])
        elif model == "schedule":
            self.service.post_schedules(self.batch_queues[model])
        elif model == "transaction":
            self.service.post_transactions(self.batch_queues[model])
        elif model == "transaction_detail":
            self.service.post_transaction_details(self.batch_queues[model])
        elif model == "engagement":
            self.service.post_engagements(self.batch_queues[model])

        self.batch_queues[model] = []

    def add_to_queue(self, model, record):
        # Add record to the queue
        self.batch_queues[model].append(record)

        if len(self.batch_queues[model]) >= BATCH_SIZE:
            # If we hit our batch size then post the data to the API
            self.post_batch(model)

    def process_schema(self, message):
        # TODO should we do something with the schema?
        pass

    def process_record(self, message):
        self.add_to_queue(message['stream'], message['record'])

    def process_state(self, message):
        # Pass on state messages
        singer.write_state(message['value'])

    def process(self, message):
        if message['type'] == 'SCHEMA':
            self.process_schema(message)
        elif message['type'] == 'RECORD':
            self.process_record(message)
        elif message['type'] == 'STATE':
            self.process_state(message)

    def finalize(self):
        # Send any left over batch values here
        for model, records in self.batch_queues.items():
            if records:
                self.post_batch(model)
