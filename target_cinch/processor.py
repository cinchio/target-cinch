from target_cinch.service import Service
import singer
import uuid
from singer import logger
import hashlib
from datetime import datetime

BATCH_SIZE = 500

DEPENDENCIES = {
    "schedule": [
        "customer_ref",
    ],
    "transaction": [
        "customer_ref",
        "location",
        "vehicle",
        "real_estate",
    ],
    "transaction_detail": [
        "transaction",
    ],
    "transaction_coupon": [
        "transaction",
    ],
    "vehicle": [
        "customer_ref",
    ],
    "real_estate": [
        "customer_ref",
    ],
    "subscription": [
        "customer_ref",
    ],
    "cart": [
        "customer_ref",
        "location",
        "vehicle",
        "real_estate",
    ],
    "cart_detail": [
        "cart",
    ],
    "cart_coupon": [
        "cart",
    ],
    "recommendation": [
        "customer_ref",
        "transaction",
        "vehicle",
        "real_estate",
    ],
}


class Processor:
    config = None
    batch_queues = None
    session = None

    def __init__(self, args):
        self.config = args.config

        self.service = Service(
            self.config["email"],
            self.config["password"],
            self.config.get("environment"),
        )

        self.batch_queues = {
            "location": [],
            "customer_ref": [],
            "schedule": [],
            "transaction": [],
            "transaction_detail": [],
            "transaction_coupon": [],
            "cart": [],
            "cart_detail": [],
            "cart_coupon": [],
            "engagement": [],
            "vehicle": [],
            "real_estate": [],
            "subscription": [],
            "recommendation": [],
        }

    def get_log_id(self, model=None):
        hex_string = hashlib.md5(f'{self.session["info"].get("id")}|{model or ""}'.encode("UTF-8")).hexdigest()
        return str(uuid.UUID(hex=hex_string))

    def post_log(self, model):
        if not self.session:
            return

        # Update integration log counts
        is_first = False
        if self.session['counts'].get(model, None) is None:
            is_first = True
            self.session['counts'][model] = 0

        self.session['counts'][model] += len(self.batch_queues[model])

        if is_first:
            self.service.post('integration/logs', {
                'id': self.get_log_id(model),
                'company': self.session["info"].get("company"),
                'credential': self.session["info"].get("credential"),
                'filepath': self.session["info"].get("filepath"),
                'source_stream': self.session["info"].get("stream"),
                'target_stream': model,
                'rows_affected': self.session['counts'][model]
            })
        else:
            self.service.patch(f'integration/logs/{self.get_log_id(model)}', {
                'rows_affected': self.session['counts'][model]
            })

    def post_batch(self, model):
        if not self.batch_queues[model]:
            return

        if model in DEPENDENCIES:
            # Make sure all queued-dependent models are posted first.
            # Warning: this can caues a circular dependency issue if not set up correctly
            for dependency in DEPENDENCIES[model]:
                self.post_batch(dependency)

        # Update integration log info
        self.post_log(model)

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
        elif model == "transaction_coupon":
            self.service.post_transaction_coupons(self.batch_queues[model])
        elif model == "cart":
            self.service.post_carts(self.batch_queues[model])
        elif model == "cart_detail":
            self.service.post_cart_details(self.batch_queues[model])
        elif model == "cart_coupon":
            self.service.post_cart_coupons(self.batch_queues[model])
        elif model == "engagement":
            self.service.post_engagements(self.batch_queues[model])
        elif model == "vehicle":
            self.service.post_vehicles(self.batch_queues[model])
        elif model == "real_estate":
            self.service.post_real_estates(self.batch_queues[model])
        elif model == "subscription":
            self.service.post_subscriptions(self.batch_queues[model])
        elif model == "recommendation":
            self.service.post_recommendations(self.batch_queues[model])

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
        self.add_to_queue(message["stream"], message["record"])

    def process_state(self, message):
        # Send necessary data before we send the state message so our state isn't out of sync
        self.finalize()
        # Pass on state messages
        singer.write_state(message["value"])

    def process(self, message):
        if message["type"] == "SCHEMA":
            self.process_schema(message)
        elif message["type"] == "RECORD":
            self.process_record(message)
        elif message["type"] == "STATE":
            self.process_state(message)

    def process_log(self, message):
        if message.get('event') == 'START':
            # TODO check if we have a previous unclosed session?
            # save session info in memory
            self.session = {
                "counts": {},
                "info": message
            }
        elif self.session and message.get('event') == 'END':
            # mark session complete? .strftime("%d/%m/%Y %H:%M:%S")
            for model, _counts in self.session['counts'].items():
                self.service.patch(f'integration/logs/{self.get_log_id(model)}', {
                    'completed_when': datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                })

            self.session = None

    def send_error(self, message):
        if self.session:
            if self.session['counts']:
                for model, _counts in self.session['counts'].items():
                    self.service.patch(f'integration/logs/{self.get_log_id(model)}', {
                        'error_message': message
                    })
            else:
                self.service.post('integration/logs', {
                    'id': self.get_log_id(),
                    'company': self.session["info"].get("company"),
                    'credential': self.session["info"].get("credential"),
                    'filepath': self.session["info"].get("filepath"),
                    'source_stream': self.session["info"].get("stream"),
                    'error_message': message
                })


    def finalize(self):
        # Send any left over batch values here
        for model, records in self.batch_queues.items():
            if records:
                self.post_batch(model)
