import io
import json
import sys
import traceback

import singer
from target_cinch.processor import Processor

LOGGER = singer.get_logger()
REQUIRED_CONFIG_KEYS = ['email', 'password']


@singer.utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    processor = Processor(args)

    try:
        log_message = None
        input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
        for message in input_messages:
            message = message.strip()

            if not message:
                continue

            try:
                message = json.loads(message)
                processor.process(message)
            except ValueError:
                print(message, flush=True)
                bits = message.split(' ', 1)

                try:
                    # Check if we have a JSON log message
                    message = json.loads(bits[1])
                    processor.process_log(message)
                except (ValueError, IndexError):
                    # Log message is not JSON
                    if bits[0] in ('NOTSET', 'DEBUG', 'INFO', 'WARNING',):
                        # This is a log message but we don't care about it.
                        if log_message:
                            processor.send_error(log_message)

                        log_message = None
                    elif bits[0] in ('ERROR', 'CRITICAL',):
                        # This is an error message we need to track
                        log_message = message

                    elif log_message:
                        # We have a potentially multi-line log message
                        log_message += '\n' + message

        if log_message:
            processor.send_error(log_message)

    except:
        # Catch all exceptions and try and send to the service
        processor.send_error(traceback.format_exc())

    processor.finalize()


if __name__ == "__main__":
    main()
