import io
import json
import sys

import singer
from target_cinch.processor import Processor

LOGGER = singer.get_logger()
REQUIRED_CONFIG_KEYS = ['email', 'password']


@singer.utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    processor = Processor(args)

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    for message in input_messages:
        message = message.strip()

        if not message:
            continue

        try:
            message = json.loads(message)
            processor.process(message)
        except:
            print(message)

    processor.finalize()


if __name__ == "__main__":
    main()
