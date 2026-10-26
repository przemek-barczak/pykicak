"""The parts `pykicak.injector.KicakInjectorAbstract` is built of; private, not part of the API.

- `topology`: the exchange an Injector publishes to, and how it is declared.
- `settings`: how long an attempt waits while blocked, how often it is retried, and the interval.
- `message_type`: the message type an Injector class declares, and checking a message against it.
- `statistics`: the `Message injected` line logged for every message handed to RabbitMQ.
"""

import logging

logger = logging.getLogger("pykicak.injector")
"""The logger of every part: the Injector's records keep the name of its public module, so that
an application's handler or level on `pykicak.injector` receives all of them."""
