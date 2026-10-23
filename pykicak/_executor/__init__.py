"""The parts `pykicak.executor.KicakExecutorAbstract` is built of; private, not part of the API.

- `topology`: the queue type, the names derived from a queue, and the declarations.
- `settings`: how often `execute()` is retried, and how long a message waits between rounds.
- `message_types`: the message types an Executor class declares in its class header.
- `headers`: what the Executor reads from a delivery's properties, and writes on its copies.
- `status`: what `get_info()` reports, and how the deliveries ended.
- `statistics`: the `Message handled` line logged for every delivery.
- `session`: the consumer of one connection, and the channel that publishes its results.
- `settlement`: acknowledging, requeueing, dead-lettering, or moving a delivery to the retry queue.
"""

import logging

logger = logging.getLogger("pykicak.executor")
"""The logger of every part: the Executor's records keep the name of its public module, so that
an application's handler or level on `pykicak.executor` receives all of them."""
