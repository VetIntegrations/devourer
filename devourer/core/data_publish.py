import json
import queue
import threading
import os
import logging
from google.cloud import pubsub_v1

from devourer import config
from devourer.utils import json_helpers


logger = logging.getLogger('devourer.publisher')


class DataPublisher:

    def __init__(self, workers_count: int = None):
        self.futures = queue.Queue()
        self.exit_event = threading.Event()

        self.client = pubsub_v1.PublisherClient(
            batch_settings=pubsub_v1.types.BatchSettings(max_messages=100)
        )
        self.topic_path = self.client.topic_path(
            config.GCP_PROJECT_ID,
            config.GCP_PUBSUB_PUBLIC_TOPIC
        )

        self.workers = []
        if workers_count is None:
            workers_count = max(os.cpu_count() // 2, 2)
        for i in range(workers_count):
            worker = threading.Thread(
                target=self._publish_worker,
                kwargs={'futures': self.futures, 'exit_event': self.exit_event},
                daemon=True
            )
            worker.start()
            self.workers.append(worker)

    def publish(self, data: dict) -> bool:
        """Send json serialized message to GCP Pub/Sub and put sending future
        to the queue for checking
        """
        msg = json.dumps(data, cls=json_helpers.JSONEncoder)

        future = self.client.publish(self.topic_path, data=msg.encode('utf-8'))
        self.futures.put(future)

    def exit(self):
        """Send exit signal to thread workers"""
        self.exit_event.set()

    def wait(self):
        """Waiting until futures queue become empty"""
        self.futures.join()

    @staticmethod
    def _publish_worker(futures: queue.Queue, exit_event: threading.Event):
        """Thread worker that check that message was sent to GCP Pub/Sub"""
        while not exit_event.is_set() or not futures.empty():
            try:
                future = futures.get(timeout=0.2)
            except queue.Empty:
                pass
            else:
                try:
                    future.result()
                except Exception as ex:
                    logger.exception('Unable to send message to GCP Pub/Sub: %s', ex)

                futures.task_done()
