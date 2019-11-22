import json
import sentry_sdk
from google.cloud import pubsub_v1

from devourer import config
from devourer.utils import json_helpers


class DataPublisher:

    def __init__(self):
        self.client = pubsub_v1.PublisherClient()
        self.topic_path = self.client.topic_path(
            config.GCP_PROJECT_ID,
            config.GCP_PUBSUB_PUBLIC_TOPIC
        )

    def publish(self, data: dict) -> bool:
        msg = json.dumps(data, cls=json_helpers.JSONEncoder)

        future = self.client.publish(self.topic_path, data=msg.encode('utf-8'))

        try:
            future.result()
        except Exception as ex:
            sentry_sdk.capture_exception(ex)

        return future.done()
