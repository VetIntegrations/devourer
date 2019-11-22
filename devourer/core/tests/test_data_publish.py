from google.cloud import pubsub_v1

from devourer import config
from devourer.core import data_publish


TEST_GCP_PROJECT_ID = 'gcp-prj-id'
TEST_GCP_PUBSUB_PUBLIC_TOPIC = 'test-topic'


def test_datapublisher(monkeypatch):
    log = []

    class FakeFuture:

        def done(self):
            log.append('future-done')

            return True

    class FakePublisherClient:

        def topic_path(self, project_id, topic_name):
            log.append(('topic_path', project_id, topic_name))
            return f'{project_id}/{topic_name}'

        def publish(self, topic, data):
            log.append(('publish', topic, data))

            return FakeFuture()

    monkeypatch.setattr(config, 'GCP_PROJECT_ID', TEST_GCP_PROJECT_ID)
    monkeypatch.setattr(config, 'GCP_PUBSUB_PUBLIC_TOPIC', TEST_GCP_PUBSUB_PUBLIC_TOPIC)
    monkeypatch.setattr(pubsub_v1, 'PublisherClient', FakePublisherClient)

    assert len(log) == 0
    publisher = data_publish.DataPublisher()
    assert publisher.publish({'msg': 'Hello'})
    assert log == [
        ('topic_path', TEST_GCP_PROJECT_ID, TEST_GCP_PUBSUB_PUBLIC_TOPIC),
        ('publish', f'{TEST_GCP_PROJECT_ID}/{TEST_GCP_PUBSUB_PUBLIC_TOPIC}', b'{"msg": "Hello"}'),
        'future-done'
    ]
