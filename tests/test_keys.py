from dramatiq_redis_streams.keys import (
    GROUP_NAME,
    consumer_name,
    delayed_key,
    dlq_stream_key,
    stream_key,
)


class TestStreamKey:
    def test_default_namespace(self):
        assert stream_key("default") == "dramatiq:stream:default"

    def test_custom_namespace(self):
        assert stream_key("default", namespace="myapp") == "myapp:stream:default"

    def test_special_characters_in_queue_name(self):
        assert stream_key("my-queue.DQ") == "dramatiq:stream:my-queue.DQ"


class TestDelayedKey:
    def test_default_namespace(self):
        assert delayed_key() == "dramatiq:delayed"

    def test_custom_namespace(self):
        assert delayed_key(namespace="myapp") == "myapp:delayed"


class TestDlqStreamKey:
    def test_default_namespace(self):
        assert dlq_stream_key("default") == "dramatiq:dlq:default"

    def test_custom_namespace(self):
        assert dlq_stream_key("default", namespace="myapp") == "myapp:dlq:default"


class TestConsumerName:
    def test_format(self):
        assert consumer_name("abc123") == "worker-abc123"


class TestGroupName:
    def test_value(self):
        assert GROUP_NAME == "workers"
