import requests
import pytest


def test_get_topics(create_topic):
    topics = requests.get("http://localhost:8080/topics")
    print("Hello")
    print(topics.text)
    assert topics.status_code == 200


def test_describe_topics():
    r = requests.get("http://localhost:8080/topics/describe")
    assert r.status_code == 200


def test_describe_topic(create_topic):
    r = requests.get("http://localhost:8080/topic/describe/test-topic")
    if r.status_code == 202:
        assert len(r.headers["queue-id"]) > 0
    if r.status_code > 202:
        AssertionError('status code > 202')


@pytest.fixture(scope="session")
def create_topic():
    r = requests.post("http://localhost:8080/topic/test-topic")
