import requests
import pytest


def test_create_topic():
    topic_response = requests.post("http://localhost:8080/topic/another-topic")
    assert topic_response.status_code == 202
