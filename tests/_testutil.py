import asyncio
import os
import socket
import string
import random
import unittest
import uuid

from functools import wraps

__all__ = ['get_open_port', 'KafkaIntegrationTestCase', 'random_string']


def run_until_complete(fun):
    if not asyncio.iscoroutinefunction(fun):
        fun = asyncio.coroutine(fun)

    @wraps(fun)
    def wrapper(test, *args, **kw):
        loop = test.loop
        ret = loop.run_until_complete(
            asyncio.wait_for(fun(test, *args, **kw), 15, loop=loop))
        return ret
    return wrapper


class BaseTest(unittest.TestCase):
    """Base test case for unittests.
    """
    kafka_host = os.environ.get('KAFKA_HOST')
    kafka_port = os.environ.get('KAFKA_PORT')

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()
        del self.loop


class KafkaIntegrationTestCase(BaseTest):

    topic = None

    def setUp(self):
        super().setUp()
        assert all([self.kafka_host, self.kafka_port]),\
            'Required env variables are not provided'
        self.hosts = ['{}:{}'.format(self.kafka_host, self.kafka_port)]
        self._messages = {}

    def tearDown(self):
        super().tearDown()

    @asyncio.coroutine
    def wait_topic(self, client, topic):
        client.add_topic(topic)
        for i in range(5):
            ok = yield from client.force_metadata_update()
            if ok:
                ok = topic in client.cluster.topics()
            if not ok:
                yield from asyncio.sleep(1, loop=self.loop)
            else:
                return
        raise AssertionError('No topic "{}" exists'.format(topic))

    def msgs(self, iterable):
        return [self.msg(x) for x in iterable]

    def msg(self, s):
        if s not in self._messages:
            self._messages[s] = '%s-%s-%s' % (s, self.id(), str(uuid.uuid4()))

        return self._messages[s].encode('utf-8')

    def key(self, k):
        return k.encode('utf-8')


def get_open_port():
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def random_string(length):
    s = "".join(random.choice(string.ascii_letters) for _ in range(length))
    return s.encode('utf-8')
