from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import contextlib

from threading import Thread

from kombu.mixins import ConsumerMixin
from kombu.pools import Producers

from .._compat import queue
from ..context import ContextFactory


class KombuPublisherContextFactory(ContextFactory):
    def __init__(self, connection, max_connections=None):
        self.connection = connection
        self.producers = Producers(limit=max_connections)

    def make_object_for_context(self, name, span):
        return KombuPublisher(name, span, self.connection, self.producers)


class KombuPublisher(object):
    def __init__(self, name, span, connection, producers):
        self.name = name
        self.span = span
        self.connection = connection
        self.producers = producers

    def publish(self, *args, **kwargs):
        trace_name = "{}.{}".format(self.name, "publish")
        child_span = self.span.make_child(trace_name)

        child_span.set_tag("kind", "producer")
        routing_key = kwargs.get("routing_key")
        if routing_key:
            child_span.set_tag("message_bus.destination", routing_key)

        with child_span:
            producer_pool = self.producers[self.connection]
            with producer_pool.acquire(block=True) as producer:
                return producer.publish(*args, **kwargs)


class _ConsumerWorker(ConsumerMixin):
    def __init__(self, connection, queues, no_ack, work_queue):
        self.connection = connection
        self.queues = queues
        self.no_ack = no_ack
        self.work_queue = work_queue

    def get_consumers(self, Consumer, channel):
        return [Consumer(
            self.queues,
            no_ack=self.no_ack,
            on_message=self.on_message,
        )]

    def on_message(self, message):
        self.work_queue.put(message)


class ConsumerContext(object):
    pass


# TODO: how does this work with qos()?
# TODO: use heartbeat
class KombuConsumer(object):
    def __init__(self, baseplate, connection, queues, no_ack=False):
        self.work_queue = queue.Queue()
        self.baseplate = baseplate
        self.consumer = _ConsumerWorker(
            connection, queues, no_ack, self.work_queue)
        self.no_ack = no_ack
        self.thread = None

    def start(self):
        assert self.thread is None, "consumer already started"
        self.thread = Thread(target=self.consumer.run)
        self.thread.name = "consumer message pump"
        self.thread.daemon = True
        self.thread.start()

    @contextlib.contextmanager
    def get_message(self, timeout=None):
        with self.get_batch(max_items=1, timeout=timeout) as (context, batch):
            yield context, batch[0]

    @contextlib.contextmanager
    def get_batch(self, max_items, timeout):
        assert self.thread is not None, "you must start the consumer first"

        if timeout == 0:
            block = False
        else:
            block = True

        batch = []
        # TODO: decrease timeout over time
        for _ in range(max_items):
            try:
                item = self.work_queue.get(block=block, timeout=timeout)
                batch.append(item)
            except queue.Empty:
                break

        context = ConsumerContext()
        with self.baseplate.make_server_span(context, "?????") as span:
            span.set_tag

            yield context, batch

            if not self.no_ack:
                for message in batch:
                    message.ack()
