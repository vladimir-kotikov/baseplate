from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sys
import kombu

from ... import config


def connection_from_config(app_config, prefix="rabbit."):
    assert prefix.endswith(".")
    config_prefix = prefix[:-1]
    cfg = config.parse_config(app_config, {
        config_prefix: {
            # TODO: config.Endpoint?
            "connection_url": config.String,
        },
    })

    url = getattr(cfg, config_prefix).connection_url

    return kombu.Connection(url)


def queue_from_config(app_config, prefix="rabbit."):
    assert prefix.endswith(".")
    config_prefix = prefix[:-1]
    # TODO: other config parameters
    cfg = config.parse_config(app_config, {
        config_prefix: {
            "queue_name": config.String,
            "queue_durable": config.Optional(config.Boolean, False),
            "queue_exclusive": config.Optional(config.Boolean, False),
        },
    })

    options = getattr(cfg, config_prefix)
    # get all prefs, prefixed with queue_ and build Queue kwargs
    remapped_opts = [(k.replace("queue_", ""), v)
                     for k, v in options.items()
                     if k.startswith("queue")]

    options = dict(remapped_opts)
    return make_queue(**options)


def make_queue(*args, **kwargs):
    return kombu.Queue(*args, **kwargs)


def exchange_from_config(app_config, prefix="rabbit."):
    assert prefix.endswith(".")
    config_prefix = prefix[:-1]
    cfg = config.parse_config(app_config, {
        config_prefix: {
            "exchange_name": config.String,
            # TODO: enum?
            "exchange_type": config.String,
        },
    })

    options = getattr(cfg, config_prefix)
    return make_exchange(**options)


def make_exchange(*args, **kwargs):
    return kombu.Exchange(*args, **kwargs)


class MessageContext(object):
    pass


class BaseplateConsumer(kombu.Consumer):
    """kombu's Consumer extension for baseplate.

    :param kombu.ChannelT channel: The channel to use for this consumer.
    :param baseplate.core.Baseplate baseplate: The baseplate instance for your
        application.
    :param str name: A name to identify the the consumer in trace info.

    """
    def __init__(self, channel, baseplate, name=None, **kwargs):
        self.baseplate = baseplate
        super(BaseplateConsumer, self).__init__(channel, **kwargs)
        self.name = name or "baseplate"

    def receive(self, body, message):
        context = MessageContext()
        # TODO: build trace_info from upstream
        with self.baseplate.make_server_span(context, self.name) as span:
            # TODO: Add appropriate headers to trace
            context.trace = span

            for callback in self.callbacks:
                # Pass baseplate context as an argument
                callback(body, message, context)


class BaseplateConsumerFactory(object):
    """
    Consumer factory class for injecting dependencies into BaseplateConsumer
        during application construction.

    :param object handler: Handler object which must expose 'get_callbacks'
        method returning a list of callback to be called when new message
        arrives.
    :param baseplate.core.Baseplate baseplate: The baseplate instance for your
        application.
    :param str name: A name to identify the the consumer handling the incoming
        message in trace info. If not specified, handler's class name will be
        used.
    """
    def __init__(self, handler, baseplate, name=None):
        self.handler = handler
        self.__callbacks = handler.get_callbacks()
        assert self.__callbacks, "At least one callback must be specified"

        self.baseplate = baseplate
        # If name is not specified - use handler's class name
        self.name = name or handler.__class__.__name__

    def get_consumers(self, channel, queues):
        consumer = BaseplateConsumer(
            channel,
            self.baseplate,
            queues=queues,
            callbacks=self.__callbacks,
            name=self.name)

        return [consumer]
