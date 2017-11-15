import kombu

from thrift.util.Serializer import serialize, deserialize
from thrift.protocol.TBinaryProtocol import TBinaryProtocolFactory


class ThriftSerializer(object):
    """Thrift object serializer for Kombu."""
    def __init__(self, clazz):
        self.clazz = clazz
        self.name = 'thrift_serializer_%s' % (self.clazz.__name__)

    def serialize(self, obj):
        err_msg = "object to serialize must be of {} type"\
            .format(self.clazz.__name__)

        assert obj.__class__ is self.clazz, err_msg
        return serialize(TBinaryProtocolFactory(), obj)

    def deserialize(self, message):
        return deserialize(TBinaryProtocolFactory(), message, self.clazz())

    def register(self):
        kombu.serialization.register(
            self.name,
            self.serialize,
            self.deserialize,
            content_type='application/x-thrift',
            content_encoding='binary',
        )

__all__=["ThriftSerializer"]
