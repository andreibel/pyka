from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Record(_message.Message):
    __slots__ = ("offset", "timestamp", "key", "value")
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    offset: int
    timestamp: int
    key: bytes
    value: bytes
    def __init__(self, offset: _Optional[int] = ..., timestamp: _Optional[int] = ..., key: _Optional[bytes] = ..., value: _Optional[bytes] = ...) -> None: ...

class ProduceRequest(_message.Message):
    __slots__ = ("topic", "key", "value", "timestamp", "correlation_id")
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    topic: str
    key: bytes
    value: bytes
    timestamp: int
    correlation_id: int
    def __init__(self, topic: _Optional[str] = ..., key: _Optional[bytes] = ..., value: _Optional[bytes] = ..., timestamp: _Optional[int] = ..., correlation_id: _Optional[int] = ...) -> None: ...

class ProduceResponse(_message.Message):
    __slots__ = ("partition", "offset", "correlation_id")
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    partition: int
    offset: int
    correlation_id: int
    def __init__(self, partition: _Optional[int] = ..., offset: _Optional[int] = ..., correlation_id: _Optional[int] = ...) -> None: ...

class ConsumeRequest(_message.Message):
    __slots__ = ("topic", "partition", "offset", "follow", "max_records")
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    FOLLOW_FIELD_NUMBER: _ClassVar[int]
    MAX_RECORDS_FIELD_NUMBER: _ClassVar[int]
    topic: str
    partition: int
    offset: int
    follow: bool
    max_records: int
    def __init__(self, topic: _Optional[str] = ..., partition: _Optional[int] = ..., offset: _Optional[int] = ..., follow: _Optional[bool] = ..., max_records: _Optional[int] = ...) -> None: ...

class MetadataRequest(_message.Message):
    __slots__ = ("topics",)
    TOPICS_FIELD_NUMBER: _ClassVar[int]
    topics: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, topics: _Optional[_Iterable[str]] = ...) -> None: ...

class MetadataResponse(_message.Message):
    __slots__ = ("topics", "broker_count", "broker_id")
    TOPICS_FIELD_NUMBER: _ClassVar[int]
    BROKER_COUNT_FIELD_NUMBER: _ClassVar[int]
    BROKER_ID_FIELD_NUMBER: _ClassVar[int]
    topics: _containers.RepeatedCompositeFieldContainer[TopicMetadata]
    broker_count: int
    broker_id: int
    def __init__(self, topics: _Optional[_Iterable[_Union[TopicMetadata, _Mapping]]] = ..., broker_count: _Optional[int] = ..., broker_id: _Optional[int] = ...) -> None: ...

class TopicMetadata(_message.Message):
    __slots__ = ("name", "partitions")
    NAME_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    name: str
    partitions: _containers.RepeatedCompositeFieldContainer[PartitionMetadata]
    def __init__(self, name: _Optional[str] = ..., partitions: _Optional[_Iterable[_Union[PartitionMetadata, _Mapping]]] = ...) -> None: ...

class PartitionMetadata(_message.Message):
    __slots__ = ("partition", "broker", "address")
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    BROKER_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    partition: int
    broker: int
    address: str
    def __init__(self, partition: _Optional[int] = ..., broker: _Optional[int] = ..., address: _Optional[str] = ...) -> None: ...
