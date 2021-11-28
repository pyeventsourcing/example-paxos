from dataclasses import dataclass
from typing import Dict, List, Optional
from uuid import NAMESPACE_URL, UUID, uuid5

from eventsourcing.domain import Aggregate, event


@dataclass
class AppliesTo:
    aggregate_id: UUID
    aggregate_version: Optional[int]


@dataclass
class KVProposal:
    cmd_text: str
    applies_to: AppliesTo


@dataclass
class PaxosProposal:
    key: UUID
    value: KVProposal


class TypeMismatchError(Exception):
    pass


class KVAggregate(Aggregate):

    TYPE_HASH = "hash"

    @classmethod
    def create(cls, id: UUID, key_name: str):
        return cls._create(event_class=cls.Created, id=id, key_name=key_name)

    class Created(Aggregate.Created["KVAggregate"]):
        key_name: str

    def __init__(self, key_name):
        self.key_name = key_name
        self.type_name: Optional[str] = None
        self.hash = {}

    @staticmethod
    def create_id(key_name: str) -> UUID:
        return uuid5(NAMESPACE_URL, f"/keys/{key_name}")

    def get_field_value(self, field_name):
        return self.hash.get(field_name, None)

    @event("FieldValueSet")
    def set_field_value(self, field_name, field_value):
        self.assert_hash()
        self.hash[field_name] = field_value

    @event("FieldValueDeleted")
    def del_field_value(self, field_name):
        self.assert_hash()
        del self.hash[field_name]

    def assert_hash(self):
        self.assert_type(self.TYPE_HASH)

    def assert_type(self, type_name):
        if self.type_name is None:
            self.type_name = type_name
        elif self.type_name != type_name:
            raise TypeMismatchError(f"Excepted {type_name} but is {self.type_name}")

