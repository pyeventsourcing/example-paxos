from dataclasses import dataclass
from typing import List, Optional
from uuid import NAMESPACE_URL, UUID, uuid5

from eventsourcing.domain import Aggregate, event


def create_kv_aggregate_id(key_name: str):
    return uuid5(NAMESPACE_URL, f"/keys/{key_name}")


def create_paxos_aggregate_id(aggregate_id: UUID, aggregate_version: Optional[int]):
    return uuid5(NAMESPACE_URL, f"/proposals/{aggregate_id}/{aggregate_version}")


class KVAggregate(Aggregate):
    def __init__(self, key_name):
        self.key_name = key_name

    class Created(Aggregate.Created["KVAggregate"]):
        key_name: str

    @classmethod
    def create(cls, id: UUID, key_name: str):
        return cls._create(event_class=cls.Created, id=id, key_name=key_name)


class HashAggregate(KVAggregate):
    def __init__(self, key_name):
        super().__init__(key_name)
        self.hash = {}

    @staticmethod
    def create_id(key_name: str):
        return create_kv_aggregate_id(key_name)

    class Created(KVAggregate.Created):
        pass

    def get_field_value(self, field_name):
        return self.hash.get(field_name, None)

    @event("FieldValueSet")
    def set_field_value(self, field_name, field_value):
        self.hash[field_name] = field_value

    @event("FieldValueDeleted")
    def del_field_value(self, field_name):
        del self.hash[field_name]


@dataclass
class AppliesTo:
    aggregate_id: UUID
    aggregate_version: Optional[int]


@dataclass
class KVProposal:
    cmd: List[str]
    applies_to: AppliesTo


@dataclass
class PaxosProposal:
    key: UUID
    value: KVProposal