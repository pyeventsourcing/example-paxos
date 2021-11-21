from uuid import NAMESPACE_URL, uuid5

from eventsourcing.domain import Aggregate, event


class KVAggregate(Aggregate):
    def __init__(self, key_name):
        self.key_name = key_name

    @staticmethod
    def create_id(key_name):
        return uuid5(NAMESPACE_URL, f"/keys/{key_name}")

    @staticmethod
    def create_paxos_id(aggregate_id, aggregate_version):
        return uuid5(NAMESPACE_URL, f"/paxos/{aggregate_id}/{aggregate_version}")


class HashAggregate(KVAggregate):
    def __init__(self, key_name):
        super().__init__(key_name)
        self.hash = {}

    def get_field_value(self, field_name):
        return self.hash.get(field_name, None)

    @event("FieldValueSet")
    def set_field_value(self, field_name, field_value):
        self.hash[field_name] = field_value
