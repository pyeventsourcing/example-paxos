from typing import Optional
from uuid import NAMESPACE_URL, UUID, uuid5

from eventsourcing.domain import Aggregate, event

from kvstore.exceptions import TypeMismatchError


class KVAggregate(Aggregate):

    TYPE_HASH = "hash"

    def __init__(self, id: UUID, key_name: str):
        self._id = id
        self.key_name = key_name
        self.type_name: Optional[str] = None
        self.hash = {}

    class Created(Aggregate.Created["KVAggregate"]):
        key_name: str

    @event("Renamed")
    def rename(self, new_key_name: str):
        self.key_name = new_key_name

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


class KVIndex(Aggregate):
    def __init__(self, key_name: str, ref: Optional[UUID]):
        self.key_name = key_name
        self.ref = ref

    @staticmethod
    def create_id(key_name: str) -> UUID:
        return uuid5(NAMESPACE_URL, f"/keys/{key_name}")

    @event("RefUpdated")
    def update_ref(self, new_ref: Optional[UUID]):
        self.ref = new_ref
