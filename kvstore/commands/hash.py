from decimal import Decimal
from typing import Optional, cast

from eventsourcing.application import AggregateNotFound

from kvstore.application import KVStore
from kvstore.commands.base import OperationOnKey
from kvstore.domainmodel import (
    AppliesTo,
    HashAggregate,
    KVAggregate,
    create_kv_aggregate_id,
)


class HashCommand(OperationOnKey):
    @staticmethod
    def get_hash_aggregate(
        app: KVStore, applies_to: AppliesTo
    ) -> Optional[HashAggregate]:
        return cast(
            Optional[HashAggregate], OperationOnKey.get_aggregate(app, applies_to)
        )

    def create_hash_aggregate(self, app: KVStore) -> HashAggregate:
        aggregate_id = create_kv_aggregate_id(self.key_name)
        aggregate = HashAggregate.create(aggregate_id, self.key_name)
        app.repository.cache.put(aggregate_id, aggregate)
        return aggregate

    def get_or_create_hash_aggregate(
        self, app: KVStore, applies_to: AppliesTo
    ) -> HashAggregate:
        return self.get_hash_aggregate(app, applies_to) or self.create_hash_aggregate(
            app
        )


class HSETCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    @property
    def field_value(self) -> str:
        return self.cmd[3]

    def execute(self, app: KVStore, applies_to: AppliesTo) -> KVAggregate:
        aggregate = self.get_or_create_hash_aggregate(app, applies_to)
        aggregate.set_field_value(self.field_name, self.field_value)
        return aggregate


class HSETNXCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    @property
    def field_value(self) -> str:
        return self.cmd[3]

    def execute(self, app: KVStore, applies_to: AppliesTo) -> KVAggregate:
        aggregate = self.get_or_create_hash_aggregate(app, applies_to)
        if self.field_name not in aggregate.hash:
            aggregate.set_field_value(self.field_name, self.field_value)
        return aggregate


class HINCRBYCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    @property
    def incr_by(self) -> Decimal:
        return Decimal(self.cmd[3])

    def execute(self, app: KVStore, applies_to: AppliesTo) -> KVAggregate:
        aggregate = self.get_or_create_hash_aggregate(app, applies_to)
        aggregate.set_field_value(
            self.field_name,
            str(
                self.incr_by + Decimal(aggregate.get_field_value(self.field_name) or 0)
            ),
        )
        return aggregate


class HDELCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    def execute(self, app: KVStore, applies_to: AppliesTo) -> KVAggregate:
        aggregate = self.get_hash_aggregate(app, applies_to)
        if aggregate is not None:
            try:
                aggregate.del_field_value(self.field_name)
            except KeyError:
                pass
        return aggregate


class HGETCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    def do_query(self, app: KVStore):
        aggregate_id = create_kv_aggregate_id(self.key_name)
        try:
            aggregate = app.repository.get(aggregate_id)
        except AggregateNotFound:
            return None
        else:
            assert isinstance(aggregate, HashAggregate), aggregate
            return aggregate.get_field_value(self.field_name)
