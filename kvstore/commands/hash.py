from decimal import Decimal
from typing import Any

from kvstore.application import KVCommand, KVStore
from kvstore.domainmodel import (
    AppliesTo,
    KVAggregate,
)


class HashCommand(KVCommand):
    pass


class HSETCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    @property
    def field_value(self) -> str:
        return self.cmd[3]

    def execute(self, app: KVStore, applies_to: AppliesTo) -> KVAggregate:
        aggregate = app.get_or_create_kv_aggregate(self, applies_to.aggregate_id, applies_to.aggregate_version)
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
        aggregate = app.get_or_create_kv_aggregate(self, applies_to.aggregate_id, applies_to.aggregate_version)
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
        aggregate = app.get_or_create_kv_aggregate(self, applies_to.aggregate_id, applies_to.aggregate_version)
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
        aggregate = app.get_kv_aggregate(applies_to.aggregate_id, applies_to.aggregate_version)
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

    def do_query(self, app: KVStore) -> Any:
        aggregate_id = KVAggregate.create_id(self.key_name)
        aggregate = app.get_kv_aggregate(aggregate_id)
        if aggregate:
            return aggregate.get_field_value(self.field_name)
