from decimal import Decimal
from typing import Any, Optional, Tuple

from eventsourcing.application import AggregateNotFound

from kvstore.application import KVCommand, KVStore
from kvstore.domainmodel import (
    AppliesTo,
    KVAggregate,
    KVIndex,
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

    def execute(
        self, app: KVStore, applies_to: AppliesTo
    ) -> Tuple[KVAggregate, Optional[KVIndex], Optional[KVIndex]]:
        aggregate, index = app.get_or_create_kv_aggregate(self, applies_to)
        aggregate.set_field_value(self.field_name, self.field_value)
        return aggregate, index, None


class HSETNXCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    @property
    def field_value(self) -> str:
        return self.cmd[3]

    def execute(
        self, app: KVStore, applies_to: AppliesTo
    ) -> Tuple[KVAggregate, Optional[KVIndex], Optional[KVIndex]]:
        aggregate, index = app.get_or_create_kv_aggregate(self, applies_to)
        if self.field_name not in aggregate.hash:
            aggregate.set_field_value(self.field_name, self.field_value)
        return aggregate, index, None


class HINCRBYCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    @property
    def incr_by(self) -> Decimal:
        return Decimal(self.cmd[3])

    def execute(
        self, app: KVStore, applies_to: AppliesTo
    ) -> Tuple[KVAggregate, Optional[KVIndex], Optional[KVIndex]]:
        aggregate, index = app.get_or_create_kv_aggregate(self, applies_to)
        aggregate.set_field_value(
            self.field_name,
            str(
                self.incr_by + Decimal(aggregate.get_field_value(self.field_name) or 0)
            ),
        )
        return aggregate, index, None


class HDELCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    def execute(
        self, app: KVStore, applies_to: AppliesTo
    ) -> Tuple[KVAggregate, Optional[KVIndex], Optional[KVIndex]]:
        aggregate = app.get_and_validate_kv_aggregate(applies_to)
        if aggregate is not None:
            try:
                aggregate.del_field_value(self.field_name)
            except KeyError:
                pass
        return aggregate, None, None


class HGETCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    def do_query(self, app: KVStore) -> Any:
        index_id = KVIndex.create_id(self.key_name)
        try:
            index = app.repository.get(
                index_id,
            )
        except AggregateNotFound:
            return None
        if index.ref:
            aggregate_id = index.ref
            aggregate = app.get_kv_aggregate(aggregate_id)
            if aggregate:
                return aggregate.get_field_value(self.field_name)
        else:
            return None


class RENAMECommand(KVCommand):
    @property
    def old_field_name(self) -> str:
        return self.cmd[1]

    @property
    def new_field_name(self) -> str:
        return self.cmd[2]

    def execute(
        self, app: KVStore, applies_to: AppliesTo
    ) -> Tuple[KVAggregate, Optional[KVIndex], Optional[KVIndex]]:
        aggregate = app.get_and_validate_kv_aggregate(applies_to)
        if aggregate is not None:
            old_index = app.get_kv_index(self.old_field_name)
            old_index.update_ref(None)
            new_index = app.get_kv_index(self.new_field_name)
            if new_index is None:
                new_index = KVIndex(self.new_field_name, aggregate.id)
                # app.repository.cache.put(new_index.id, new_index)
            new_index.update_ref(aggregate.id)
            aggregate.rename(self.new_field_name)
            return aggregate, old_index, new_index
