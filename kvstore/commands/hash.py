from decimal import Decimal
from typing import Any, Optional, Tuple

from eventsourcing.application import AggregateNotFound
from eventsourcing.domain import Aggregate

from replicatedstatemachine.application import StateMachineReplica
from kvstore.commands import KVCommand
from kvstore.domainmodel import (
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

    def execute(self, app: StateMachineReplica) -> Tuple[Aggregate, ...]:
        kv_aggregate, index = self.resolve_key_name(app, self.key_name)
        kv_aggregate.set_field_value(self.field_name, self.field_value)
        return kv_aggregate, index


class HSETNXCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    @property
    def field_value(self) -> str:
        return self.cmd[3]

    def execute(self, app: StateMachineReplica) -> Tuple[Aggregate, ...]:
        kv_aggregate, index = self.resolve_key_name(app, self.key_name)
        if self.field_name not in kv_aggregate.hash:
            kv_aggregate.set_field_value(self.field_name, self.field_value)
        return kv_aggregate, index


class HINCRBYCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    @property
    def incr_by(self) -> Decimal:
        return Decimal(self.cmd[3])

    def execute(self, app: StateMachineReplica) -> Tuple[Aggregate, ...]:
        kv_aggregate, index = self.resolve_key_name(app, self.key_name)
        kv_aggregate.set_field_value(
            self.field_name,
            str(
                self.incr_by
                + Decimal(kv_aggregate.get_field_value(self.field_name) or 0)
            ),
        )
        return kv_aggregate, index


class HDELCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    def execute(self, app: StateMachineReplica) -> Tuple[Aggregate, ...]:
        kv_aggregate, index = self.resolve_key_name(app, self.key_name)
        if kv_aggregate is not None:
            try:
                kv_aggregate.del_field_value(self.field_name)
            except KeyError:
                pass
        return (kv_aggregate,)


class HGETCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    def do_query(self, app: StateMachineReplica) -> Any:
        index_id = KVIndex.create_id(self.key_name)
        try:
            index = app.repository.get(
                index_id,
            )
        except AggregateNotFound:
            return None
        if index.ref:
            aggregate_id = index.ref
            aggregate = self.get_kv_aggregate(app, aggregate_id)
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

    def execute(self, app: StateMachineReplica) -> Tuple[Aggregate, ...]:
        kv_aggregate, index = self.resolve_key_name(app, self.key_name)
        if kv_aggregate is not None:
            old_index = self.get_kv_index(app, self.old_field_name)
            old_index.update_ref(None)
            new_index = self.get_kv_index(app, self.new_field_name)
            if new_index is None:
                new_index = KVIndex(self.new_field_name, kv_aggregate.id)
            new_index.update_ref(kv_aggregate.id)
            kv_aggregate.rename(self.new_field_name)
            return kv_aggregate, old_index, new_index
