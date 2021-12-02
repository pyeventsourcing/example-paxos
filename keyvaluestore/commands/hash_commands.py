from decimal import Decimal
from typing import Any, Tuple

from eventsourcing.application import AggregateNotFound
from eventsourcing.domain import Aggregate

from replicatedstatemachine.application import StateMachineReplica
from keyvaluestore.commands import KeyValueStoreCommand
from keyvaluestore.domainmodel import (
    KeyNameIndex,
)


class HashCommand(KeyValueStoreCommand):
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
        index_id = KeyNameIndex.create_id(self.key_name)
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
