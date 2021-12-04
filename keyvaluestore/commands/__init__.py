import shlex
from typing import Any, List, Optional, Tuple, cast
from uuid import UUID, uuid4

from eventsourcing.domain import Aggregate

from replicatedstatemachine.application import Command, StateMachineReplica
from keyvaluestore.domainmodel import KeyValueAggregate, KeyNameIndex


class KeyValueStoreCommand(Command):
    @classmethod
    def parse(cls, cmd_text: str) -> "KeyValueStoreCommand":
        cmd = split(cmd_text)
        command_class = globals()[cmd[0].upper() + "Command"]
        return command_class(cmd)

    def __init__(self, cmd: List[str]):
        self.cmd = cmd

    @property
    def key_name(self) -> str:
        return self.cmd[1]

    def do_query(self, app: StateMachineReplica) -> Any:
        pass

    def execute(self, app: StateMachineReplica) -> Tuple[Tuple[Aggregate, ...], Any]:
        pass

    def resolve_key_name(
        self, app: StateMachineReplica, key_name
    ) -> Tuple[KeyValueAggregate, KeyNameIndex]:
        index = self.get_kv_index(app, key_name)
        if index is None:
            kv_aggregate = KeyValueAggregate(uuid4(), key_name)
            index = KeyNameIndex(key_name, kv_aggregate.id)
        else:
            kv_aggregate = self.get_kv_aggregate(app, index.ref)
        return kv_aggregate, index

    def get_kv_index(self, app, key_name: str) -> Optional[KeyNameIndex]:
        index_id = KeyNameIndex.create_id(key_name)
        try:
            return cast(
                KeyNameIndex,
                app.repository.get(
                    index_id,
                ),
            )
        except AggregateNotFound:
            return None

    def get_kv_aggregate(self, app, aggregate_id: UUID) -> Optional[KeyValueAggregate]:
        try:
            aggregate = cast(
                KeyValueAggregate,
                app.repository.get(
                    aggregate_id,
                ),
            )
        except AggregateNotFound:
            aggregate = None
        return aggregate


class RENAMECommand(KeyValueStoreCommand):
    @property
    def new_key_name(self) -> str:
        return self.cmd[2]

    def execute(self, app: StateMachineReplica) -> Tuple[Tuple[Aggregate, ...], Any]:
        kv_aggregate, index = self.resolve_key_name(app, self.key_name)
        if kv_aggregate is not None:
            old_index = self.get_kv_index(app, self.key_name)
            old_index.update_ref(None)
            new_index = self.get_kv_index(app, self.new_key_name)
            if new_index is None:
                new_index = KeyNameIndex(self.new_key_name, kv_aggregate.id)
            new_index.update_ref(kv_aggregate.id)
            kv_aggregate.rename(self.new_key_name)
            return (kv_aggregate, old_index, new_index), "OK"


def split(text: str) -> List[str]:
    return shlex.split(text)


from .hash_commands import *
