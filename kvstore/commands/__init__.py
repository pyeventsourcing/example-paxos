import shlex
from typing import Any, List, Optional, Tuple, cast
from uuid import UUID, uuid4

from eventsourcing.application import AggregateNotFound
from eventsourcing.domain import Aggregate

from replicatedstatemachine.application import Command, StateMachineReplica
from kvstore.domainmodel import KVAggregate, KVIndex


class KVCommand(Command):
    @classmethod
    def parse(cls, cmd_text: str) -> "KVCommand":
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

    def execute(self, app: StateMachineReplica) -> Tuple[Aggregate, ...]:
        pass

    def resolve_key_name(
        self, app: StateMachineReplica, key_name
    ) -> Tuple[KVAggregate, KVIndex]:
        index = self.get_kv_index(app, key_name)
        if index is None:
            kv_aggregate = KVAggregate(uuid4(), key_name)
            index = KVIndex(key_name, kv_aggregate.id)
        else:
            kv_aggregate = self.get_kv_aggregate(app, index.ref)
        return kv_aggregate, index

    def get_kv_index(self, app, key_name: str) -> Optional[KVIndex]:
        index_id = KVIndex.create_id(key_name)
        try:
            return cast(
                KVIndex,
                app.repository.get(
                    index_id,
                ),
            )
        except AggregateNotFound:
            return None

    def get_kv_aggregate(self, app, aggregate_id: UUID) -> Optional[KVAggregate]:
        try:
            aggregate = cast(
                KVAggregate,
                app.repository.get(
                    aggregate_id,
                ),
            )
        except AggregateNotFound:
            aggregate = None
        return aggregate


def split(text: str) -> List[str]:
    return shlex.split(text)


from .hash import *
