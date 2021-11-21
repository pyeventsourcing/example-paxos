import itertools
from typing import Any, Dict, List, Type, cast

from eventsourcing.application import AggregateNotFound, ProcessEvent
from eventsourcing.domain import Aggregate, AggregateEvent, TAggregate
from eventsourcing.persistence import IntegrityError
from eventsourcing.system import System
from eventsourcing.utils import get_topic, resolve_topic

from kvstore.domainmodel import HashAggregate, KVAggregate
from paxos.application import PaxosApplication
from paxos.domainmodel import PaxosAggregate


class Command:
    def __init__(self, args):
        self.args = args

    def do_command(self, app: "KVStore"):
        pass

    def do_query(self, app: "KVStore"):
        pass

    def finalize(self, app: "KVStore", final_value) -> KVAggregate:
        pass


class CommandRejected(Exception):
    pass


class HSETCommand(Command):
    def do_command(self, app: "KVStore"):
        key_name = self.args[0]
        aggregate_id = KVAggregate.create_id(key_name)

        try:
            aggregate_version = app.repository.get(aggregate_id).version
        except AggregateNotFound:
            aggregate_version = None

        paxos_key = KVAggregate.create_paxos_id(aggregate_id, aggregate_version)

        paxos_value = {
            "cmd": {
                "topic": get_topic(type(self)),
                "args": self.args,
            },
            "applies_to": {
                "aggregate_id": aggregate_id,
                "aggregate_version": aggregate_version,
            }
        }
        try:
            app.propose_value(paxos_key, paxos_value, assume_leader=True)
        except IntegrityError as e:
            raise CommandRejected from e

    def finalize(self, app: "KVStore", final_value) -> KVAggregate:
        aggregate_id = final_value["applies_to"]["aggregate_id"]
        aggregate_version = final_value["applies_to"]["aggregate_version"]
        if aggregate_version:
            kv_aggregate = cast(
                KVAggregate, app.repository.get(aggregate_id, version=aggregate_version)
            )
        else:
            kv_aggregate = HashAggregate(self.args[0])

        kv_aggregate.set_field_value(self.args[1], self.args[2][1:-1])
        return kv_aggregate


class HGETCommand(Command):
    def do_query(self, app: "KVStore"):
        key_name = self.args[0]
        field_name = self.args[1]
        aggregate_id = KVAggregate.create_id(key_name)

        try:
            aggregate = app.repository.get(aggregate_id)
        except AggregateNotFound:
            return None
        else:
            assert isinstance(aggregate, HashAggregate)
            return aggregate.get_field_value(field_name)


cmd_classes: Dict[str, Type[Command]] = {
    "HSET": HSETCommand,
    "HGET": HGETCommand,
}


class KVStore(PaxosApplication):
    snapshotting_intervals = {
        PaxosAggregate: 1,
        HashAggregate: 1,
    }

    def do_command(self, cmd_text: str):
        cmd_parts = cmd_text.split(" ")
        cmd_class = cmd_classes[cmd_parts[0]]
        cmd = cmd_class(cmd_parts[1:])
        cmd.do_command(self)
        return cmd.do_query(self)

    def policy(
        self,
        domain_event: AggregateEvent[TAggregate],
        process_event: ProcessEvent,
    ) -> None:
        """
        Processes paxos "message announced" events of other applications
        by starting or continuing a paxos aggregate in this application.
        """
        # print(self.__class__.__name__, domain_event)
        if isinstance(domain_event, PaxosAggregate.MessageAnnounced):
            paxos_aggregate, resolution_msg = self.process_message_announced(
                domain_event
            )
            process_event.save(paxos_aggregate)
            if resolution_msg:
                cmd_topic = paxos_aggregate.final_value["cmd"]["topic"]
                cmd_args = paxos_aggregate.final_value["cmd"]["args"]
                cmd_class: Type[Command] = resolve_topic(cmd_topic)
                cmd = cmd_class(cmd_args)
                kv_aggregate = cmd.finalize(self, paxos_aggregate.final_value)
                process_event.save(kv_aggregate)

    def notify(self, new_events: List[AggregateEvent[Aggregate]]) -> None:
        """
        Extends the application :func:`~eventsourcing.application.Application.notify`
        method by calling :func:`prompt_followers` whenever new events have just
        been saved.
        """
        for new_event in new_events:
            if isinstance(new_event, PaxosAggregate.MessageAnnounced):
                self.prompt_followers()
                return


class KVSystem(System):
    def __init__(self, num_participants: int = 3, **kwargs: Any):
        self.num_participants = num_participants
        self.quorum_size = (num_participants + 2) // 2
        classes = [
            type(
                "KVStore{}".format(i),
                (KVStore,),
                {"quorum_size": self.quorum_size},
            )
            for i in range(num_participants)
        ]
        assert num_participants > 1
        pipes = [[c[0], c[1], c[0]] for c in itertools.combinations(classes, 2)]
        super(KVSystem, self).__init__(pipes)
