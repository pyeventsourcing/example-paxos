from copy import deepcopy
from typing import Any, Set, Dict, Optional
from uuid import UUID

from eventsourcing.domain.model.aggregate import BaseAggregateRoot

from paxos.composable import ProposalStatus, PaxosInstance, PaxosMessage, Resolution


class PaxosAggregate(BaseAggregateRoot):
    """
    Event-sourced Paxos participant.
    """

    paxos_variables = [
        "proposed_value",
        "proposal_id",
        "promised_id",
        "accepted_id",
        "accepted_value",
        "highest_proposal_id",
        "promises_received",
        "nacks_received",
        "highest_accepted_id",
        "leader",
        "proposals",
        "acceptors",
        "final_value",
    ]
    is_verbose = False

    def __init__(self, quorum_size: int, network_uid: str, **kwargs: Any):
        assert isinstance(quorum_size, int)
        self.quorum_size = quorum_size
        self.network_uid = network_uid
        self.promises_received: Set[str] = set()
        self.nacks_received: Set[str] = set()
        self.leader = False
        self.proposals: Dict[str, ProposalStatus] = {}
        self.acceptors: Dict[str, str] = {}
        self.final_value = None
        super(PaxosAggregate, self).__init__(**kwargs)

    @property
    def paxos_instance(self) -> PaxosInstance:
        """
        Returns instance of PaxosInstance (protocol implementation).
        """
        # Construct instance with the constant attributes.
        instance = PaxosInstance(self.network_uid, self.quorum_size)

        # Set the variable attributes from the aggregate.
        for name in self.paxos_variables:
            value = getattr(self, name, None)
            if value is not None:
                if isinstance(value, (set, list, dict, tuple)):
                    value = deepcopy(value)
                setattr(instance, name, value)

        # Return the instance.
        return instance

    class Event(BaseAggregateRoot.Event["PaxosAggregate"]):
        """
        Base event class for PaxosAggregate.
        """

    class Started(Event, BaseAggregateRoot.Created["PaxosAggregate"]):
        """
        Published when a PaxosAggregate is started.
        """

        __notifiable__ = False

    class AttributesChanged(Event):
        """
        Published when attributes of paxos_instance are changed.
        """

        __notifiable__ = False

        def __init__(self, changes: Optional[Dict[str, Any]] = None, **kwargs: Any):
            super(PaxosAggregate.AttributesChanged, self).__init__(
                changes=changes, **kwargs
            )

        @property
        def changes(self) -> Dict[str, Any]:
            return self.__dict__["changes"]

        def mutate(self, obj: "PaxosAggregate") -> None:
            for name, value in self.changes.items():
                setattr(obj, name, value)

    @classmethod
    def start(
        cls, originator_id: UUID, quorum_size: int, network_uid: str
    ) -> "PaxosAggregate":
        """
        Factory method that returns a new Paxos aggregate.
        """
        assert isinstance(quorum_size, int), "Not an integer: {}".format(quorum_size)
        started_class = cls.Started
        return cls.__create__(
            originator_id=originator_id,
            event_class=started_class,
            quorum_size=quorum_size,
            network_uid=network_uid,
        )

    def propose_value(self, value: Any, assume_leader: bool = False) -> PaxosMessage:
        """
        Proposes a value to the network.
        """
        if value is None:
            raise ValueError("Not allowed to propose value None")
        paxos = self.paxos_instance
        paxos.leader = assume_leader
        msg = paxos.propose_value(value)
        if msg is None:
            msg = paxos.prepare()
        self.setattrs_from_paxos(paxos)
        self.announce(msg)
        return msg

    def receive_message(self, msg: PaxosMessage) -> None:
        """
        Responds to messages from other participants.
        """
        if isinstance(msg, Resolution):
            return
        paxos = self.paxos_instance
        while msg:
            if isinstance(msg, Resolution):
                self.print_if_verbose(
                    "{} resolved value {}".format(self.network_uid, msg.value)
                )
                break
            else:
                self.print_if_verbose(
                    "{} <- {} <- {}".format(
                        self.network_uid, msg.__class__.__name__, msg.from_uid
                    )
                )
                msg = paxos.receive(msg)
                # Todo: Make it optional not to announce resolution
                #  (without which it's hard to see final value).
                do_announce_resolution = True
                if msg and (do_announce_resolution or not isinstance(msg, Resolution)):
                    self.announce(msg)

        self.setattrs_from_paxos(paxos)

    def announce(self, msg: PaxosMessage) -> None:
        """
        Announces a Paxos message.
        """
        self.print_if_verbose(
            "{} -> {}".format(self.network_uid, msg.__class__.__name__)
        )
        self.__trigger_event__(event_class=self.MessageAnnounced, msg=msg)

    class MessageAnnounced(Event):
        """
        Published when a Paxos message is announced.
        """

        @property
        def msg(self) -> PaxosMessage:
            return self.__dict__["msg"]

    def setattrs_from_paxos(self, paxos: PaxosInstance) -> None:
        """
        Registers changes of attribute value on Paxos instance.
        """
        changes = {}
        for name in self.paxos_variables:
            paxos_value = getattr(paxos, name)
            if paxos_value != getattr(self, name, None):
                self.print_if_verbose(
                    "{} {}: {}".format(self.network_uid, name, paxos_value)
                )
                changes[name] = paxos_value
                setattr(self, name, paxos_value)
        if changes:
            self.__trigger_event__(event_class=self.AttributesChanged, changes=changes)

    def print_if_verbose(self, param: Any) -> None:
        if self.is_verbose:
            print(param)

    def __str__(self) -> str:
        return (
            "PaxosAggregate("
            "final_value='{final_value}', "
            "proposed_value='{proposed_value}', "
            "network_uid='{network_uid}', "
            "proposal_id='{proposal_id}', "
            "promised_id='{promised_id}', "
            "promises_received='{promises_received}'"
            ")"
        ).format(**self.__dict__)