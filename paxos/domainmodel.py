from copy import deepcopy
from typing import Any, Set, Dict
from uuid import UUID

from eventsourcing.domain import Aggregate, event

from paxos.composable import ProposalStatus, PaxosInstance, PaxosMessage, Resolution


class PaxosAggregate(Aggregate):
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

    def __init__(self, quorum_size: int, network_uid: str, **kwargs: Any):
        assert isinstance(quorum_size, int)
        self.quorum_size = quorum_size
        self.network_uid = network_uid
        self.promises_received: Set[str] = set()
        self.nacks_received: Set[str] = set()
        self.leader = False
        self.proposals: Dict[str, ProposalStatus] = {}
        self.acceptors: Dict[str, str] = {}
        self.final_value: Any = None
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

    class Started(Aggregate.Created["PaxosAggregate"]):
        """
        Published when a PaxosAggregate is started.
        """

        quorum_size: int
        network_uid: str

    class AttributesChanged(Aggregate.Event):
        """
        Published when attributes of paxos_instance are changed.
        """

        changes: Dict

        def apply(self, obj: "PaxosAggregate") -> None:
            obj.__dict__.update(self.changes)

    @classmethod
    def start(
        cls, originator_id: UUID, quorum_size: int, network_uid: str
    ) -> "PaxosAggregate":
        """
        Factory method that returns a new Paxos aggregate.
        """
        assert isinstance(quorum_size, int), "Not an integer: {}".format(quorum_size)
        return cls._create(
            event_class=cls.Started,
            id=originator_id,
            quorum_size=quorum_size,
            network_uid=network_uid,
        )

    def propose_value(self, value: Any, assume_leader: bool = False) -> PaxosMessage:
        """
        Proposes a value to the network.
        """
        assert value is not None, "Not allowed to propose value None"
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
        resolution_msg = None
        if not isinstance(msg, Resolution):
            paxos = self.paxos_instance
            while msg:
                if isinstance(msg, Resolution):
                    resolution_msg = msg
                    break
                else:
                    msg = paxos.receive(msg)
                    # Todo: Make it optional not to announce resolution
                    #  (without which it's hard to see final value).
                    do_announce_resolution = True
                    if msg and (
                        do_announce_resolution or not isinstance(msg, Resolution)
                    ):
                        self.announce(msg)

            self.setattrs_from_paxos(paxos)
        return resolution_msg

    class MessageAnnounced(Aggregate.Event):
        """
        Published when a Paxos message is announced.
        """

        msg: PaxosMessage

    # @event("MessageAnnounced")
    def announce(self, msg: PaxosMessage) -> None:
        """
        Announces a Paxos message.
        """
        self.trigger_event(event_class=self.MessageAnnounced, msg=msg)

    def setattrs_from_paxos(self, paxos: PaxosInstance) -> None:
        """
        Registers changes of attribute value on Paxos instance.
        """
        changes = {}
        for name in self.paxos_variables:
            paxos_value = getattr(paxos, name)
            if paxos_value != getattr(self, name, None):
                changes[name] = paxos_value
                setattr(self, name, paxos_value)
        if changes:
            self.trigger_event(event_class=self.AttributesChanged, changes=changes)
