from typing import Dict, List, Set

from eventsourcing.persistence import Transcoding

from paxossystem.composable import (
    Accept,
    Accepted,
    Nack,
    Prepare,
    Promise,
    ProposalID,
    ProposalStatus,
    Resolution,
)


class ObjAsDict(Transcoding):
    def encode(self, obj: object) -> Dict:
        return obj.__dict__

    def decode(self, data: Dict) -> object:
        return self.type(**data)


class ProposalIDAsDict(ObjAsDict):
    name = "proposal_id"
    type = ProposalID


class PrepareAsDict(ObjAsDict):
    name = "prepare"
    type = Prepare


class PromiseAsDict(ObjAsDict):
    name = "promise"
    type = Promise


class AcceptAsDict(ObjAsDict):
    name = "accept"
    type = Accept


class AcceptedAsDict(ObjAsDict):
    name = "accepted"
    type = Accepted


class NackAsDict(ObjAsDict):
    name = "nack"
    type = Nack


class ResolutionAsDict(ObjAsDict):
    name = "resolution"
    type = Resolution


class ProposalStatusAsDict(Transcoding):
    name = "proposal_status"
    type = ProposalStatus

    def encode(self, obj: ProposalStatus) -> Dict:
        return {
            "accept_count": obj.accept_count,
            "retain_count": obj.retain_count,
            "acceptors": obj.acceptors,
            "value": obj.value,
        }

    def decode(self, data: Dict) -> ProposalStatus:
        obj = ProposalStatus(data["value"])
        obj.accept_count = data["accept_count"]
        obj.retain_count = data["retain_count"]
        obj.acceptors = data["acceptors"]
        return obj


class SetAsList(Transcoding):
    name = "set"
    type = set

    def encode(self, obj: Set) -> List:
        return list(obj)

    def decode(self, data: List) -> Set:
        return set(data)
