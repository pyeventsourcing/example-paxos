from typing import List

from eventsourcing.persistence import Transcoding

from kvstore.domainmodel import AppliesTo, KVProposal


class KVProposalAsList(Transcoding):
    name = "k_v_proposal"
    type = KVProposal

    def encode(self, obj: KVProposal) -> List:
        return [obj.cmd_text, obj.applies_to]

    def decode(self, data: List) -> KVProposal:
        return KVProposal(*data)


class AppliesToAsList(Transcoding):
    name = "applies_to"
    type = AppliesTo

    def encode(self, obj: AppliesTo) -> List:
        return [obj.aggregate_id, obj.aggregate_version]

    def decode(self, data: List) -> AppliesTo:
        return AppliesTo(*data)
