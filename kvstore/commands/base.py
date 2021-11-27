from typing import Optional, cast
from uuid import UUID

from eventsourcing.application import AggregateNotFound

from kvstore.application import Command, KVStore
from kvstore.domainmodel import (
    AppliesTo,
    KVAggregate,
    KVProposal,
    PaxosProposal,
    create_kv_aggregate_id,
    create_paxos_aggregate_id,
)
from kvstore.exceptions import AggregateVersionMismatch


class OperationOnKey(Command):
    @property
    def key_name(self) -> str:
        return self.cmd[1]

    def create_paxos_proposal(self, app: KVStore):
        aggregate_id = create_kv_aggregate_id(self.key_name)
        aggregate_version = self.get_current_version(app, aggregate_id)
        proposal_key = create_paxos_aggregate_id(aggregate_id, aggregate_version)
        proposal_value = KVProposal(
            cmd=self.cmd,
            applies_to=AppliesTo(
                aggregate_id=aggregate_id,
                aggregate_version=aggregate_version,
            ),
        )
        return PaxosProposal(proposal_key, proposal_value)

    @staticmethod
    def get_current_version(app: KVStore, aggregate_id: UUID) -> Optional[UUID]:
        try:
            last_event = next(
                app.repository.event_store.get(aggregate_id, desc=True, limit=1)
            )
        except StopIteration:
            aggregate_version = None
        else:
            aggregate_version = last_event.originator_version
        return aggregate_version

    @staticmethod
    def get_aggregate(app: KVStore, applies_to: AppliesTo) -> Optional[KVAggregate]:
        try:
            aggregate = cast(KVAggregate, app.repository.get(applies_to.aggregate_id))
        except AggregateNotFound:
            if applies_to.aggregate_version is not None:
                raise AggregateVersionMismatch(
                    "Proposal applies to aggregate "
                    f"ID {applies_to.aggregate_id} "
                    f"version {applies_to.aggregate_version} "
                    "but aggregate not found in repository"
                )
            else:
                return None
        else:
            if applies_to.aggregate_version != aggregate.version:
                raise AggregateVersionMismatch(
                    "Proposal applies to aggregate "
                    f"ID {applies_to.aggregate_id} "
                    f"version {applies_to.aggregate_version} "
                    "but aggregate in repository has "
                    f"version {aggregate.version}"
                )
            else:
                return aggregate
