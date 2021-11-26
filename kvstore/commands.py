from decimal import Decimal
from typing import Optional, cast
from uuid import UUID

from eventsourcing.application import AggregateNotFound

from kvstore.application import Command, KVStore
from kvstore.domainmodel import (
    AppliesTo,
    HashAggregate,
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


class HashCommand(OperationOnKey):
    @staticmethod
    def get_hash_aggregate(
        app: KVStore, applies_to: AppliesTo
    ) -> Optional[HashAggregate]:
        return cast(
            Optional[HashAggregate], OperationOnKey.get_aggregate(app, applies_to)
        )

    def create_hash_aggregate(self, app: KVStore) -> HashAggregate:
        aggregate_id = create_kv_aggregate_id(self.key_name)
        aggregate = HashAggregate.create(aggregate_id, self.key_name)
        app.repository.cache.put(aggregate_id, aggregate)
        return aggregate

    def get_or_create_hash_aggregate(
        self, app: KVStore, applies_to: AppliesTo
    ) -> HashAggregate:
        return self.get_hash_aggregate(app, applies_to) or self.create_hash_aggregate(
            app
        )


class HSETCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    @property
    def field_value(self) -> str:
        return self.cmd[3]

    def execute(self, app: KVStore, applies_to: AppliesTo) -> KVAggregate:
        aggregate = self.get_or_create_hash_aggregate(app, applies_to)
        aggregate.set_field_value(self.field_name, self.field_value)
        return aggregate


class HSETNXCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    @property
    def field_value(self) -> str:
        return self.cmd[3]

    def execute(self, app: KVStore, applies_to: AppliesTo) -> KVAggregate:
        aggregate = self.get_or_create_hash_aggregate(app, applies_to)
        if self.field_name not in aggregate.hash:
            aggregate.set_field_value(self.field_name, self.field_value)
        return aggregate


class HINCRBYCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    @property
    def incr_by(self) -> Decimal:
        return Decimal(self.cmd[3])

    def execute(self, app: KVStore, applies_to: AppliesTo) -> KVAggregate:
        aggregate = self.get_or_create_hash_aggregate(app, applies_to)
        aggregate.set_field_value(
            self.field_name,
            str(
                self.incr_by + Decimal(aggregate.get_field_value(self.field_name) or 0)
            ),
        )
        return aggregate


class HDELCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    def execute(self, app: KVStore, applies_to: AppliesTo) -> KVAggregate:
        aggregate = self.get_hash_aggregate(app, applies_to)
        if aggregate is not None:
            try:
                aggregate.del_field_value(self.field_name)
            except KeyError:
                pass
        return aggregate


class HGETCommand(HashCommand):
    @property
    def field_name(self) -> str:
        return self.cmd[2]

    def do_query(self, app: KVStore):
        aggregate_id = create_kv_aggregate_id(self.key_name)
        try:
            aggregate = app.repository.get(aggregate_id)
        except AggregateNotFound:
            return None
        else:
            assert isinstance(aggregate, HashAggregate), aggregate
            return aggregate.get_field_value(self.field_name)
