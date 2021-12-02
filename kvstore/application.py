from kvstore.domainmodel import KVAggregate, KVIndex
from replicatedstatemachine.application import StateMachineReplica


class KVStore(StateMachineReplica):
    snapshotting_intervals = {
        KVAggregate: 100,
        KVIndex: 100,
    }
