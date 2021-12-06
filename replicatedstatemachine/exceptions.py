class CommandRejected(Exception):
    pass


class CommandFutureEvicted(Exception):
    pass


class CommandExecutionError(Exception):
    pass


class PaxosProtocolError(Exception):
    pass
