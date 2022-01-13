class CommandError(Exception):
    pass


class CommandRejectedError(CommandError):
    pass


class CommandEvictedError(CommandError):
    pass


class CommandTimeoutError(CommandError):
    pass


class CommandExecutionError(CommandError):
    pass


class PaxosProtocolError(CommandError):
    pass
