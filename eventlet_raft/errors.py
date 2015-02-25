class BaseRaftError(Exception):
    message = "General Eventlet-Raft error"

    def __init__(self, *args, **kwargs):
        super(BaseRaftError, self).__init__(self.message.format(**kwargs))


class DiskJournalFileDoesNotExist(BaseRaftError):
    message = 'Disk journal file "{file_path}" does not exist'


class DiskJournalFileCrushed(BaseRaftError):
    message = 'Disk journal file crushed because of {reason}'


class StateMachineKeyDoesNotExist(BaseRaftError):
    message = 'The StateMachineKey "{key}" does not exist'
