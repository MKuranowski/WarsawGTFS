from impuls import Task, TaskRuntime


class LoadJSON(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        raise NotImplementedError
