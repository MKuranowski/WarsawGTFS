from impuls import Task, TaskRuntime


class SetFeedVersion(Task):
    def __init__(self, feed_version: str) -> None:
        super().__init__()
        self.feed_version = feed_version

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            r.db.raw_execute("UPDATE feed_info SET version = ?", (self.feed_version,))
