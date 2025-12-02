from typing import NotRequired, TypedDict, cast

from impuls import Task, TaskRuntime


class CuratedName(TypedDict):
    name: str
    stem: NotRequired[str]
    town: NotRequired[str]


class CurateStopNames(Task):
    def __init__(self, resource: str) -> None:
        super().__init__()
        self.resource = resource

    def execute(self, r: TaskRuntime) -> None:
        to_curate = cast(dict[str, CuratedName], r.resources[self.resource].json()["names"])
        curated = 0

        with r.db.transaction():
            for stop_id_prefix, data in to_curate.items():
                new_name = data["name"]
                new_stem = data.get("stem", new_name)
                new_town = data.get("town")

                if new_town:
                    sql = (
                        "UPDATE stops SET name = ?, "
                        "extra_fields_json = json_replace(extra_fields_json, "
                        "                                 '$.stop_name_stem', ?, "
                        "                                 '$.town_name', ?) "
                        "WHERE SUBSTR(stop_id, 1, 4) = ?"
                    )
                    args = (new_name, new_stem, new_town, stop_id_prefix)
                else:
                    sql = (
                        "UPDATE stops SET name = ?, "
                        "extra_fields_json = json_replace(extra_fields_json, '$.stop_name_stem', ?) "
                        "WHERE SUBSTR(stop_id, 1, 4) = ?"
                    )
                    args = (new_name, new_stem, stop_id_prefix)

                result = r.db.raw_execute(sql, args)
                if result.rowcount == 0:
                    self.logger.warning("No stops in group %s, curation skipped", stop_id_prefix)
                else:
                    curated += result.rowcount

        self.logger.info("Curated %d stops", curated)
