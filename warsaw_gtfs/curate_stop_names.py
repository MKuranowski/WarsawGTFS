from typing import LiteralString, NotRequired, TypedDict, cast

from impuls import Task, TaskRuntime
from impuls.tools.types import SQLNativeType


class CuratedName(TypedDict):
    name: str
    stem: NotRequired[str]
    town: NotRequired[str]
    zone: NotRequired[str]


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
                new_zone = data.get("zone")

                sql: list[LiteralString] = ["UPDATE stops SET name = ?"]
                args: list[SQLNativeType] = [new_name]

                if new_town:
                    sql.append(
                        ", extra_fields_json = json_replace(extra_fields_json, "
                        "                                 '$.stop_name_stem', ?, "
                        "                                 '$.town_name', ?)"
                    )
                    args.append(new_stem)
                    args.append(new_town)
                else:
                    sql.append(
                        ", extra_fields_json = json_replace(extra_fields_json, "
                        "                                   '$.stop_name_stem', ?)"
                    )
                    args.append(new_stem)

                if new_zone:
                    sql.append(", zone_id = ?")
                    args.append(new_zone)

                sql.append(" WHERE SUBSTR(stop_id, 1, 4) = ?")
                args.append(stop_id_prefix)

                result = r.db.raw_execute("".join(sql), args)
                if result.rowcount == 0:
                    self.logger.warning("No stops in group %s, curation skipped", stop_id_prefix)
                else:
                    curated += result.rowcount

        self.logger.info("Curated %d stops", curated)
