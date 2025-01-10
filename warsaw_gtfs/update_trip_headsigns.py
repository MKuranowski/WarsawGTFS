from typing import cast

from impuls import Task, TaskRuntime


class UpdateTripHeadsigns(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        trips = cast(
            list[tuple[str, str, str]],
            list(
                r.db.raw_execute(
                    "SELECT t.trip_id, s.stop_id, s.name "
                    "FROM trips AS t "
                    "LEFT JOIN stops AS s ON (s.stop_id = ("
                    "    SELECT st.stop_id"
                    "    FROM stop_times AS st"
                    "    WHERE st.trip_id = t.trip_id"
                    "    ORDER BY st.stop_sequence DESC"
                    "    LIMIT 1"
                    "))"
                )
            ),
        )

        with r.db.transaction():
            r.db.raw_execute_many(
                "UPDATE trips SET headsign = ? WHERE trip_id = ?",
                (
                    (self.get_headsign(last_stop_id, last_stop_name), trip_id)
                    for trip_id, last_stop_id, last_stop_name in trips
                ),
            )

    @staticmethod
    def get_headsign(last_stop_id: str, last_stop_name: str) -> str:
        if last_stop_id in {"503803", "503804"}:
            return "Zjazd do zajezdni Wola"
        elif last_stop_id == "103002":
            return "Zjazd do zajezdni Praga"
        elif last_stop_id == "324010":
            return "Zjazd do zajezdni Mokotów"
        elif last_stop_id in {"606107", "606108"}:
            return "Zjazd do zajezdni Żoliborz"
        elif last_stop_id == "108806":
            return "Zjazd do zajezdni Annopol"
        elif last_stop_id.startswith("4202"):
            return "Lotnisko Chopina"
        else:
            return last_stop_name
