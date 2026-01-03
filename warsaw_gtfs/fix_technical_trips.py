from typing import cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import StopTime, Trip

def is_depot(stop_id: str) -> bool:
    return stop_id[-2:] == "00" or stop_id[-1] == "9"

class FixTechnicalTrips(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        self.split_depot_trips(r.db)
        self.merge_technical_trips(r.db)

    def split_depot_trips(self, db: DBConnection) -> None:
        self.logger.info("Searching for combined depot trips to split...")
        
        query = """
            SELECT st.trip_id, st.stop_sequence, st.stop_id, 
                   (s.extra_fields_json ->> 'depot' = '1')
            FROM stop_times st
            JOIN stops s ON st.stop_id = s.stop_id
            ORDER BY st.trip_id, st.stop_sequence
        """
        
        current_trip = None
        stops: list[tuple[int, str, bool]] = [] # (seq, stop_id, is_depot)
        candidates: list[tuple[str, int]] = [] 

        for row in db.raw_execute(query):
            t_id = cast(str, row[0])
            seq = cast(int, row[1])
            s_id = cast(str, row[2])
            is_depot = bool(row[3]) or is_depot(s_id)

            if t_id != current_trip:
                if current_trip:
                    idx = self._find_split_index(stops)
                    if idx is not None:
                        candidates.append((current_trip, idx))
                current_trip = t_id
                stops = []
            
            stops.append((seq, s_id, is_depot))
            
        if current_trip:
            idx = self._find_split_index(stops)
            if idx is not None:
                candidates.append((current_trip, idx))

        if not candidates:
            return

        with db.transaction():
            for trip_id, split_idx in candidates:
                self._perform_split(db, trip_id, split_idx)
        
        self.logger.info(f"Split {len(candidates)} depot trips.")

    def _find_split_index(self, stops: list[tuple[int, str, bool]]) -> int | None:
        if len(stops) < 2: return None

        for i in range(len(stops) - 1):
            curr_depot = stops[i][2]
            next_depot = stops[i+1][2]

            # Depot -> Route. Split BEFORE the first route stop.
            if curr_depot and not next_depot:
                return i + 1 

            # Route -> Depot. Split AT the last route stop.
            if not curr_depot and next_depot:
                return i 
                
        return None

    def _perform_split(self, db: DBConnection, trip_id: str, split_index: int):
        trip = db.typed_out_execute("SELECT * FROM trips WHERE trip_id = ?", Trip, (trip_id,)).one()
        stop_times = list(db.typed_out_execute(
            "SELECT * FROM stop_times WHERE trip_id = ? ORDER BY stop_sequence",
            StopTime, (trip_id,)
        ))
        
        if not trip or not stop_times: return

        pivot_st = stop_times[split_index]
        new_trip_id = f"{trip_id}:part2"
        
        new_trip = Trip(
            id=new_trip_id,
            route_id=trip.route_id,
            calendar_id=trip.calendar_id, 
            shape_id=trip.shape_id,
            headsign=trip.headsign,
            direction=trip.direction,
            block_id=trip.block_id,
            wheelchair_accessible=trip.wheelchair_accessible,
            bikes_allowed=trip.bikes_allowed,
            exceptional=trip.exceptional,
            extra_fields_json=trip.extra_fields_json
        )
        db.create(new_trip)

        to_move = stop_times[split_index+1:]
        if to_move:
            db.raw_execute_many(
                "UPDATE stop_times SET trip_id = ? WHERE trip_id = ? AND stop_sequence = ?",
                ((new_trip_id, trip_id, st.stop_sequence) for st in to_move)
            )
        
        pivot_copy = StopTime(
            trip_id=new_trip_id,
            stop_id=pivot_st.stop_id,
            stop_sequence=pivot_st.stop_sequence, 
            arrival_time=pivot_st.arrival_time,
            departure_time=pivot_st.departure_time,
            stop_headsign=pivot_st.stop_headsign,
            pickup_type=pivot_st.pickup_type,
            drop_off_type=pivot_st.drop_off_type,
            shape_dist_traveled=pivot_st.shape_dist_traveled,
            extra_fields_json=pivot_st.extra_fields_json
        )
        db.create(pivot_copy)

    def merge_technical_trips(self, db: DBConnection) -> None:
        self.logger.info("Merging technical trips...")

        query = """
            SELECT 
                t.trip_id, 
                t.calendar_id, 
                t.block_id,
                
                st_start.stop_id AS start_stop,
                (
                    SELECT is_not_available 
                    FROM variant_stops vs 
                    WHERE vs.variant_id = json_extract(st_start.extra_fields_json, '$.variant_id')
                      AND vs.stop_sequence = st_start.stop_sequence
                ) AS start_is_tech,
                
                st_end.stop_id AS end_stop,
                (
                    SELECT is_not_available 
                    FROM variant_stops vs 
                    WHERE vs.variant_id = json_extract(st_end.extra_fields_json, '$.variant_id')
                      AND vs.stop_sequence = st_end.stop_sequence
                ) AS end_is_tech
                
            FROM trips t
            JOIN stop_times st_start ON t.trip_id = st_start.trip_id
            JOIN stop_times st_end ON t.trip_id = st_end.trip_id
            WHERE t.block_id IS NOT NULL AND t.block_id != ''
              AND st_start.stop_sequence = (SELECT MIN(stop_sequence) FROM stop_times WHERE trip_id = t.trip_id)
              AND st_end.stop_sequence = (SELECT MAX(stop_sequence) FROM stop_times WHERE trip_id = t.trip_id)
            ORDER BY t.calendar_id, t.block_id, st_start.departure_time
        """
        
        cursor = db.raw_execute(query)
        
        merged_count = 0
        
        # prev_trip: [trip_id, calendar_id, block_id, start_stop, start_tech, end_stop, end_tech]
        prev_trip = None 
    
        with db.transaction():
            for row in cursor:
                curr_trip_id = cast(str, row[0])
                curr_service = cast(str, row[1])
                curr_block = cast(str, row[2])
                curr_start_stop = cast(str, row[3])
                curr_start_tech = bool(row[4])
                curr_end_stop = cast(str, row[5])
                curr_end_tech = bool(row[6])
                
                if prev_trip is None:
                    prev_trip = [
                        curr_trip_id, curr_service, curr_block, 
                        curr_start_stop, curr_start_tech, 
                        curr_end_stop, curr_end_tech
                    ]
                    continue

                (prev_id, prev_service, prev_block, 
                 prev_start_stop, prev_start_tech, 
                 prev_end_stop, prev_end_tech) = prev_trip

                should_merge = False
                
                # 1. Same block and service (calendar)
                if curr_service == prev_service and curr_block == prev_block:
                    # 2. Ignore depots
                    if not is_depot(prev_end_stop) and not is_depot(curr_start_stop):
                        # 3. At least one of the connecting stops is marked as technical
                        if prev_end_tech or curr_start_tech:
                            should_merge = True

                if should_merge:
                    # MERGE prev + curr -> prev (extended)
                    
                    # 1. Calculate sequence offset for prev
                    max_seq = cast(int, db.raw_execute(
                        "SELECT MAX(stop_sequence) FROM stop_times WHERE trip_id = ?", (prev_id,)
                    ).one()[0])
                    offset = max_seq + 10

                    # 2. Move stop_times from curr to prev
                    db.raw_execute(
                        "UPDATE stop_times SET trip_id = ?, stop_sequence = stop_sequence + ? WHERE trip_id = ?",
                        (prev_id, offset, curr_trip_id)
                    )
                    
                    # 3. Delete curr trip
                    db.raw_execute("DELETE FROM trips WHERE trip_id = ?", (curr_trip_id,))
                    
                    merged_count += 1
                    
                    prev_trip = [
                        prev_id, prev_service, prev_block,
                        prev_start_stop, prev_start_tech,
                        curr_end_stop, curr_end_tech
                    ]

                else:
                    prev_trip = [
                        curr_trip_id, curr_service, curr_block, 
                        curr_start_stop, curr_start_tech, 
                        curr_end_stop, curr_end_tech
                    ]

        self.logger.info(f"Merged {merged_count} technical trip pairs.")