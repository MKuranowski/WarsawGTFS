from collections.abc import Iterable
from itertools import pairwise
from typing import IO

import pyroutelib3


class ShapeGenerator:
    def __init__(self, osm_file: IO[bytes]) -> None:
        self.graph = read_graph_from_osm(osm_file)
        self.stop_id_to_node_id = read_stops_from_osm(osm_file)

    def generate_shape(self, stop_ids: Iterable[str]) -> list[tuple[float, float]]:
        shape = list[tuple[float, float]]()
        is_first_leg = True

        for stop_a, stop_b in pairwise(stop_ids):
            nodes = pyroutelib3.find_route_without_turn_around(
                self.graph,
                self.stop_id_to_node_id[stop_a],
                self.stop_id_to_node_id[stop_b],
            )
            offset = 0 if is_first_leg else 1
            shape.extend(self.graph.get_node(node).position for node in nodes[offset:])
            is_first_leg = False

        return shape


def read_stops_from_osm(osm_file: IO[bytes]) -> dict[str, int]:
    osm_file.seek(0)
    return {
        feature.tags["ref"]: feature.id
        for feature in pyroutelib3.osm.reader.read_features(osm_file)
        if isinstance(feature, pyroutelib3.osm.reader.Node)
        and feature.tags.get("public_transport") == "stop_position"
        and feature.tags.get("subway") == "yes"
        and "ref" in feature.tags
    }


def read_graph_from_osm(osm_file: IO[bytes]) -> pyroutelib3.osm.Graph:
    osm_file.seek(0)
    return pyroutelib3.osm.Graph.from_file(pyroutelib3.osm.SubwayProfile(), osm_file)
