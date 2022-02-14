from typing import Iterable, List, Mapping, NamedTuple, Tuple, Union

from .helpers import _Pt


class Point(NamedTuple):
    lat: float
    lon: float
    id: int


def _dist_squared(p: Union[Point, _Pt], q: Union[Point, _Pt]) -> float:
    """Returns the euclidian distance squared between two points."""
    dx = p[0] - q[0]
    dy = p[1] - q[1]
    return dx*dx + dy*dy


def _brute_nn(root: _Pt, space: Iterable[Point]) -> Point:
    """
    Implements a brute-force nearest-neighbor search.
    Finds the nearest Point to given search root.
    """
    best = None
    best_dist = float("inf")

    for pt in space:
        dist = _dist_squared(root, pt)
        if dist < best_dist:
            best = pt
            best_dist = dist

    if best is None:
        raise ValueError("brute NN search in empty space")

    return best


def _pick_closest(root: _Pt, p: Point, q: Point) -> Tuple[Point, float]:
    """Decides whether p or q is closer to root. Returns (Point, distance_squared)."""
    p_dist = _dist_squared(root, p)
    q_dist = _dist_squared(root, q)

    if p_dist < q_dist:
        return p, p_dist
    else:
        return q, q_dist


class KDTree:
    """
    Implements a simple 2-dimensional KD Tree.
    Create new trees with KDTree.build().
    """
    __slots__ = ("is_leaf", "pivot", "left", "right", "points")

    def __init__(self, is_leaf: bool, pivot: Point = None, left: "KDTree" = None,
                 right: "KDTree" = None, points: List[Point] = None) -> None:
        self.is_leaf = is_leaf
        self.pivot = pivot
        self.left = left
        self.right = right
        self.points = points

    @classmethod
    def build(cls, points: Iterable[Point], leaf_size: int = 64, axis: int = 0) -> "KDTree":
        """
        Creates a KDTree from a list of points.
        Don't set the axis argument, it meant only for subsequent recursive calls.
        """
        sorted_points = sorted(points, key=lambda i: i[axis])
        n = len(sorted_points)

        if n <= leaf_size:
            return cls(is_leaf=True, points=sorted_points)

        median = n // 2

        return cls(
            is_leaf=False,
            pivot=sorted_points[median],
            left=cls.build(sorted_points[:median], leaf_size, axis ^ 1),
            right=cls.build(sorted_points[median + 1:], leaf_size, axis ^ 1)
        )

    @classmethod
    def build_from_dict(cls, points: Mapping[int, _Pt], leaf_size: int = 16) -> "KDTree":
        """Creates a KDTree from a mapping {id: (lat, lon)}."""
        return cls.build((Point(*v, k) for k, v in points.items()), leaf_size)

    def search_nn(self, search_root: _Pt, axis: int = 0) -> Point:
        """
        A short implementation of the nearest-neighbor search.
        Considers the search space is a rectangle and uses euclidian distance when comparing.
        Not perfect for calculations on Earth, but close enough for most use cases.
        """
        # Leaf reached - no more branches to recurse into
        if self.is_leaf:
            assert self.points is not None, \
                "Logical error: KDTree.is_leaf and KDTree.points is None!"

            return _brute_nn(search_root, self.points)

        # Non-leaf - assert that all required data is there
        assert self.pivot is not None, \
            "Logical error: not KDTree.is_leaf and KDTree.pivot is None!"
        assert self.left is not None, "Logical error: not KDTree.is_leaf and KDTree.left is None!"
        assert self.right is not None, \
            "Logical error: not KDTree.is_leaf and KDTree.right is None!"

        # Check which branch to recurse into first
        if search_root[axis] < self.pivot[axis]:
            first = self.left
            second = self.right
        else:
            first = self.right
            second = self.left

        # Recursively check nn in first branch and comapre the distance with current pivot
        best, best_square_dist = _pick_closest(
            search_root,
            first.search_nn(search_root, axis ^ 1),
            self.pivot
        )

        # Check if there is a possibility of a closer node on the opposite side
        # That is - whether current pivot is closer to search_root
        d_to_axis = (search_root[axis] - self.pivot[axis]) ** 2
        if best_square_dist > d_to_axis:
            best, _ = _pick_closest(
                search_root,
                second.search_nn(search_root, axis ^ 1),
                best
            )

        return best
