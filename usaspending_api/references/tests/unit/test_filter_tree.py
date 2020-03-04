from usaspending_api.references.v2.views.filter_trees.filter_tree import FilterTree, Node


class TestFilterTree(FilterTree):
    def toptier_search(self):
        return [{"value": 1}, {"value": 2}]

    def tier_one_search(self, key):
        if key == "test":
            return [{"value": 3}, {"value": 4}]

    def tier_two_search(self, key):
        if key == "test2":
            return [{"value": 5}, {"value": 6}]

    def tier_three_search(self, key):
        if key == "test3":
            return [{"value": 7}, {"value": 8}]

    def construct_node_from_raw(self, tier: int, ancestors: list, data, poulate_children: bool) -> Node:
        return Node(
            id=data["value"], ancestors=ancestors, description=f"description of {data['value']}", count=1, children=[]
        )


def test_toptier_search():
    test_tree = TestFilterTree()
    assert _nodes_to_json(test_tree.search(None, None, None, 1)) == [_node_by_id(1), _node_by_id(2)]


def test_tier_one_search():
    test_tree = TestFilterTree()
    assert _nodes_to_json(test_tree.search("test", None, None, 1)) == [
        _node_by_id(3, ["test"]),
        _node_by_id(4, ["test"]),
    ]


def test_tier_two_search():
    test_tree = TestFilterTree()
    assert _nodes_to_json(test_tree.search("test", "test2", None, 1)) == [
        _node_by_id(5, ["test", "test2"]),
        _node_by_id(6, ["test", "test2"]),
    ]


def test_tier_three_search():
    test_tree = TestFilterTree()
    assert _nodes_to_json(test_tree.search("test", "test2", "test3", 1)) == [
        _node_by_id(7, ["test", "test2", "test3"]),
        _node_by_id(8, ["test", "test2", "test3"]),
    ]


def _nodes_to_json(nodes):
    return [node.to_JSON() for node in nodes]


def _node_by_id(id, ancestors=[]):
    return {"id": id, "ancestors": ancestors, "description": f"description of {id}", "count": 1, "children": []}
