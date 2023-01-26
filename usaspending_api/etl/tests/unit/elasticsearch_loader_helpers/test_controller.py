from usaspending_api.etl.elasticsearch_loader_helpers.controller import PostgresElasticsearchIndexerController
from math import ceil


def test_get_id_range_for_partition_one_records():
    min_id = 1
    max_id = 1
    id_range_item_count = max_id - min_id + 1  # this many individual IDs should be processed for continuous ID range
    etl_config = {"partition_size": 10000}
    ctrl = PostgresElasticsearchIndexerController(etl_config)
    ctrl.min_id = min_id
    ctrl.max_id = max_id
    ctrl.record_count = id_range_item_count  # assume records exist for each ID in range
    ctrl.config["partitions"] = ctrl.determine_partitions()
    partition_range = range(0, ctrl.config["partitions"])
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[0])
    assert lower_bound == min_id
    assert upper_bound == max_id
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[-1])
    assert lower_bound == min_id
    assert upper_bound == max_id
    id_set = set(range(min_id, max_id + 1))
    assert _remove_seen_ids(ctrl, id_set) == set({})


def test_get_id_range_for_partition_two_records():
    min_id = 1
    max_id = 2
    id_range_item_count = max_id - min_id + 1  # this many individual IDs should be processed for continuous ID range
    etl_config = {"partition_size": 10000}
    ctrl = PostgresElasticsearchIndexerController(etl_config)
    ctrl.min_id = min_id
    ctrl.max_id = max_id
    ctrl.record_count = id_range_item_count  # assume records exist for each ID in range
    ctrl.config["partitions"] = ctrl.determine_partitions()
    partition_range = range(0, ctrl.config["partitions"])
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[0])
    assert lower_bound == min_id
    assert upper_bound == max_id
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[-1])
    assert lower_bound == min_id
    assert upper_bound == max_id
    id_set = set(range(min_id, max_id + 1))
    assert _remove_seen_ids(ctrl, id_set) == set({})


def test_get_id_range_for_partition_with_evenly_divisible():
    """Check all is good when set of records fit evenly into partitions (each partition full)"""
    min_id = 1
    max_id = 100
    partition_size = 20
    id_range_item_count = max_id - min_id + 1  # this many individual IDs should be processed for continuous ID range
    assert id_range_item_count % partition_size == 0  # evenly divisible
    etl_config = {"partition_size": partition_size}
    ctrl = PostgresElasticsearchIndexerController(etl_config)
    ctrl.min_id = min_id
    ctrl.max_id = max_id
    ctrl.record_count = id_range_item_count  # assume records exist for each ID in range
    ctrl.config["partitions"] = ctrl.determine_partitions()
    assert ctrl.config["partitions"] == ceil(id_range_item_count / partition_size)
    partition_range = range(0, ctrl.config["partitions"])
    # First batch
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[0])
    assert lower_bound == min_id
    assert upper_bound == lower_bound + (partition_size - 1)
    # Second batch
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[1])
    assert lower_bound == min_id + partition_size
    assert upper_bound == lower_bound + (partition_size - 1)
    # Last batch should go all the way up to max_id
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[-1])
    assert lower_bound == (max_id - partition_size + 1) == (min_id + (partition_size * partition_range[-1]))
    assert upper_bound == max_id
    id_set = set(range(min_id, max_id + 1))
    assert _remove_seen_ids(ctrl, id_set) == set({})


def test_get_id_range_for_partition_with_one_over():
    """Checks that the proper upper and lower bound are retrieved even when the range of IDs leaves only 1 item
    in the last partition. There was a bug here before."""
    min_id = 1
    max_id = 101
    partition_size = 20
    id_range_item_count = max_id - min_id + 1  # this many individual IDs should be processed for continuous ID range
    assert id_range_item_count % partition_size == 1  # one over the partition size
    etl_config = {"partition_size": partition_size}
    ctrl = PostgresElasticsearchIndexerController(etl_config)
    ctrl.min_id = min_id
    ctrl.max_id = max_id
    ctrl.record_count = id_range_item_count  # assume records exist for each ID in range
    ctrl.config["partitions"] = ctrl.determine_partitions()
    assert ctrl.config["partitions"] == ceil(id_range_item_count / partition_size)
    partition_range = range(0, ctrl.config["partitions"])
    # First batch
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[0])
    assert lower_bound == min_id
    assert upper_bound == lower_bound + (partition_size - 1)
    # Second batch
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[1])
    assert lower_bound == min_id + partition_size
    assert upper_bound == lower_bound + (partition_size - 1)
    # Last batch should go all the way up to max_id
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[-1])
    assert lower_bound == (min_id + (partition_size * partition_range[-1])) == 101
    assert upper_bound == max_id == 101
    id_set = set(range(min_id, max_id + 1))
    assert _remove_seen_ids(ctrl, id_set) == set({})


def test_get_id_range_for_partition_with_evenly_divisible_partition_size_offset():
    """Checks that the proper upper and lower bound are retrieved even when the range of IDs is evenly divisible by
    the partition size. There was a bug here before."""
    min_id = 4
    max_id = 6004
    partition_size = 2000
    id_range_item_count = max_id - min_id + 1  # this many individual IDs should be processed for continuous ID range
    etl_config = {"partition_size": partition_size}
    ctrl = PostgresElasticsearchIndexerController(etl_config)
    ctrl.min_id = min_id
    ctrl.max_id = max_id
    ctrl.record_count = id_range_item_count  # assume records exist for each ID in range
    ctrl.config["partitions"] = ctrl.determine_partitions()
    assert ctrl.config["partitions"] == ceil(id_range_item_count / partition_size)
    partition_range = range(0, ctrl.config["partitions"])
    # First batch
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[0])
    assert lower_bound == min_id
    assert upper_bound == lower_bound + (partition_size - 1)
    # Second batch
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[1])
    assert lower_bound == min_id + partition_size
    assert upper_bound == lower_bound + (partition_size - 1)
    # Last batch should go all the way up to max_id
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[-1])
    assert lower_bound == (min_id + (partition_size * partition_range[-1]))
    assert upper_bound == max_id
    id_set = set(range(min_id, max_id + 1))
    assert _remove_seen_ids(ctrl, id_set) == set({})


def test_get_id_range_for_partition_with_sparse_range():
    """Checks that the proper upper and lower bound are retrieved even when the range of IDs is evenly divisible by
    the partition size. There was a bug here before."""
    min_id = 4
    max_id = 5999
    partition_size = 2000
    id_range_item_count = max_id - min_id + 1  # this many individual IDs should be processed for continuous ID range
    record_ids = {4, 5, 7, 99, 101, 120, 1998, 1999, 2000, 2001, 2002, 4444, 5999}
    etl_config = {"partition_size": partition_size}
    ctrl = PostgresElasticsearchIndexerController(etl_config)
    ctrl.min_id = min_id
    ctrl.max_id = max_id
    ctrl.record_count = len(record_ids)
    ctrl.config["partitions"] = ctrl.determine_partitions()
    assert ctrl.config["partitions"] == ceil(id_range_item_count / partition_size)
    partition_range = range(0, ctrl.config["partitions"])
    # First batch
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[0])
    assert lower_bound == min_id
    assert upper_bound == lower_bound + (partition_size - 1)
    # Second batch
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[1])
    assert lower_bound == min_id + partition_size
    assert upper_bound == lower_bound + (partition_size - 1)
    # Last batch should go all the way up to max_id
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[-1])
    assert lower_bound == (min_id + (partition_size * partition_range[-1]))
    assert upper_bound == max_id
    assert _remove_seen_ids(ctrl, record_ids) == set({})


def test_get_id_range_for_partition_with_empty_partitions():
    """Checks that the proper upper and lower bound are retrieved even when the range of IDs is evenly divisible by
    the partition size. There was a bug here before."""
    min_id = 1
    max_id = 100
    partition_size = 20
    id_range_item_count = max_id - min_id + 1  # this many individual IDs should be processed for continuous ID range
    record_ids = {1, 5, 7, 15, 19, 20, 41, 100}
    etl_config = {"partition_size": partition_size}
    ctrl = PostgresElasticsearchIndexerController(etl_config)
    ctrl.min_id = min_id
    ctrl.max_id = max_id
    ctrl.record_count = len(record_ids)
    ctrl.config["partitions"] = ctrl.determine_partitions()
    assert ctrl.config["partitions"] == ceil(id_range_item_count / partition_size)
    partition_range = range(0, ctrl.config["partitions"])
    # First batch
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[0])
    assert lower_bound == min_id
    assert upper_bound == lower_bound + (partition_size - 1)
    # Second batch
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[1])
    assert lower_bound == min_id + partition_size
    assert upper_bound == lower_bound + (partition_size - 1)
    # Last batch should go all the way up to max_id
    lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_range[-1])
    assert lower_bound == (min_id + (partition_size * partition_range[-1]))
    assert upper_bound == max_id
    assert _remove_seen_ids(ctrl, record_ids) == set({})


def _remove_seen_ids(ctrl, id_set):
    """Iterates through each bounded id-range, and removes IDs seen"""
    partition_range = range(0, ctrl.config["partitions"])
    unseen_ids = id_set.copy()
    for partition_idx in partition_range:
        lower_bound, upper_bound = ctrl.get_id_range_for_partition(partition_idx)
        for seen_id in id_set:
            if lower_bound <= seen_id <= upper_bound:
                unseen_ids.remove(seen_id)
    return unseen_ids
