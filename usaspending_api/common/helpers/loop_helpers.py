def chunk_ids(min_id, max_id, chunk_size):
    """
    Since I tend to use this a lot, figured I should just immortalize it into
    its own function.  In a nutshell, this is a way to loop over a range of
    numbers from min_id to max_id (inclusive) in chunks of chunk_size.  It is
    very similar to using range with a step except:

        - it returns a tuple with the current chunk min and max and progress ratio
        - max will never exceed max_id
        - there will never be overlap between chunks

    Example:

        chunk_ids(10, 25, 6) will return
        (10, 15, 0.375)
        (16, 21, 0.75)
        (22, 25, 1.0)

    Useful for grafting into queries where you're chunking updates by id.

        for _min, _max in chunk_ids(10, 25, 6):
            cursor.execute(
                "update my_table set my_column = 1 where id betwee %s and %s",
                _min,
                _max
            )
    """
    if min_id > max_id:
        raise ValueError("min_id must be less than or equal to max_id")
    if chunk_size < 1:
        raise ValueError("chunk_size must be a positive integer")
    _min = min_id
    while _min <= max_id:
        _max = min(_min + chunk_size - 1, max_id)
        yield(_min, _max, (_max - min_id + 1) / (max_id - min_id + 1))
        _min = _max + 1
