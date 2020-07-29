class Bunch:
    """Generic class to hold a group of attributes."""

    def __init__(self, **kwds):
        self.__dict__.update(kwds)
