from abc import ABCMeta, abstractmethod

DEFAULT_CHILDREN = 0


class FilterTree(metaclass=ABCMeta):
    def search(self, tier1, tier2, tier3, child_layers, filter_string) -> list:
        if tier3:
            ancestor_array = [tier1, tier2, tier3]
        elif tier2:
            ancestor_array = [tier1, tier2]
        elif tier1:
            ancestor_array = [tier1]
        else:
            ancestor_array = []

        retval = self.raw_search(ancestor_array, child_layers, filter_string)
        return retval

    @abstractmethod
    def raw_search(self, tiered_keys: list) -> list:
        """
        Basic unit of searching, given the path to the parent and the filter string. Output can be a list of any type, and is
        only used by the unlinked_node_from_data abstract function.

        :param: tiered_keys - list
        :param: filter_string - string or null

        """
        pass
