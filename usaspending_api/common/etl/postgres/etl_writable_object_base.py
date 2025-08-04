from abc import abstractmethod, ABCMeta
from django.utils.functional import cached_property
from typing import List, Optional
from usaspending_api.common.etl.postgres.etl_object_base import ETLObjectBase
from usaspending_api.common.etl.postgres.primatives import ColumnOverrides, DataTypes, KeyColumns


class ETLWritableObjectBase(ETLObjectBase, metaclass=ABCMeta):
    def __init__(
        self,
        key_overrides: Optional[List[str]] = None,
        insert_overrides: Optional[ColumnOverrides] = None,
        update_overrides: Optional[ColumnOverrides] = None,
    ):
        self._key_overrides = key_overrides or []
        self._insert_overrides = insert_overrides or {}
        self._update_overrides = update_overrides or {}
        super(ETLWritableObjectBase, self).__init__()

    @cached_property
    def data_types(self) -> DataTypes:
        data_types = self._get_data_types()
        if not data_types:
            raise RuntimeError("No columns found.  Do we have permission to see the database object?")
        return data_types

    @cached_property
    def insert_overrides(self) -> ColumnOverrides:
        if self._insert_overrides:
            if not all(c in self.columns for c in self._insert_overrides):
                raise RuntimeError("All columns listed in insert_overrides must exist in database object.")
            key_columns = [k.name for k in self.key_columns]
            if any(c in key_columns for c in self._insert_overrides):
                raise RuntimeError("Overriding key columns is not currently supported.")
        return self._insert_overrides

    @cached_property
    def key_columns(self) -> KeyColumns:
        key_columns = self._get_key_columns()
        if not key_columns:
            raise RuntimeError("No columns found.  Do we have permission to see the database object?")
        return key_columns

    @cached_property
    def update_overrides(self) -> ColumnOverrides:
        if self._update_overrides:
            if not all(c in self.columns for c in self._update_overrides):
                raise RuntimeError("All columns listed in update_overrides must exist in database object.")
            key_columns = [k.name for k in self.key_columns]
            if any(c in key_columns for c in self._update_overrides):
                raise RuntimeError("Overriding key columns is not currently supported.")
        return self._update_overrides

    @abstractmethod
    def _get_data_types(self) -> DataTypes:
        """Returns a mapping of columns names to database data types."""
        raise NotImplementedError("Must be implemented in subclasses of ETLWritableObjectBase.")

    def _get_key_columns(self) -> KeyColumns:
        """Returns caller supplied or primary key columns (if caller did not supply keys)."""
        if self._key_overrides:
            if not all(c in self.columns for c in self._key_overrides):
                raise RuntimeError("All columns listed in key_overrides must exist in database object.")
            keys = self._key_overrides
        else:
            keys = self._get_primary_key_columns()
            if not keys:
                raise RuntimeError(
                    "The database object either must have primary keys or key overrides must be supplied."
                )
        data_types = self.data_types
        return [data_types[c] for c in keys]

    @abstractmethod
    def _get_primary_key_columns(self) -> List[str]:
        """Returns caller supplied or primary key columns (if caller did not supply keys)."""
        raise NotImplementedError("Must be implemented in subclasses of ETLWritableObjectBase.")


__all__ = ["ETLWritableObjectBase"]
