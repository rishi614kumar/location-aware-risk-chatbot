from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from sodapy import Socrata
from dotenv import load_dotenv

import pandas as pd
import geopandas as gpd
import os
from config import settings

# Configuration has been moved to config.settings; create local aliases for compatibility.
DATASET_DESCRIPTIONS: Dict[str, str] = settings.DATASET_DESCRIPTIONS

# Lightweight topical tags that downstream UIs can group/filter on.
DATASET_TAGS: Dict[str, List[str]] = settings.DATASET_TAGS

# Capabilities advertised by the dataset fetch layer.
DEFAULT_DATASET_FLAGS: Dict[str, bool] = settings.DEFAULT_DATASET_FLAGS

# Socrata dataset identifiers (4-4 codes) when available.
DATASET_API_IDS: Dict[str, str] = settings.DATASET_API_IDS


# Canonical mapping from risk categories to the datasets that provide answers.
cat_to_ds: Dict[str, List[str]] = settings.cat_to_ds

_DATASET_TO_CATEGORIES: Dict[str, List[str]] = {}
for category, dataset_names in cat_to_ds.items():
    for name in dataset_names:
        _DATASET_TO_CATEGORIES.setdefault(name, []).append(category)


# -----------------------------
# Core dataset structures
# -----------------------------

def load_flatfile_dataset(name: str, path: str, layer: str = None, **kwargs):
    """
    General loader for flatfile datasets. Supports gdb, shp, csv, excel, pkl, parquet, etc.
    Returns a pandas or geopandas DataFrame.
    """
    ext = os.path.splitext(path)[-1].lower()
    if ext in [".csv"]:
        return pd.read_csv(path, **kwargs)
    elif ext in [".xlsx", ".xls"]:
        return pd.read_excel(path, **kwargs)
    elif ext in [".pkl"]:
        return pd.read_pickle(path, **kwargs)
    elif ext in [".parquet"]:
        return pd.read_parquet(path, **kwargs)
    elif ext in [".shp"]:
        return gpd.read_file(path, **kwargs)
    elif ext in [".gdb"]:
        # specify a layer name
        layers = gpd.io.file.fiona.listlayers(path)
        use_layer = layer or (layers[0] if layers else None)
        if use_layer:
            return gpd.read_file(path, layer=use_layer, **kwargs)
        else:
            raise ValueError(f"No layers found in GDB: {path}")
    else:
        raise NotImplementedError(f"Unsupported flatfile format: {ext} for dataset '{name}'")

@dataclass(frozen=True)
class DataSet:
    name: str
    categories: List[str]
    description: str = ""
    tags: List[str] = field(default_factory=list)
    priority: int = 0
    supports_point_radius: bool = True
    supports_intersections: bool = True
    supports_addresses: bool = True
    _df_cache: Optional[Union[pd.DataFrame, gpd.GeoDataFrame]] = field(default=None, init=False, repr=False, compare=False)

    @property
    def desc(self) -> str:
        # Short alias used by legacy code paths (e.g., handler.d1.desc)
        return self.description

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "categories": list(self.categories),
            "description": self.description,
            "tags": list(self.tags),
            "priority": self.priority,
            "supports_point_radius": self.supports_point_radius,
            "supports_intersections": self.supports_intersections,
            "supports_addresses": self.supports_addresses,
        }

    def fetch_data_frame(self, where=None, limit=None) -> pd.DataFrame:
        """Fetch a pandas DataFrame for this dataset using its configured source, with optional filtering."""
        api_id = DATASET_API_IDS.get(self.name)
        if api_id is None:
            path = settings.FLATFILE_PATHS.get(self.name)
            layer = settings.FLATFILE_LAYERS.get(self.name)
            if not path:
                raise FileNotFoundError(f"No path configured for flatfile dataset '{self.name}'")
            df = load_flatfile_dataset(self.name, path, layer=layer)
            # apply filtering if 'where' or 'limit' is provided
            if where:
                # simple pandas query
                df = df.query(where)
            if limit is not None:
                df = df.head(limit)
            return df
        if not api_id:
            raise NotImplementedError(f"No API mapping configured for dataset '{self.name}'")

        client = Socrata("data.cityofnewyork.us", None)
        params = {}
        if limit is not None:
            params["limit"] = limit
        if where:
            params["where"] = where
        results = client.get(api_id, **params)
        return pd.DataFrame.from_records(results)

    @property
    def df(self) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """Return a cached DataFrame, fetching it if necessary."""
        if self._df_cache is None:
            object.__setattr__(self, "_df_cache", self.fetch_data_frame())
        cached = self._df_cache
        return cached.copy() if hasattr(cached, "copy") else cached

    def get_df(self) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """Backwards-compatible method alias to retrieve the dataset."""
        return self.df

    def df_filtered(self, where=None, limit=None) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """Return a DataFrame with optional filtering. Returns empty DataFrame if any error occurs."""
        try:
            return self.fetch_data_frame(where=where, limit=limit)
        except Exception as e:
            # Try to infer type: if it's a flatfile and expected to be geodata, return empty GeoDataFrame
            print("Error fetching DataFrame:", str(e))
            api_id = DATASET_API_IDS.get(self.name)
            path = settings.FLATFILE_PATHS.get(self.name)
            if api_id is None and path and os.path.splitext(path)[-1].lower() in [".shp", ".gdb"]:
                return gpd.GeoDataFrame()
            return pd.DataFrame()

def _build_dataset(name: str) -> DataSet:
    """Create a DataSet object from metadata tables, preserving flags & tags."""
    categories = sorted(set(_DATASET_TO_CATEGORIES.get(name, [])))
    description = DATASET_DESCRIPTIONS.get(name, "")
    tags = DATASET_TAGS.get(name, [])
    return DataSet(
        name=name,
        categories=categories,
        description=description,
        tags=list(tags),
        **DEFAULT_DATASET_FLAGS,
    )


class DataHandler:
    """
    Container that hydrates dataset names into DataSet objects and exposes them
    as attributes (d1, d2, ...).
    """

    def __init__(self, dataset_names: Iterable[str]) -> None:
        # Deduplicate while preserving order so attribute naming stays stable.
        unique_names: List[str] = []
        seen = set()
        for name in dataset_names:
            if not name:
                continue
            if name not in seen:
                seen.add(name)
                unique_names.append(name)

        self._datasets: List[DataSet] = [_build_dataset(name) for name in unique_names]
        for idx, dataset in enumerate(self._datasets, start=1):
            setattr(self, f"d{idx}", dataset)

    def __iter__(self):
        # Support simple loops like `for ds in handler`
        return iter(self._datasets)

    def __len__(self) -> int:
        return len(self._datasets)

    def __getitem__(self, index: int) -> DataSet:
        return self._datasets[index]

    def __repr__(self) -> str:
        names = ", ".join(ds.name for ds in self._datasets) or "∅"
        return f"DataHandler([{names}])"

    @property
    def datasets(self) -> List[DataSet]:
        return list(self._datasets)

    @property
    def names(self) -> List[str]:
        return [ds.name for ds in self._datasets]

    def to_list(self) -> List[DataSet]:
        return self.datasets


# -----------------------------
# Dataset utilities
# -----------------------------

def describe_datasets(dataset_names: Iterable[str], *, as_dict: bool = True) -> List[Any]:
    """Utility helper for callers that only need ad-hoc dataset descriptions."""
    datasets = [_build_dataset(name) for name in dataset_names if name]
    if as_dict:
        return [ds.to_dict() for ds in datasets]
    return datasets


__all__ = [
    "DataSet",
    "DataHandler",
    "cat_to_ds"
]
