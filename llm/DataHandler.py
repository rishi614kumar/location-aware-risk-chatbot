from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from sodapy import Socrata
from dotenv import load_dotenv

import pandas as pd
import geopandas as gpd
import os

# Human-readable context for each dataset; extend as new sources come online.
DATASET_DESCRIPTIONS: Dict[str, str] = {
    "Asbestos Control Program": (
        "ACP7 form is an asbestos project notification. Any time asbestos abatement is perform on "
        "quantities greater than a minor project amount, the applicant has to file this form with "
        "DEP Asbestos Control Program (ACP). All asbestos documents are filed through the Asbestos "
        "Reporting and Tracking System (ARTS) E-file system. This system is web based and entirely "
        "paperless. All information on the ACP7 is essential to meet the requirements setforth in "
        "the asbestos rules and regulations Title15, Chapter 1 (RCNY). ACP enforcement staff utilizes "
        "this form for inspection of asbestos abatement activities."
    ),
    "Citywide Catch Basins" : (
        "NYCDEP Citywide Catch Basins. "
        "Catch basins are an important part of New York City’s 7,500-mile sewer network. "
        "They are connected to underground pipes that channel stormwater from the street "
        "to one of DEP’s 14 wastewater resource recovery facilities, or directly into our "
        "surrounding waterbodies. DEP cleans and maintains over 150,000 catch basins citywide."
    )
}

# Lightweight topical tags that downstream UIs can group/filter on.
DATASET_TAGS: Dict[str, List[str]] = {
    "Asbestos Control Program": ["asbestos", "environmental", "health"],
}

# Capabilities advertised by the dataset fetch layer.
DEFAULT_DATASET_FLAGS: Dict[str, bool] = dict(
    supports_point_radius=True,
    supports_intersections=True,
    supports_addresses=True,
)

# Socrata dataset identifiers (4-4 codes) when available.
DATASET_API_IDS: Dict[str, str] = {
    "Asbestos Control Program": "vq35-j9qm",
    "Clean Air Tracking System (CATS)": "f4rp-2kvy",
}

NYC_OD_APPLICATION_TOKEN = os.getenv('NYC_OD_APPLICATION_TOKEN')
NYC_OD_USERNAME = os.getenv('NYC_OD_USERNAME')
NYC_OD_PASSWORD = os.getenv('NYC_OD_PASSWORD')

# Canonical mapping from risk categories to the datasets that provide answers.
cat_to_ds: Dict[str, List[str]] = {
    "Environmental & Health Risks": [
        "Asbestos Control Program",
        "Population by Neighborhood Tabulation Area",
        "Clean Air Tracking System (CATS)",
        "Citywide Catch Basins",
        "Sewer System Data",
    ],
    "Zoning & Land Use": [
        "City Owned and Leased Property",
        "NYC OpenData Zoning and Tax Lot Database",
        "Historic Districts map",
        "LION data",
        "Zoning GIS data",
        "Digital City map shapefile",
        "Parks Monuments",
        "City Owned and Leased Property",
    ],
    "Construction & Permitting": [
        "Street Construction Permits",
        "DOB permits",
        "City Owned and Leased Property",
        "DOB Job filings",
        "Water and Sewer Permits",
        "DOB NOW: Build - Job Application Findings",
    ],
    "Transportation & Traffic": [
        "NYC OpenData Automated Traffic Volume Counts",
        "NYC OpenData Motor Vehicle Collisions",
        "Crime",
        "Street Construction Permits",
        "MTA subway and other underground train lines",
    ],
    "Public Safety & Social Context": [
        "NYC OpenData PLUTO",
        "NYC OpenData Motor Vehicle Collisions",
        "NYC OpenData Automated Traffic Volume Counts",
        "Population by Community Districts",
        "Population by Neighborhood Tabulation Area",
        "Crime",
        "Citywide Hydrants",
    ],
    "Comparative Site Queries": [
        "MTA subway and other underground train lines",
        "Sewer System Data",
        "Water and Sewer Permits",
        "DOB NOW: Build - Job Application Findings",
        "Citywide Hydrants",
    ],
}

_DATASET_TO_CATEGORIES: Dict[str, List[str]] = {}
for category, dataset_names in cat_to_ds.items():
    for name in dataset_names:
        _DATASET_TO_CATEGORIES.setdefault(name, []).append(category)


# -----------------------------
# Core dataset structures
# -----------------------------

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

    def fetch_data_frame(self) -> pd.DataFrame:
        """Fetch a pandas DataFrame for this dataset using its configured source."""
        api_id = DATASET_API_IDS.get(self.name)
        if not api_id:
            raise NotImplementedError(f"No API mapping configured for dataset '{self.name}'")

        client = Socrata("data.cityofnewyork.us", NYC_OD_APPLICATION_TOKEN, username=NYC_OD_USERNAME, password=NYC_OD_PASSWORD)
        results = client.get(api_id, limit=1000)
        return pd.DataFrame.from_records(results)

    @property
    def df(self) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """Return a fresh DataFrame for callers expecting property-style access."""
        return self.fetch_data_frame()

    def get_df(self) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """Backwards-compatible method alias to retrieve the dataset."""
        return self.fetch_data_frame()

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
