from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field


class SourceMeta(BaseModel):
    """Tracks which adapter produced which data."""
    geoclient: Optional[str] = None
    pluto: Optional[str] = None
    precinct: Optional[str] = None
    lion: Optional[str] = None
    nta: Optional[str] = None
    coords: Optional[str] = None
    epsg: Optional[str] = None


class GeoBundle(BaseModel):
    """Unified geospatial data bundle used across adapters."""
    # Core IDs
    bbl: Optional[str] = Field(None, description="Borough-Block-Lot identifier")
    bins: Optional[List[str]] = Field(None, description="Building Identification Numbers associated with this parcel")

    # Locational data
    borough: Optional[str] = None
    nta: Optional[str] = None
    precinct: Optional[str] = None
    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Provenance and extras
    sources: Optional[SourceMeta] = Field(default_factory=SourceMeta)
    extras: Optional[Dict[str, Any]] = Field(default_factory=dict)

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"
