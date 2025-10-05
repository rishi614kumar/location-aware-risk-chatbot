from __future__ import annotations
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, Any, Optional
from config.settings import GEOCLIENT_API_KEY  

BASE_URL = "https://api.nyc.gov/geoclient/v2"

def _build_session() -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"])
    )
    sess.headers.update({"Ocp-Apim-Subscription-Key": GEOCLIENT_API_KEY})
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    return sess

# BBL is 10 digits: B (1) + Block (5) + Lot (4).
# returns (borough_code, block_int, lot_int).
def _split_bbl(bbl: str) -> tuple[str, int, int]:
    bbl = str(bbl).strip()
    if len(bbl) != 10 or not bbl.isdigit():
        raise ValueError(f"invalid BBL format: {bbl}")
    borough = bbl[0]          
    block = int(bbl[1:6])          
    lot   = int(bbl[6:10])         
    return borough, block, lot
def _borough_from_code(code: Optional[str]) -> Optional[str]:
    mapping = {"1": "Manhattan", "2": "Bronx", "3": "Brooklyn", "4": "Queens", "5": "Staten Island"}
    return mapping.get(str(code)) if code else None
def _normalize(section: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "bbl": section.get("bbl"),
        "bin": section.get("buildingIdentificationNumber") or section.get("bin"),
        "borough": (
                section.get("boroughName")
                or section.get("firstBoroughName")
                or _borough_from_code(section.get("boroughCode1In"))
            ),
        "nta": section.get("nta"),
        "policePrecinct": section.get("policePrecinct"),
        "communityDistrict": section.get("communityDistrict"),
        "censusTract": section.get("censusTract2010") or section.get("censusTract"),
        "latitude": section.get("latitude"),
        "longitude": section.get("longitude"),
        "grc": section.get("grc"),  # geosupport return code, '00' == exact
        "raw": section,
    }
def _extract_bins(section: Dict[str, Any]) -> list[str]:
    """
    Collects all BINs present in a Geoclient response section.
    Geoclient often returns:
      - 'buildingIdentificationNumber' (single)
      - 'bin' (sometimes)
      - 'giBuildingIdentificationNumber1'..'giBuildingIdentificationNumber6' (address-based lists)
    """
    bins = set()

    # Primary keys
    if v := section.get("buildingIdentificationNumber"):
        bins.add(str(v).strip())
    if v := section.get("bin"):
        bins.add(str(v).strip())

    # GI list-style keys (seen in address responses)
    for k, v in section.items():
        if k.lower().startswith("gibuildingidentificationnumber") and v:
            bins.add(str(v).strip())

    # Return sorted, unique list
    return sorted(b for b in bins if b and b.isdigit())


# --- public helpers your teammates can call -----------------------------------
def get_bin_from_address(address: str, borough: str) -> Optional[str]:
    """
    Returns a single 'primary' BIN for the address (if present).
    For all BINs, use get_bins_from_address().
    """
    info = _get_client().address_string(address, borough)
    bins = _extract_bins(info.get("raw", {}))
    return bins[0] if bins else None


def get_bins_from_address(address: str, borough: str) -> list[str]:
    """
    Returns all BINs Geoclient associates with the input address.
    Addresses can map to multiple buildings (condos/campuses).
    """
    info = _get_client().address_string(address, borough)
    return _extract_bins(info.get("raw", {}))


def get_bins_from_bbl(bbl: str) -> list[str]:
    """
    Returns all BINs Geoclient lists for the input BBL.
    Note: /bbl responses may have fewer GI fields than /address,
    but we still scan for both 'bin' and GI BIN fields.
    """
    info = _get_client().bbl(bbl)
    return _extract_bins(info.get("raw", {}))


def get_bbl_from_bin(bin_: str) -> Optional[str]:
    """
    Returns the BBL associated with a BIN.
    If 'bbl' is not present in the normalized section, try to compose it
    from borough/block/lot fields in the raw payload.
    """
    client = _get_client()
    section = client.bin(bin_)

    # 1) Best case: normalized already has it
    if section.get("bbl"):
        return section["bbl"]

    raw = section.get("raw", {})

    # 2) Sometimes present as separate parts
    b = str(raw.get("bblBoroughCode") or "").strip()
    blk = str(raw.get("bblTaxBlock") or "").strip()
    lot = str(raw.get("bblTaxLot") or "").strip()
    if b and blk and lot and blk.isdigit() and lot.isdigit() and b.isdigit():
        # zero-pad block to 5, lot to 4
        return f"{b}{int(blk):05d}{int(lot):04d}"

    # 3) No luck
    return None
class Geoclient:
    def __init__(self, api_key: Optional[str] = None, base_url: str = BASE_URL):
        self.base_url = base_url
        if api_key:
            # allow overriding env var for tests
            self.session = _build_session()
            self.session.headers.update({"Ocp-Apim-Subscription-Key": api_key})
        else:
            self.session = _build_session()
    def bin(self, bin_: str, *, timeout: float = 10.0) -> Dict[str, Any]:
        url = f"{self.base_url}/bin.json"
        params = {"bin": bin_}
        r = self.session.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        section = data.get("bin") or {}
        return _normalize(section)
    # Address -> everything 
    def address(self, house_number: str, street: str, borough: str, *, timeout: float = 10.0) -> Dict[str, Any]:
        url = f"{self.base_url}/address.json"
        params = {"houseNumber": house_number, "street": street, "borough": borough}
        r = self.session.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        section = data.get("address") or {}
        return _normalize(section)

    # single string pass
    def address_string(self, address: str, borough: str, *, timeout: float = 10.0) -> Dict[str, Any]:
        parts = address.strip().split(maxsplit=1)
        house = parts[0]
        street = parts[1] if len(parts) > 1 else ""
        return self.address(house, street, borough, timeout=timeout)

    # BBL -> everything
    def bbl(self, bbl: str, *, timeout: float = 10.0) -> Dict[str, Any]:
        url = f"{self.base_url}/bbl.json"
        borough, block, lot = _split_bbl(bbl)
        params = {"borough": borough, "block": block, "lot": lot}
        r = self.session.get(url, params=params, timeout=timeout)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            raise requests.HTTPError(f"{e}\nPayload: {params}\nBody: {r.text}") from e
        data = r.json()
        section = data.get("bbl") or {}
        return _normalize(section)


_client: Optional[Geoclient] = None

def _get_client() -> Geoclient:
    global _client
    if _client is None:
        _client = Geoclient()
    return _client

def get_bbl_from_address(address: str, borough: str) -> Optional[str]:
    info = _get_client().address_string(address, borough)
    return info.get("bbl")


