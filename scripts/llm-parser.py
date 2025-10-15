# ============================================================
# NYC Risk Router: LLM Classification, Address Extraction,
#                  and DataSet Selection
# ============================================================

from __future__ import annotations
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

import ollama

# -----------------------------
# 0) CONFIG & STATIC MAPPINGS
# -----------------------------

LANGUAGE_MODEL = "hf.co/bartowski/Qwen2.5-1.5B-Instruct-GGUF"

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

MAIN_CATS: List[str] = list(cat_to_ds.keys())



# -----------------------------
# 1) LLM CATEGORY CLASSIFIER
# -----------------------------

SYS_MULTI = """Classify the user's question into one or more of these categories:
- Environmental & Health Risks
- Zoning & Land Use
- Construction & Permitting
- Transportation & Traffic
- Public Safety & Social Context
- Comparative Site Queries

Return STRICT JSON only:
{"categories": ["<labels>"], "confidence": <0..1>}

Rules:
- Traffic, collisions, congestion, road closures, counts, speeds, hotspots -> Transportation & Traffic
- DOB permits, job filings, street construction, water/sewer permits -> Construction & Permitting
- Zoning, land use, city-owned property, districts -> Zoning & Land Use
- Flood/air/health exposure, sewer systems, population health -> Environmental & Health Risks
- Crime, safety, hydrants, social context -> Public Safety & Social Context
- Comparing between two sites (more/less, better/worse, higher/lower) -> include Comparative Site Queries plus other relevant labels
""".strip()

FEWSHOTS_MULTI: List[Tuple[str, dict]] = [
    ("Where are the top traffic accident hotspots within 500 feet of 163rd Street?",
     {"categories": ["Transportation & Traffic"], "confidence": 0.85}),
    ("Any active DOB permits near 10 Jay St?",
     {"categories": ["Construction & Permitting"], "confidence": 0.85}),
    ("Is this parcel in a historic district and what’s the zoning?",
     {"categories": ["Zoning & Land Use"], "confidence": 0.80}),
    ("Any flood or sewer risk around 123 Main St?",
     {"categories": ["Environmental & Health Risks"], "confidence": 0.80}),
    ("Where are the nearest fire hydrants near Borough Hall?",
     {"categories": ["Public Safety & Social Context"], "confidence": 0.75}),
    ("Compare zoning and environmental risks for 149th Street & Grand Concourse versus 181st Street & St. Nicholas Avenue.",
     {"categories": ["Comparative Site Queries", "Zoning & Land Use", "Environmental & Health Risks"], "confidence": 0.88}),
]

def _build_user_prompt_multi(query: str) -> str:
    """
    Builds a multi-example user prompt for the LLM category classifier.

    This function formats few-shot examples and the user's query into a structured
    prompt string used for multi-category classification.

    Args:
        query (str): The user query to be classified.

    Returns:
        str: The formatted prompt containing few-shot examples and the query.
    """
    parts: List[str] = []
    for u, js in FEWSHOTS_MULTI:
        parts += [f'User: "{u}"', "JSON:", json.dumps(js, ensure_ascii=False), ""]
    parts += [f'User: "{query}"', "JSON:"]
    return "\n".join(parts)

def _llm_classify_multi(query: str, model: str) -> Tuple[List[str], float]:
    """
    Classifies a user query into one or more predefined categories using an LLM.

    This function sends a structured prompt (with few-shot examples and system rules)
    to the specified language model, parses its JSON response, and extracts both
    the predicted categories and confidence score. Categories are filtered to match
    only known `MAIN_CATS` defined in the configuration.

    Args:
        query (str): The user query to be classified.
        model (str): The identifier or path of the language model to use.

    Returns:
        Tuple[List[str], float]: A tuple containing:
            - List[str]: Predicted categories (filtered to known categories or ['Other'] on error).
            - float: Model-reported confidence score between 0 and 1.
    """
    user_prompt = _build_user_prompt_multi(query)
    try:
        resp = ollama.chat(
            model=model,
            messages=[
                {"role": "system", "content": SYS_MULTI},
                {"role": "user", "content": user_prompt},
            ],
            options={"temperature": 0.0},
            format="json",
        )
        data = json.loads(resp["message"]["content"])
    except Exception:
        return (["Other"], 0.5)

    # Accept either {"categories": [...]} or {"category": "..."}
    raw_cats = data.get("categories")
    if not raw_cats:
        single = data.get("category")
        raw_cats = [single] if single else []

    cats = [c for c in raw_cats if c in cat_to_ds] or ["Other"]
    conf = float(data.get("confidence", 0.5))
    return cats, conf

# -----------------------------
# 2) ADDRESS/POI EXTRACTION
# -----------------------------

SYS_ADDR = """Extract all location mentions from the user's query as mailing-style addresses, intersections, or named places/POIs.
Return STRICT JSON only: {"addresses":[<strings>]}
Rules:
- Include numbered street addresses (e.g., "10 Jay St", "123 Main Street")
- Include intersections as "X & Y" (e.g., "149th Street & Grand Concourse")
- Include named places/POIs/landmarks/neighborhoods when used as locators, even without qualifiers
  (e.g., "Times Square", "Union Square Park", "Columbia University", "Penn Station")
- If the query says "near X", include X
- Do NOT include cities/states/countries unless explicitly mentioned
- Preserve original wording/casing; trim whitespace
- Deduplicate while preserving order
""".strip()

FEWSHOTS_ADDR: List[Tuple[str, dict]] = [
    ('Any active DOB permits near 10 Jay St?', {"addresses": ["10 Jay St"]}),
    ('Compare zoning and environmental risks for 149th Street & Grand Concourse versus 181st Street & St. Nicholas Avenue.',
     {"addresses": ["149th Street & Grand Concourse", "181st Street & St. Nicholas Avenue"]}),
    ('Traffic hotspots near Borough Hall and 123 Main St', {"addresses": ["Borough Hall", "123 Main St"]}),
    ('What types of NYPD complaints are most common near Times Square?', {"addresses": ["Times Square"]}),
    ('Collisions around Union Square Park and Penn Station', {"addresses": ["Union Square Park", "Penn Station"]}),
    ('Incidents by Columbia University and Central Park West', {"addresses": ["Columbia University", "Central Park West"]}),
]

def _build_user_prompt_addr(query: str) -> str:
    """
    Builds a few-shot prompt for the LLM address extractor using example queries and JSON outputs.
    """
    parts: List[str] = []
    for u, js in FEWSHOTS_ADDR:
        parts += [f'User: "{u}"', "JSON:", json.dumps(js, ensure_ascii=False), ""]
    parts += [f'User: "{query}"', "JSON:"]
    return "\n".join(parts)

def _normalize_dedupe(seq: Optional[List[str]]) -> List[str]:
    """
    Cleans and deduplicates a list of strings while preserving order.

    Strips whitespace from each element, removes empty entries,
    and ensures that each unique string appears only once in the output.
    """

    seen, out = set(), []
    for s in (seq or []):
        t = (s or "").strip()
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out

# Heuristics for fallback
# Regular expression to match the word "near" followed by a location or phrase.
# Example: "near Central Park" will match "Central Park".
_NEAR_RE = re.compile(r"\bnear\s+([^.,;?!]+)", re.IGNORECASE)

# A set of words that are commonly used as hints for points of interest (POI).
# These words help identify locations such as "park", "station", "university", etc.
_POI_HINT_WORDS = {
    "square", "park", "station", "university", "hall", "center",
    "heights", "village", "plaza", "concourse", "campus"
}

# Regular expression to match proper nouns or capitalized phrases.
# It matches sequences of capitalized words (up to 5 words long) that may include
# letters, numbers, apostrophes, ampersands, hyphens, or periods.
# Example: "New York University" or "St. John's Plaza".
_PROPER_NOUN_SPAN = re.compile(r"\b([A-Z][\w'&.-]*(?:\s+[A-Z][\w'&.-]*){0,4})\b")

def _regex_place_fallback(query: str) -> List[str]:
    """
    Fallback method to extract possible place names or POIs using regex.
    """
    cands: List[str] = []
    # Match phrases after "near" (e.g., "near Central Park")
    for m in _NEAR_RE.findall(query or ""):
        frag = re.split(r"\b(and|or|but)\b", m, 1, flags=re.IGNORECASE)[0].strip()
        if frag:
            cands.append(frag)
    # Match capitalized proper nouns that resemble POIs
    for span in _PROPER_NOUN_SPAN.findall(query or ""):
        toks = span.split()
        if 1 <= len(toks) <= 4 and any(k in span.lower() for k in _POI_HINT_WORDS):
            cands.append(span.strip())
    return _normalize_dedupe(cands)


def _safe_parse_addr_json(s: str) -> List[str]:
    """
    Safely parses a JSON string to extract address-related fields.
    """
    try:
        data = json.loads(s)
    except Exception:
        return []
    # Look for possible address-related keys
    for key in ("addresses", "address", "locations"):
        v = data.get(key)
        if isinstance(v, list):
            return _normalize_dedupe(v)
    return []


def _llm_extract_addresses(query: str, model: str = LANGUAGE_MODEL) -> List[str]:
    """
    Extracts location mentions from a query using the LLM, with regex fallback.
    """
    user_prompt = _build_user_prompt_addr(query)
    # Try strict and slightly relaxed temperature settings for LLM consistency
    for temp in (0.0, 0.2):
        try:
            resp = ollama.chat(
                model=model,
                messages=[{"role": "system", "content": SYS_ADDR},
                          {"role": "user", "content": user_prompt}],
                options={"temperature": temp},
                format="json",
            )
            out = _safe_parse_addr_json(resp["message"]["content"])
            if out:
                return out
        except Exception:
            pass
    # Fallback to regex-based extraction if LLM fails
    return _regex_place_fallback(query)

# -----------------------------
# 3) PUBLIC ROUTER
# -----------------------------

def route_query_to_datasets_multi(query: str) -> Dict[str, object]:
    """
    Classify the query -> categories & datasets; also return 'address' list extracted by LLM.
    Returns: {"categories": [...], "confidence": float, "address": [...], "datasets": [...]}
    """

    cats, conf = _llm_classify_multi(query, LANGUAGE_MODEL)
    cats = sorted(set(cats))
    datasets = sorted({d for c in cats for d in cat_to_ds.get(c, [])})
    addr_list = _llm_extract_addresses(query)

    return {
        "categories": cats,
        "confidence": round(conf, 3),
        "address": addr_list,
        "datasets": datasets,
    }

# -----------------------------
# 4) DATASET OBJECTS & HANDLER
# -----------------------------

@dataclass(frozen=True)
class DataSet:
    """
    Represents a single data source (e.g., 'NYC OpenData PLUTO').
    """
    name: str
    categories: List[str]
    tags: List[str] = field(default_factory=list)
    priority: int = 0
    supports_point_radius: bool = True
    supports_intersections: bool = True
    supports_addresses: bool = True
    fetch_fn: Optional[Callable[..., object]] = None

    def score(
        self,
        categories: List[str],
        confidence: float,
        addresses: List[str],
        query_text: Optional[str] = None,
    ) -> Tuple[float, str]:
        matched = set(self.categories).intersection(categories)
        cat_score = float(len(matched))
        conf_boost = 0.25 * confidence * (1 if cat_score > 0 else 0)

        addr_bonus = 0.0
        if addresses:
            if self.supports_addresses:
                addr_bonus += 0.5
            if any("&" in a for a in addresses) and self.supports_intersections:
                addr_bonus += 0.25
            if self.supports_point_radius:
                addr_bonus += 0.25

        prio = 0.1 * self.priority
        total = cat_score + conf_boost + addr_bonus + prio
        reason = (
            f"cats={list(matched)} (x1), conf_boost={conf_boost:.2f}, "
            f"addr_bonus={addr_bonus:.2f}, priority={self.priority} -> score={total:.2f}"
        )
        return total, reason

@dataclass
class DataSelection:
    dataset: DataSet
    score: float
    reason: str

class DataHandler:
    """
    Registry + selector. Register DataSet objects, then call .select(...)
    to get ranked datasets + explanations.
    """
    def __init__(self) -> None:
        self._registry: Dict[str, DataSet] = {}
        self._custom_scorers: List[Callable[[DataSet, dict], Optional[Tuple[float, str]]]] = []

    # Registry
    def register(self, ds: DataSet) -> None:
        self._registry[ds.name] = ds

    def list_datasets(self) -> List[str]:
        return sorted(self._registry.keys())

    def add_custom_scorer(self, fn: Callable[[DataSet, dict], Optional[Tuple[float, str]]]) -> None:
        self._custom_scorers.append(fn)

    # Selection
    def select(
        self,
        *,
        categories: List[str],
        confidence: float,
        addresses: List[str],
        query_text: Optional[str] = None,
        top_k: int = 8,
        min_score: float = 0.5,
    ) -> List[DataSelection]:
        ctx = {
            "categories": categories,
            "confidence": confidence,
            "addresses": addresses,
            "query_text": query_text,
        }

        selections: List[DataSelection] = []
        for ds in self._registry.values():
            base_score, base_reason = ds.score(categories, confidence, addresses, query_text)

            hook_best = None
            hook_reasons: List[str] = []
            for hook in self._custom_scorers:
                try:
                    out = hook(ds, ctx)
                except Exception:
                    out = None
                if out:
                    sc, rsn = out
                    hook_reasons.append(rsn)
                    hook_best = sc if hook_best is None else max(hook_best, sc)

            final = base_score if hook_best is None else max(base_score, hook_best)
            if hook_reasons:
                base_reason += " | hooks: " + " || ".join(hook_reasons)

            if final >= min_score:
                selections.append(DataSelection(dataset=ds, score=final, reason=base_reason))

        selections.sort(key=lambda s: (-s.score, s.dataset.name))
        return selections[:top_k]

def build_default_handler(cat_to_ds_map: Dict[str, List[str]]) -> DataHandler:
    handler = DataHandler()

    # gather categories per unique dataset name
    name_to_categories: Dict[str, set] = defaultdict(set)
    for cat, names in cat_to_ds_map.items():
        for nm in names:
            name_to_categories[nm].add(cat)

    # optional priority hints
    priority_hint = {
        "NYC OpenData PLUTO": 2,
        "NYC OpenData Motor Vehicle Collisions": 2,
        "NYC OpenData Automated Traffic Volume Counts": 1,
        "DOB permits": 2,
        "DOB Job filings": 1,
        "MTA subway and other underground train lines": 1,
    }

    def _flags_for(_name: str) -> Dict[str, bool]:
        return dict(
            supports_point_radius=True,
            supports_intersections=True,
            supports_addresses=True,
        )

    for name, cats in name_to_categories.items():
        handler.register(
            DataSet(
                name=name,
                categories=sorted(cats),
                priority=priority_hint.get(name, 0),
                **_flags_for(name),
            )
        )

    # Example custom scorer
    def poi_bias(ds: DataSet, ctx: dict) -> Optional[Tuple[float, str]]:
        cats = set(ctx["categories"])
        addrs = ctx["addresses"] or []
        if "Public Safety & Social Context" in cats and addrs:
            has_number = any(any(ch.isdigit() for ch in a) for a in addrs)
            if not has_number and ds.supports_point_radius:
                return (1.25, "poi_boost: POI-only + point-radius supported")
        return None

    handler.add_custom_scorer(poi_bias)
    return handler

# -----------------------------
# 5) END-TO-END EXAMPLE
# -----------------------------

def select_datasets_for_query_result(result: Dict[str, object]) -> List[Dict[str, object]]:
    """
    Convert router result -> ranked datasets with reasons.
    """
    categories = list(result.get("categories", []))
    confidence = float(result.get("confidence", 0.5))
    addresses = list(result.get("address", []))

    handler = build_default_handler(cat_to_ds)  # build once in prod; here for demo
    selections = handler.select(
        categories=categories,
        confidence=confidence,
        addresses=addresses,
        query_text=None,
        top_k=8,
        min_score=0.5,
    )
    return [{"name": s.dataset.name, "score": round(s.score, 3), "reason": s.reason} for s in selections]

if __name__ == '__main__':
    # Example query
    q = "Are there asbestos filings or air quality complaints near 45-10 21st Street in Queens?"
    res = route_query_to_datasets_multi(q)
    ranked = select_datasets_for_query_result(res)
    print("\n---------------- Example Query: ----------------")
    print("\nQuery:", q)
    print("Router Result:", res)
    print("\n---------------- Example Ranked Datasets: ----------------")
    print('Ranked Datasets:')
    print(json.dumps(ranked, indent=2))