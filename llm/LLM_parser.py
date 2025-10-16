# ============================================================
# NYC Risk Router: LLM Classification, Address Extraction,
#                  and DataSet Selection
# ============================================================

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional, Protocol, Tuple
from llm.DataHandler import DataHandler
from dotenv import load_dotenv

from llm.LLM_interface import ChatBackend, make_backend
from llm.DataHandler import cat_to_ds


# -----------------------------
# 0) CONFIG & STATIC MAPPINGS
# -----------------------------
load_dotenv()

BOROUGHS: Tuple[str, ...] = ("Queens", "Manhattan", "Bronx", "Staten Island", "Brooklyn")

_BOROUGH_ALIASES: Dict[str, str] = {
    "queens": "Queens",
    "manhattan": "Manhattan",
    "the bronx": "Bronx",
    "bronx": "Bronx",
    "brooklyn": "Brooklyn",
    "bk": "Brooklyn",
    "staten island": "Staten Island",
    "staten-island": "Staten Island",
    "si": "Staten Island",
}

MAIN_CATS: List[str] = list(cat_to_ds.keys())


class ChatModel(Protocol):
    """
    Minimal protocol for pluggable chat model clients.
    """

    def request(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.0,
        response_format: str = "json",
    ) -> str:
        ...


class BackendChatModel:
    """
    Chat model client backed by the shared LLM backend interface.

    The default behavior mirrors `make_backend()` which reads `LLM_PROVIDER`,
    `LLM_MODEL`, and provider-specific environment variables. For Gemini, we
    also accept `GEMINI_KEY` (in addition to `GEMINI_API_KEY`) so existing env
    files continue to work.
    """

    def __init__(
        self,
        backend: Optional[ChatBackend] = None,
        *,
        provider: Optional[str] = None,
        model_name: Optional[str] = None,
        backend_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._prebuilt_backend = backend
        self._provider = provider
        self._model_name = model_name
        self._backend_kwargs = dict(backend_kwargs or {})

        # Backwards compat: allow GEMINI_KEY alongside GEMINI_API_KEY.
        if "api_key" not in self._backend_kwargs:
            gemini_key = os.getenv("GEMINI_KEY")
            if gemini_key:
                self._backend_kwargs["api_key"] = gemini_key

    def request(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.0,
        response_format: str = "json",
    ) -> str:
        if self._prebuilt_backend is not None:
            backend = self._prebuilt_backend
            backend.reset()
            backend.start(system_instruction=system_prompt)
            return backend.send(user_prompt)

        kwargs: Dict[str, Any] = dict(self._backend_kwargs)
        generation_config = dict(kwargs.get("generation_config", {}) or {})
        if temperature is not None:
            generation_config["temperature"] = temperature
        if response_format == "json":
            generation_config.setdefault("response_mime_type", "application/json")
        if generation_config:
            kwargs["generation_config"] = generation_config

        backend = make_backend(
            provider=self._provider,
            model_name=self._model_name,
            **kwargs,
        )
        backend.start(system_instruction=system_prompt)
        return backend.send(user_prompt)


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
{"categories": ["<labels>"], "confidence": <0..1>, "borough": "<borough names>"}

Rules:
- Traffic, collisions, congestion, road closures, counts, speeds, hotspots -> Transportation & Traffic
- DOB permits, job filings, street construction, water/sewer permits -> Construction & Permitting
- Zoning, land use, city-owned property, districts -> Zoning & Land Use
- Flood/air/health exposure, sewer systems, population health -> Environmental & Health Risks
- Crime, safety, hydrants, social context -> Public Safety & Social Context
- Comparing between two sites (more/less, better/worse, higher/lower) -> include Comparative Site Queries plus other relevant labels
- Borough names must be one of: Queens, Manhattan, Bronx, Staten Island, Brooklyn. If the user mentions a borough, preserve it. 
    Otherwise, pick the best borough based on the query or if it is a comparative query, then include 2 or more depending on the query.
""".strip()

FEWSHOTS_MULTI: List[Tuple[str, dict]] = [
    ("Where are the top traffic accident hotspots within 500 feet of 163rd Street?",
     {"categories": ["Transportation & Traffic"], "confidence": 0.85, "borough": "Bronx"}),
    ("Any active DOB permits near 10 Jay St?",
     {"categories": ["Construction & Permitting"], "confidence": 0.85, "borough": "Brooklyn"}),
    ("Is this parcel in a historic district and what’s the zoning?",
     {"categories": ["Zoning & Land Use"], "confidence": 0.80, "borough": "Manhattan"}),
    ("Any flood or sewer risk around 123 Main St?",
     {"categories": ["Environmental & Health Risks"], "confidence": 0.80, "borough": "Queens"}),
    ("Where are the nearest fire hydrants near Borough Hall?",
     {"categories": ["Public Safety & Social Context"], "confidence": 0.75, "borough": "Brooklyn"}),
    ("Compare zoning and environmental risks for 149th Street & Grand Concourse versus 181st Street & St. Nicholas Avenue.",
     {"categories": ["Comparative Site Queries", "Zoning & Land Use", "Environmental & Health Risks"], "confidence": 0.88, "borough": "Manhattan"}),
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


class LLMParser:
    """
    Parser that coordinates category classification and address extraction via an LLM client.
    """

    def __init__(
        self,
        model_client: ChatModel,
        *,
        category_to_datasets: Optional[Dict[str, List[str]]] = None,
        category_system_prompt: str = SYS_MULTI,
        address_system_prompt: str = SYS_ADDR,
        address_temperatures: Tuple[float, ...] = (0.0, 0.2),
        default_borough: str = "Manhattan",
    ) -> None:
        self._model_client = model_client
        self._cat_to_ds = category_to_datasets or cat_to_ds
        self._category_system_prompt = category_system_prompt
        self._address_system_prompt = address_system_prompt
        self._address_temperatures = address_temperatures
        self._default_borough = self._normalize_borough(default_borough) or BOROUGHS[0]

    def _normalize_borough(self, borough: Optional[str]) -> Optional[str]:
        if not borough:
            return None
        text = str(borough).strip().lower()
        if not text:
            return None
        text = text.replace(",", " ").replace("borough of", "").strip()
        if text in _BOROUGH_ALIASES:
            return _BOROUGH_ALIASES[text]
        for candidate in BOROUGHS:
            if text == candidate.lower():
                return candidate
        # allow forms like "the bronx"
        normalized = text.replace("the ", "")
        if normalized in _BOROUGH_ALIASES:
            return _BOROUGH_ALIASES[normalized]
        return None

    def _infer_borough_from_query(self, query: str) -> str:
        lowered = (query or "").lower()
        for alias, canonical in _BOROUGH_ALIASES.items():
            if alias and alias in lowered:
                return canonical
        for borough in BOROUGHS:
            if borough.lower() in lowered:
                return borough
        return self._default_borough

    def classify_query(self, query: str) -> Tuple[List[str], float, str]:
        """
        Classify the query into one or more categories and return confidence and borough.
        """
        user_prompt = _build_user_prompt_multi(query)
        try:
            raw = self._model_client.request(
                system_prompt=self._category_system_prompt,
                user_prompt=user_prompt,
                temperature=0.0,
                response_format="json",
            )
            data = json.loads(raw)
        except Exception:
            return (["Other"], 0.5, self._infer_borough_from_query(query))

        raw_cats = data.get("categories")
        if not raw_cats:
            single = data.get("category")
            raw_cats = [single] if single else []

        cats = [c for c in raw_cats if c in self._cat_to_ds] or ["Other"]
        conf = float(data.get("confidence", 0.5))
        borough = self._normalize_borough(data.get("borough"))
        if borough is None:
            borough = self._infer_borough_from_query(query)
        return cats, conf, borough

    def extract_addresses(self, query: str) -> List[str]:
        """
        Extract addresses/POIs with LLM call and regex fallback.
        """
        user_prompt = _build_user_prompt_addr(query)
        for temp in self._address_temperatures:
            try:
                raw = self._model_client.request(
                    system_prompt=self._address_system_prompt,
                    user_prompt=user_prompt,
                    temperature=temp,
                    response_format="json",
                )
                out = _safe_parse_addr_json(raw)
                if out:
                    return out
            except Exception:
                continue
        return _regex_place_fallback(query)

    def route_query_to_datasets(self, query: str) -> Dict[str, object]:
        """
        Classify and extract addresses, then map to dataset names.
        """
        cats_raw, conf, borough = self.classify_query(query)
        cats = sorted(set(cats_raw))
        dataset_names = sorted({d for c in cats for d in self._cat_to_ds.get(c, [])})
        addr_list = self.extract_addresses(query)
        return {
            "categories": cats,
            "confidence": round(conf, 3),
            "borough": borough,
            "address": addr_list,
            "dataset_names": dataset_names,
        }

# -----------------------------
# 3) PUBLIC ROUTER
# -----------------------------

_DEFAULT_PARSER: Optional[LLMParser] = None


def get_default_parser() -> LLMParser:
    """
    Lazily build and reuse a parser backed by the project LLM backend.
    """
    global _DEFAULT_PARSER
    if _DEFAULT_PARSER is None:
        _DEFAULT_PARSER = LLMParser(BackendChatModel())
    return _DEFAULT_PARSER


def route_query_to_datasets_multi(
    query: str,
    *,
    parser: Optional[LLMParser] = None,
) -> Dict[str, object]:
    """
    Classify the query -> categories & dataset names; also return address list and borough.
    Returns: {"categories": [...], "confidence": float, "borough": str, "address": [...], "dataset_names": [...]}
    """
    active_parser = parser or get_default_parser()
    return active_parser.route_query_to_datasets(query)


if __name__ == '__main__':

    print("------------------Example 1: Environmental & Health Risks------------------")
    example_query = "Are there asbestos filings or air quality complaints near 45-10 21st Street in Queens?"
    parser = get_default_parser()
    result = parser.route_query_to_datasets(example_query)
    print("\nQuery:", example_query)
    print("Router Result:", json.dumps(result, indent=2))

    handler = DataHandler(result["dataset_names"])
    first_dataset = getattr(handler, "d1", None)
    if first_dataset:
        print("\nFirst Dataset:", first_dataset.name)
        print("Description:", first_dataset.description)

    second_dataset = getattr(handler, 'd2', None)
    if second_dataset:
        print("\nSecond Dataset:", second_dataset.name)
        print("Description:", second_dataset.description)

    print("\n------------------Example 2: Comparative Site Queries------------------")
    example_query = 'Which location has fewer open permits: Jamaica Avenue in Queens or Broadway in Upper Manhattan?”'
    result = parser.route_query_to_datasets(example_query)
    print("\nQuery:", example_query)
    print("Router Result:", json.dumps(result, indent=2))

    handler = DataHandler(result["dataset_names"])
    first_dataset = getattr(handler, "d1", None)
    if first_dataset:
        print("\nFirst Dataset:", first_dataset.name)
        print("Description:", first_dataset.description)

    second_dataset = getattr(handler, 'd2', None)
    if second_dataset:
        print("\nSecond Dataset:", second_dataset.name)
        print("Description:", second_dataset.description)