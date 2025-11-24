# ============================================================
# NYC Risk Router: LLM Classification, Address Extraction,
#                  and DataSet Selection
# ============================================================

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional, Protocol, Tuple
from dotenv import load_dotenv
from llm.LLMInterface import ChatBackend, Chat, make_backend
from scripts.DataHandler import cat_to_ds
from config import settings


# -----------------------------
# 0) CONFIG & STATIC MAPPINGS
# -----------------------------
load_dotenv()  # ensure GEMINI_KEY / provider overrides are available to downstream clients

BOROUGHS: Tuple[str, ...] = settings.BOROUGHS
_BOROUGH_ALIASES: Dict[str, str] = settings._BOROUGH_ALIASES
MAIN_CATS: List[str] = settings.MAIN_CATS
ALL_DATASETS: List[str] = settings.ALL_DATASETS


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

        self._api_key = (
            self._backend_kwargs.get("api_key")
            or os.getenv("GEMINI_API_KEY")
        )
        if self._api_key and "api_key" not in self._backend_kwargs:
            self._backend_kwargs["api_key"] = self._api_key

    def request(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.0,
        response_format: str = "json",
    ) -> str:
        if self._prebuilt_backend is not None:
            chat = Chat(self._prebuilt_backend)
            chat.start(system_instruction=system_prompt)
            response = chat.ask(user_prompt)
            chat.reset()
            return response

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
        chat = Chat(backend)
        chat.start(system_instruction=system_prompt)
        response = chat.ask(user_prompt)
        chat.reset()
        return response


# -----------------------------
# 1) LLM CATEGORY CLASSIFIER
# -----------------------------

SYS_MULTI = settings.SYS_MULTI
FEWSHOTS_MULTI = settings.FEWSHOTS_MULTI

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

SYS_ADDR = settings.SYS_ADDR
FEWSHOTS_ADDR = settings.FEWSHOTS_ADDR


def _build_user_prompt_addr(query: str) -> str:
    """
    Builds a few-shot prompt for the LLM address extractor using example queries and JSON outputs.
    """
    parts: List[str] = []
    for u, js in FEWSHOTS_ADDR:
        parts += [f'User: "{u}"', "JSON:", json.dumps(js, ensure_ascii=False), ""]
    parts += [f'User: "{query}"', "JSON:"]
    return "\n".join(parts)

def _record_from_raw(raw: str) -> Dict[str, str]:
    """
    Build a structured address record from a raw substring.
    """
    text = (raw or "").strip()
    if not text:
        return {"house_number": "", "street_name": "", "borough": "", "raw": "", "notes": ""}

    house = ""
    street = text
    match = re.match(r"^\s*(\d+)\s+(.*)$", text)
    if match:
        house = match.group(1).strip()
        street = match.group(2).strip()

    borough = ""
    lowered = text.lower()
    for alias, canonical in _BOROUGH_ALIASES.items():
        if alias and alias in lowered:
            borough = canonical
            break
    else:
        for b in BOROUGHS:
            if b.lower() in lowered:
                borough = b
                break

    return {
        "house_number": house,
        "street_name": street,
        "borough": borough,
        "raw": text,
        "notes": "",
    }


def _expand_between_segments(record: Dict[str, str]) -> List[Dict[str, str]]:
    text = (record.get("raw") or record.get("street_name") or "").strip()
    if not text:
        return [record]
    match = _BETWEEN_RE.search(text)
    if not match:
        return [record]

    def _clean(value: str) -> str:
        return re.sub(r"[.,]$", "", value.strip())

    main = _clean(match.group("main"))
    cross1 = _clean(match.group("cross1"))
    cross2 = _clean(match.group("cross2"))
    if not (main and cross1 and cross2):
        return [record]

    base_notes = record.get("notes") or f"{main} between {cross1} and {cross2}".strip()
    raw_text = record.get("raw") or text
    borough = record.get("borough", "")

    expanded: List[Dict[str, str]] = []
    for cross in (cross1, cross2):
        expanded.append(
            {
                "house_number": "",
                "street_name": f"{main} & {cross}",
                "borough": borough,
                "raw": raw_text,
                "notes": base_notes,
            }
        )
    return expanded


def _normalize_dedupe(seq: Optional[List[Any]]) -> List[Dict[str, str]]:
    """
    Clean and deduplicate address objects while enforcing the required keys.
    """
    seen: set = set()
    out: List[Dict[str, str]] = []
    for item in seq or []:
        if isinstance(item, dict):
            base_record = {
                "house_number": str(item.get("house_number", "") or "").strip(),
                "street_name": str(item.get("street_name", "") or "").strip(),
                "borough": str(item.get("borough", "") or "").strip(),
                "raw": str(item.get("raw", "") or "").strip(),
                "notes": str(item.get("notes", "") or "").strip(),
            }
            if not base_record["raw"]:
                base_record["raw"] = base_record["street_name"] or base_record["house_number"]
        else:
            base_record = _record_from_raw(str(item))

        for record in _expand_between_segments(base_record):
            key = (
                record["house_number"],
                record["street_name"],
                record["borough"],
                record["raw"],
                record["notes"],
            )
            if record["raw"] and key not in seen:
                seen.add(key)
                out.append(record)
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
_BETWEEN_RE = re.compile(
    r"(?P<main>[^,]+?)\s+between\s+(?P<cross1>[^,]+?)\s+(?:and|&)\s+(?P<cross2>[^,]+)",
    re.IGNORECASE,
)

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
# End of heuristics: if the LLM misbehaves, this keeps address extraction resilient.


def _safe_parse_addr_json(s: str) -> List[Dict[str, str]]:
    """
    Safely parses a JSON string to extract normalized address records.
    """
    text = (s or "").strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    try:
        data = json.loads(text)
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

    def classify_query(self, query: str) -> Tuple[List[str], List[str], float, str]:
        """
        Classify the query into one or more categories and return datasets, confidence, and borough.
        """
        user_prompt = _build_user_prompt_multi(query)
        try:
            raw = self._model_client.request(
                system_prompt=self._category_system_prompt,
                user_prompt=user_prompt,
                temperature=0.0,
                response_format="json",
            )
            print("✅ Classifier LLM call succeeded")
            data = json.loads(raw)
        except Exception as exc:
            print(f"⚠️ Classifier LLM call failed: {exc}")
            fallback_borough = self._infer_borough_from_query(query)
            return (["Other"], [], 0.5, fallback_borough)  # fall back gracefully when the LLM fails

        raw_cats = data.get("categories")
        if not raw_cats:
            single = data.get("category")
            raw_cats = [single] if single else []

        cats = [c for c in raw_cats if c in self._cat_to_ds] or ["Other"]

        raw_datasets = data.get("datasets") or data.get("dataset")
        if isinstance(raw_datasets, str):
            raw_datasets = [raw_datasets]
        if not isinstance(raw_datasets, list):
            raw_datasets = []
        datasets = []
        for ds in raw_datasets:
            name = (ds or "").strip()
            if name in ALL_DATASETS and name not in datasets:
                datasets.append(name)
        if not datasets:
            # fallback: accumulate from category mapping, prioritise highest-category match
            mapped = []
            for cat in cats:
                for ds in self._cat_to_ds.get(cat, []):
                    if ds not in mapped:
                        mapped.append(ds)
            datasets = mapped[:5]

        conf = float(data.get("confidence", 0.5))
        borough = self._normalize_borough(data.get("borough"))
        if borough is None:
            borough = self._infer_borough_from_query(query)
        return cats, datasets, conf, borough

    def extract_addresses(self, query: str) -> List[Dict[str, str]]:
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
                print(f"✅ Address LLM call succeeded (temperature={temp})")
                out = _safe_parse_addr_json(raw)
                if out:
                    return out
            except Exception as exc:
                print(f"⚠️ Address LLM call failed (temperature={temp}): {exc}")
                continue  # keep trying with the next temperature or drop to regex fallback
        print("⚠️ Falling back to regex-based address extraction")
        return _regex_place_fallback(query)

    def route_query_to_datasets(self, query: str) -> Dict[str, object]:
        """
        Classify and extract addresses, then map to dataset names.
        """
        cats_raw, datasets_llm, conf, borough = self.classify_query(query)
        cats = sorted(set(cats_raw))
        valid_llm = [ds for ds in datasets_llm if ds in ALL_DATASETS]
        if valid_llm:
            dataset_names = sorted(valid_llm)
        else:
            dataset_names = sorted({d for c in cats for d in self._cat_to_ds.get(c, [])})
        addr_list = self.extract_addresses(query)
        return {
            "categories": cats,
            "confidence": round(conf, 3),
            "address": addr_list,
            "dataset_names": dataset_names,
        }

# -----------------------------
# 3) PUBLIC ROUTER
# -----------------------------

_DEFAULT_PARSER: Optional[LLMParser] = None


def get_default_parser(
    *,
    backend: Optional[ChatBackend] = None,
    provider: Optional[str] = None,
    model_name: Optional[str] = None,
    backend_kwargs: Optional[Dict[str, Any]] = None,
) -> LLMParser:
    """
    Build a parser backed by the shared LLM backend.

    If no parameters are provided, returns a cached default instance. Supplying
    a backend or provider/model overrides the cache and constructs a new parser.
    """
    if backend or provider or model_name or backend_kwargs:
        model_client = BackendChatModel(
            backend=backend,
            provider=provider,
            model_name=model_name,
            backend_kwargs=backend_kwargs,
        )
        return LLMParser(model_client)

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
    result = active_parser.route_query_to_datasets(query)

    categories = set(result.get("categories", []))
    if "Infrastructure Projects" in categories:
        result["dataset_names"] = ["Active Projects"]
        result["address"] = []

    return result
