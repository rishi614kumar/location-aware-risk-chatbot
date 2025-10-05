from typing import Dict, Any, List, Optional

def get_bbls_from_lion_span(lion_id: str, buffer_ft: float = 25.0) -> List[str]:
    """
    Given a LION segment (or span) ID, return nearby BBLs.
    - Accept a buffer distance so teams can tune relevance.
    - Return 10-digit BBL strings.
    """
    # TODO: implement with LION centerlines + spatial buffer + intersect with PLUTO
    return []

def get_lion_span_from_bbl(bbl: str) -> Optional[str]:
    """
    Optional reverse: BBL -> 'dominant' LION segment/span.
    Return a stable identifier (e.g., LION key).
    """
    return None
