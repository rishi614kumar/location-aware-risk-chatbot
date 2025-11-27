from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

import pandas as pd
from sodapy import Socrata

from config.logger import logger
from config import settings


class SocrataClient:
    """Socrata Adapater to connect and fetch data from NYC Open Data"""
    def __init__(
        self,
        *,
        domain: Optional[str] = None,
        app_token: Optional[str] = None,
    ) -> None:
        domain = domain or settings.DEFAULT_SOCRATA_DOMAIN
        app_token 
        self.client = Socrata(domain, None)

    def fetch(self, dataset_id: str, *, where: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        params: Dict[str, Any] = {}
        if where:
            params["where"] = where
        if limit is not None:
            params["limit"] = limit

        retries = 3
        delay = 1.0
        last_err: Optional[Exception] = None

        for attempt in range(retries):
            try:
                # Currently all data can be fetched into a pd.Dataframe
                # Note: Can potentially fetch geopandas dataframe
                records = self.client.get(dataset_id, **params)
                return pd.DataFrame.from_records(records)
            except Exception as exc:  # pragma: no cover - network dependent
                last_err = exc
                msg = str(exc)
                if "503" in msg or "throttling" in msg.lower():
                    logger.warning(
                        "Attempt %s/%s 503/throttle for %s; retrying in %.1fs",
                        attempt + 1,
                        retries,
                        dataset_id,
                        delay,
                    )
                    time.sleep(delay)
                    delay *= 2
                    continue
                logger.error("Non-retryable Socrata error: %s", exc)
                break

        logger.error("Failed to fetch %s after %s attempts: %s", dataset_id, retries, last_err)
        return pd.DataFrame()
