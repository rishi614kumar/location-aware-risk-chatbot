import logging

# Configure the unified logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
logger = logging.getLogger("location_aware_risk_chatbot")

__all__ = ["logger"]
