from dotenv import load_dotenv
import os

load_dotenv()

GEOCLIENT_API_KEY= os.getenv("GEOCLIENT_API_KEY")
MAPPLUTO_GDB_PATH = os.getenv("MAPPLUTO_GDB_PATH")
LION_GDB_PATH = os.getenv("LION_GDB_PATH")

# STREET_SPAN SETTINGS
MAX_BUFFER_FT = 120  # Maximum buffer distance in feet
MIN_BUFFER_FT = 10   # Minimum buffer distance in feet
DEFAULT_BUFFER_INCREMENT_FT = 10  # Default increment to add to street width
DEFAULT_BUFFER_FT = 30  # Default buffer distance when street width is unknown
# If we don’t know the street width, assume DEFAULT_BUFFER_FT ft. Otherwise, take the width plus DEFAULT_BUFFER_INCREMENT_FT ft of margin, but keep it between MIN_BUFFER_FT and MAX_BUFFER_FT ft total.
def check_env():
    missing = [k for k, v in globals().items() if k.isupper() and v is None]
    if missing:
        print("Missing environment variables:", missing)
    else:
        print("All environment variables loaded properly")

if __name__ == "__main__":
    check_env()