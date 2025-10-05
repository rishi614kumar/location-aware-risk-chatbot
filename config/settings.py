from dotenv import load_dotenv
import os

load_dotenv()

GEOCLIENT_API_KEY= os.getenv("GEOCLIENT_API_KEY")
MAPPLUTO_GDB_PATH = os.getenv("MAPPLUTO_GDB_PATH")

def check_env():
    missing = [k for k, v in globals().items() if k.isupper() and v is None]
    if missing:
        print("Missing environment variables:", missing)
    else:
        print("All environment variables loaded properly")

if __name__ == "__main__":
    check_env()