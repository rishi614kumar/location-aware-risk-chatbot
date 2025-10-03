import requests

#API_KEY = ####
BASE_URL = "https://api.nyc.gov/geoclient/v2"

params = {
    "houseNumber": "120",
    "street": "Broadway",
    "borough": "Manhattan"
}

headers = {
    "Ocp-Apim-Subscription-Key": API_KEY
}

response = requests.get(f"{BASE_URL}/address.json", params=params, headers=headers)

print("Status:", response.status_code)
print("Response:", response.json())
