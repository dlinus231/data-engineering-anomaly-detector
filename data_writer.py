from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json, csv, os

sandbox_url = 'https://sandbox-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
prod_url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
parameters = {
  'start':'1',
  'limit':'5000',
#   'limit':'100',
  'convert':'USD'
}
headers = {
  'Accepts': 'application/json',
  'X-CMC_PRO_API_KEY': 'e884619fcba941d6b9bc174606cde615',
}

session = Session()
session.headers.update(headers)

def extract_columns(keys_to_extract, crypto_dict):

    def get_nested_value(d, key):
        for p in key.split('.'):
            if d is None:
                return None
            d = d.get(p)
        return d

    return {key: get_nested_value(crypto_dict, key) for key in keys_to_extract}

def append_to_csv(json_obj, filePath="sandbox.csv"):
    keys_to_extract = [
        'id', 
        'name', 
        'symbol',
        'quote.USD.price',
        'quote.USD.last_updated',
        'quote.USD.volume_24h',
        'quote.USD.volume_change_24h',
        'quote.USD.percent_change_1h',
        'quote.USD.percent_change_24h'
    ]

    file_exists = os.path.exists(filePath)
    file_empty = (not file_exists) or os.path.getsize(filePath) == 0

    with open(filePath, "a", newline="") as csvfile:  # append mode
        writer = csv.DictWriter(csvfile, fieldnames=keys_to_extract)

        # Only write header if file is new or empty
        if file_empty:
            writer.writeheader()

        for crypto_dict in json_obj['data']:
            flattened_crypto_dict = extract_columns(keys_to_extract, crypto_dict)
            writer.writerow(flattened_crypto_dict)

try:
  response = session.get(prod_url, params=parameters)
  json_obj = json.loads(response.text)
  append_to_csv(json_obj, filePath="prod2.csv")

#   pretty_string = json.dumps(json_obj, indent=4)
#   print(pretty_string)
except (ConnectionError, Timeout, TooManyRedirects) as e:
  print(e)