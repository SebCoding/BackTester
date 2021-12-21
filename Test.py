import pandas as pd

url = 'https://api.kraken.com/0/public/OHLC?pair=BTCUSDT'
import requests
import json

# Execute GET request and store response
# response_data = requests.get(url)

# Format data as a raw json file
# data = response_data.json()['result']
# print(data)

# Add indents to JSON and output to screen
#print(json.dumps(data["prices"], indent=3))
[4]
# Pandas read_json() accepts a URL directly, no need

import requests

result = requests.get("https://api.kraken.com/0/public/OHLC?pair=BTCUSDT").json()['result']

df = pd.DataFrame(result)

print(result)


