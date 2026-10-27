import json
import urllib.request

url = "https://datos.gob.cl/api/3/action/datastore_search?resource_id=060a0a55-13e6-4275-8ec6-36d9346ff080&limit=1"
req = urllib.request.Request(
    url,
    headers={"User-Agent": "Mozilla/5.0"}
)

with urllib.request.urlopen(req) as resp:
    data = json.loads(resp.read().decode("utf-8"))
    records = data.get("result", {}).get("records", [])
    if records:
        print("Columnas encontradas:")
        for col in records[0].keys():
            print(f"- {col}")
    else:
        print("No se encontraron registros.")