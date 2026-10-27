import json
import urllib.request

# 1. Buscar en el catálogo CKAN el recurso vigente de establecimientos de salud
search_url = "https://datos.gob.cl/api/3/action/package_search?q=establecimientos+salud+vigentes"
req = urllib.request.Request(search_url, headers={"User-Agent": "Mozilla/5.0"})

try:
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        paquetes = data.get("result", {}).get("results", [])
        
        resource_id_activo = None
        for pkg in paquetes:
            for res in pkg.get("resources", []):
                # Buscamos el recurso que tenga datastore activo
                if res.get("datastore_active"):
                    resource_id_activo = res.get("id")
                    print(f"Recurso activo encontrado: {res.get('name')} (ID: {resource_id_activo})")
                    break
            if resource_id_activo:
                break

    # 2. Si encontramos el recurso, traemos el primer registro para ver sus columnas
    if resource_id_activo:
        data_url = f"https://datos.gob.cl/api/3/action/datastore_search?resource_id={resource_id_activo}&limit=1"
        req_data = urllib.request.Request(data_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req_data) as resp_data:
            records = json.loads(resp_data.read().decode("utf-8"))["result"]["records"]
            if records:
                print("\nColumnas reales del dataset:")
                for col in records[0].keys():
                    print(f"- {col}")
    else:
        print("No se encontró un datastore activo con esa búsqueda.")

except Exception as e:
    print(f"Error durante la consulta: {e}")