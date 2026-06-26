import urllib.request
import string
import json
import time

_city_cache = {}

def to_base62(num):
    chars = string.digits + string.ascii_uppercase + string.ascii_lowercase
    base = len(chars)
    res = ""
    while num > 0:
        res = chars[num % base] + res
        num //= base
    return res if res else "0"

def get_city_name(lat, lng, location_type):
    cache_key = (round(float(lat), 3), round(float(lng), 3))
    
    if cache_key in _city_cache:
        cidade = _city_cache[cache_key]
        print(f"\n     📍 Cidade recuperada do Cache ({location_type}): {cidade}")
        return cidade

    url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lng}"
    req = urllib.request.Request(url, headers={'User-Agent': 'TripMergerPipeline/1.0 (contacto@empresa.com)'})
    
    try:
        time.sleep(1) 
        with urllib.request.urlopen(req, timeout=3) as response:
            data = json.loads(response.read().decode('utf-8'))
            address = data.get('address', {})
            cidade = address.get('city') or address.get('town') or address.get('village') or address.get('municipality')
            
            if not cidade:
                print("\n     ❌ Cidade não encontrada na API!!!")
                cidade = "Localização Desconhecida"
            else:
                print(f"\n     📍 Cidade encontrada via API ({location_type}): {cidade}")
                
            _city_cache[cache_key] = cidade
            return cidade
    except Exception as e:
        print(f"     ❌ Erro na geolocalização para {lat},{lng}: {e}")
        return "Localização Desconhecida"