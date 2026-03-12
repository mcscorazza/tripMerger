import urllib.request
import string
import json

# ==========================================
# 1. ENCURTADOR BASE62
# ==========================================
def to_base62(num):
    chars = string.digits + string.ascii_uppercase + string.ascii_lowercase
    base = len(chars)
    res = ""
    while num > 0:
        res = chars[num % base] + res
        num //= base
    return res if res else "0"


# ==========================================
# 2. GEOLOCALIZAÇÃO REVERSA (Nominatim)
# ==========================================
def get_city_name(lat, lng):
    url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lng}"
    req = urllib.request.Request(url, headers={'User-Agent': 'TripMergerLambda/1.0'})
    try:
        with urllib.request.urlopen(req, timeout=3) as response:
            data = json.loads(response.read().decode('utf-8'))
            address = data.get('address', {})
            cidade = address.get('city') or address.get('town') or address.get('village') or address.get('municipality')
            return cidade if cidade else "Localização Desconhecida"
    except Exception as e:
        print(f"Erro na geolocalização para {lat},{lng}: {e}")
        return "Localização Desconhecida"
    
