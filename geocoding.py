import pandas as pd
import re
import time
import os
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="house_price_predictor")

def build_clean_address(row):
    parts = []

    def safe(x):
        return str(x).strip() if pd.notna(x) and str(x).lower() != "nan" else None

    street_number = safe(row.get('street_number'))
    street_name = safe(row.get('street_name'))
    lot_number = safe(row.get('lot_number'))
    suburb = safe(row.get('suburb'))
    state = safe(row.get('state'))
    postcode = safe(row.get('postcode'))

    if street_number:
        parts.append(street_number)
    elif lot_number:
        parts.append(f"Lot {int(float(lot_number))}")

    if street_name:
        parts.append(street_name)
    if suburb:
        parts.append(suburb)
    if state:
        parts.append(state)
    if postcode:
        parts.append(postcode)

    addr = ", ".join(parts)
    if "Australia" not in addr:
        addr += ", Australia"

    addr = re.sub(r'\s+', ' ', addr).strip()
    return addr

def safe_geocode(address):
    try:
        if not address or not isinstance(address, str):
            return None, None
        loc = geolocator.geocode(address, timeout=10)
        time.sleep(1)  
        if loc:
            return loc.latitude, loc.longitude
        return None, None
    except Exception:
        return None, None


def smart_geocode(addr):
    if not addr or pd.isna(addr):
        return None, None

    lat, lon = safe_geocode(addr)
    if lat and lon:
        return lat, lon

    addr_no_lot = re.sub(r'\b[Ll]ot\s*\d+[A-Za-z\-]*,?\s*', '', addr)
    if addr_no_lot != addr:
        lat, lon = safe_geocode(addr_no_lot.strip())
        if lat and lon:
            return lat, lon

    m = re.search(r'([A-Za-z\s]+),\s*([A-Z]{2,3}),\s*(\d{4})', addr)
    if m:
        simpler = f"{m.group(1).strip()}, {m.group(2)} {m.group(3)}, Australia"
        lat, lon = safe_geocode(simpler)
        if lat and lon:
            return lat, lon

    return None, None

def process_in_batches(df, output_path, from_batch=0, batch_size=1000):
    if os.path.exists(output_path):
        print(f"File {output_path} is existed, reading file")
        existing = pd.read_csv(output_path)
        start_index = len(existing)
        df = df.iloc[start_index:].copy()
        df_result = existing
    else:
        start_index = 0
        df_result = pd.DataFrame()

    total = len(df)
    print(f"Start processing {total} rows left.")

    for i in range(0, total, batch_size):
        batch = df.iloc[i:i+batch_size].copy()
        print(f"\nBatch {i//batch_size + 1}:")

        lats, lons = [], []
        for _, row in batch.iterrows():
            addr = build_clean_address(row)
            lat, lon = smart_geocode(addr)
            lats.append(lat)
            lons.append(lon)
            print(f"{addr} → {lat}, {lon}")

        batch["lat"] = lats
        batch["lon"] = lons

        df_result = pd.concat([df_result, batch], ignore_index=True)
        df_result.to_csv(output_path, index=False)
        print(f"Saved batch {i//batch_size + 1} → {output_path}")

        time.sleep(5) 

    print("Completed")

df = pd.read_csv("dataset/cleaned/domain_realestate_cleaned.csv")
df_sold = pd.read_csv("dataset/cleaned/domain_realestate_sold_cleaned.csv")

process_in_batches(df, "dataset/geocoded/domain_realestate_geocoded.csv", from_batch=0, batch_size=1000)
process_in_batches(df_sold, "dataset/geocoded/domain_realestate_sold_geocoded.csv", from_batch=0, batch_size=1000)
