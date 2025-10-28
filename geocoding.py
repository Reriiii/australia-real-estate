import pandas as pd
import re
import time
import os
import random
from functools import lru_cache
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderServiceError, GeocoderTimedOut

# ---- cấu hình geolocator: thay email bằng email của bạn nếu muốn ----
geolocator = Nominatim(user_agent="house_price_predictor (your_email@example.com)")

def build_clean_address(row):
    parts = []

    def safe(x):
        # an toàn với mọi kiểu, trả về None khi NaN hoặc 'nan' string
        if pd.isna(x):
            return None
        s = str(x).strip()
        return None if s.lower() == "nan" or s == "" else s

    street_number = safe(row.get('street_number'))
    street_name = safe(row.get('street_name'))
    lot_number = safe(row.get('lot_number'))
    suburb = safe(row.get('suburb'))
    state = safe(row.get('state'))
    postcode = safe(row.get('postcode'))

    if street_number:
        parts.append(street_number)
    elif lot_number:
        # xử lý lot_number an toàn (cả dạng "1668", "1668A", "Lot 1668", ...)
        ln = re.sub(r'(?i)^lot\s*', '', lot_number).strip()
        if re.match(r'^\d+(\.\d+)?$', ln):
            parts.append(f"Lot {int(float(ln))}")
        else:
            parts.append(f"Lot {ln}")

    if street_name:
        parts.append(street_name)
    if suburb:
        parts.append(suburb)
    if state:
        parts.append(state)
    if postcode:
        parts.append(postcode)

    addr = ", ".join(parts)
    if addr and "Australia" not in addr:
        addr += ", Australia"
    addr = re.sub(r'\s+', ' ', addr).strip()
    return addr

def safe_geocode_raw(address, timeout=10, max_retries=3):
    """Geocode với retry + bắt lỗi. Trả về (lat, lon) hoặc (None, None)."""
    if not address or not isinstance(address, str):
        return None, None

    for attempt in range(1, max_retries + 1):
        try:
            loc = geolocator.geocode(address, timeout=timeout)
            # delay ngẫu nhiên nhỏ để giảm khả năng bị rate limit
            time.sleep(random.uniform(1.0, 2.0))
            if loc:
                return loc.latitude, loc.longitude
            return None, None
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            # chờ tăng dần rồi thử lại
            wait = min(5 * attempt, 30)
            print(f"Geocode error (attempt {attempt}) for '{address}': {e}. wait {wait}s")
            time.sleep(wait)
        except Exception as e:
            # exception lạ -> trả None
            print(f"Unexpected geocode error for '{address}': {e}")
            return None, None
    return None, None

# cache cho same address (giúp tiết kiệm request)
@lru_cache(maxsize=20000)
def cached_geocode(address):
    return safe_geocode_raw(address)

def smart_geocode(addr):
    """Thử nhiều biến thể address, trả về (lat, lon) hoặc (None, None)."""
    if not addr or pd.isna(addr):
        return None, None

    # chuẩn hoá khoảng trắng
    addr = re.sub(r'\s+', ' ', addr).strip()

    # 1) thử nguyên bản
    lat, lon = cached_geocode(addr)
    if lat is not None and lon is not None:
        return lat, lon

    # 2) loại Lot nếu có
    addr_no_lot = re.sub(r'\b[Ll]ot\s*\d+[A-Za-z\-]*,?\s*', '', addr)
    addr_no_lot = re.sub(r'\s+', ' ', addr_no_lot).strip()
    if addr_no_lot and addr_no_lot != addr:
        lat, lon = cached_geocode(addr_no_lot)
        if lat is not None and lon is not None:
            return lat, lon

    # 3) thử biến thể đơn giản: "Suburb, STATE POSTCODE, Australia"
    m = re.search(r'([A-Za-z\s]+),\s*([A-Z]{2,3}),\s*(\d{4})', addr)
    if m:
        simpler = f"{m.group(1).strip()}, {m.group(2)} {m.group(3)}, Australia"
        lat, lon = cached_geocode(simpler)
        if lat is not None and lon is not None:
            return lat, lon

    # 4) thử chỉ suburb + state + postcode (nếu có)
    m2 = re.search(r'([A-Za-z\s]+),\s*([A-Z]{2,3})\s*(\d{4})?', addr)
    if m2:
        simpler2 = f"{m2.group(1).strip()}, {m2.group(2)}"
        if m2.group(3):
            simpler2 += f" {m2.group(3)}"
        simpler2 += ", Australia"
        lat, lon = cached_geocode(simpler2)
        if lat is not None and lon is not None:
            return lat, lon

    return None, None

def process_in_batches(df, output_path, from_batch=0, batch_size=1000):
    if os.path.exists(output_path):
        print(f"File {output_path} exists, reading file")
        existing = pd.read_csv(output_path)
        processed_rows = len(existing)
        df_result = existing
        print(f"Resuming from row {processed_rows}")
    else:
        processed_rows = max(0, from_batch * batch_size)
        df_result = pd.DataFrame(columns=list(df.columns) + ["lat", "lon"])
        if processed_rows > 0:
            print(f"Starting fresh but skipping first {processed_rows} rows (from_batch={from_batch})")
        else:
            print("Starting fresh from row 0")

    total = len(df)
    if processed_rows >= total:
        print(f"Nothing to do: processed_rows ({processed_rows}) >= total rows ({total})")
        return

    remaining = total - processed_rows
    print(f"Start processing {remaining} rows left (total {total}).")

    # dùng range từ processed_rows tới total, bước batch_size
    for i in range(processed_rows, total, batch_size):
        batch = df.iloc[i:i+batch_size].copy()
        print(f"\nBatch {i // batch_size + 1}: rows {i}..{i+len(batch)-1}")

        lats, lons = [], []
        for _, row in batch.iterrows():
            addr = build_clean_address(row)
            lat, lon = smart_geocode(addr)
            lats.append(lat)
            lons.append(lon)
            print(f"{addr} → {lat}, {lon}")

        batch["lat"] = lats
        batch["lon"] = lons

        # nối vào kết quả hiện có và lưu (ghi đè file để dễ resume)
        df_result = pd.concat([df_result, batch], ignore_index=True)
        df_result.to_csv(output_path, index=False)
        print(f"Saved batch {i // batch_size + 1} → {output_path}")

        # pause giữa các batch (giúp giảm áp lực)
        time.sleep(random.uniform(3.0, 6.0))

    print("Completed")

if __name__ == "__main__":
    df = pd.read_csv("dataset/cleaned/domain_realestate_cleaned.csv")
    df_sold = pd.read_csv("dataset/cleaned/domain_realestate_sold_cleaned.csv")

    process_in_batches(df, "dataset/geocoded/domain_realestate_geocoded.csv", from_batch=0, batch_size=1000)
    process_in_batches(df_sold, "dataset/geocoded/domain_realestate_sold_geocoded.csv", from_batch=0, batch_size=1000)
