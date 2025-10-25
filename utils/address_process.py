import re

def parse_address(addr):
    if not isinstance(addr, str) or not addr.strip():
        return {
            'format': 'unknown',
            'unit': None,
            'street_number': None,
            'street_name': None,
            'lot_number': None,
            'suburb': None,
            'state': None,
            'postcode': None
        }

    s = addr.replace('\n', ' ').replace('\r', ' ')
    s = re.sub(r'\s+', ' ', s).strip()

    # --- Format 3: Unit format (e.g. 8E/47 Herdsman Parade, WEMBLEY WA 6014)
    m_unit = re.match(
        r'(?i)^\s*([\w-]+)\/(\d+(?:-\d+)?[A-Za-z]?)\s+([\w\s\'\-\.]+?),\s*'
        r'([A-Za-z\s]+)\s+([A-Z]{2,3})\s+(\d{4})\s*$',
        s
    )

    if m_unit:
        return {
            'format': 'unit_format',
            'unit': m_unit.group(1),
            'street_number': m_unit.group(2),
            'street_name': m_unit.group(3).strip().title(),
            'lot_number': None,
            'suburb': m_unit.group(4).strip().title(),
            'state': m_unit.group(5),
            'postcode': m_unit.group(6)
        }

    # --- Format 2: Lot format (e.g. Lot 110 Stone Boulevard, RIVERBEND QLD 4280)
    m_lot = re.match(
        r'(?i)^\s*lot\s+(\d+)\s+([\w\s\'\-\.]+?),\s*([A-Za-z\s]+)\s+([A-Z]{2,3})\s+(\d{4})\s*$',
        s
    )
    if m_lot:
        return {
            'format': 'lot_format',
            'unit': None,
            'street_number': None,
            'street_name': m_lot.group(2).strip().title(),
            'lot_number': int(m_lot.group(1)),
            'suburb': m_lot.group(3).strip().title(),
            'state': m_lot.group(4),
            'postcode': m_lot.group(5)
        }

    # --- Format 1: Simple format (e.g. 145 Jeune Drive, ACTON PARK TAS 7170)
    m_simple_full = re.match(
        r'^\s*(?P<street_number>\d+[A-Za-z]?)\s+'
        r'(?P<street_name>[\w\s\'\-\.]+?)'                # tên đường chính
        r'(?:\s*\((?P<estate>[^)]+)\))?,\s*'              # (tùy chọn) tên estate trong ngoặc
        r'(?P<suburb>[A-Za-z\s]+)\s+'
        r'(?P<state>[A-Z]{2,3})\s+'
        r'(?P<postcode>\d{4})\s*$',
        s
    )

    if m_simple_full:
        return {
            'format': 'simple_format',
            'unit': None,
            'street_number': m_simple_full.group('street_number'),
            'street_name': m_simple_full.group('street_name').strip().title(),
            'lot_number': None,
            'suburb': m_simple_full.group('suburb').strip().title(),
            'state': m_simple_full.group('state'),
            'postcode': m_simple_full.group('postcode')
        }

    # --- Fallback: chỉ có suburb, state, postcode (e.g. GILSTON QLD 4211)
    m_simple_short = re.match(
        r'^\s*(?P<suburb>[A-Za-z\s]+)\s+(?P<state>[A-Z]{2,3})\s+(?P<postcode>\d{4})\s*$',
        s
    )
    if m_simple_short:
        return {
            'format': 'simple_format',
            'unit': None,
            'street_number': None,
            'street_name': None,
            'lot_number': None,
            'suburb': m_simple_short.group('suburb').strip().title(),
            'state': m_simple_short.group('state'),
            'postcode': m_simple_short.group('postcode')
        }

    # --- Không khớp format nào ---
    return {
        'format': 'unknown',
        'unit': None,
        'street_number': None,
        'street_name': None,
        'lot_number': None,
        'suburb': None,
        'state': None,
        'postcode': None
    }
