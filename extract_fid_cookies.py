#!/home/ana/conda/bin/python3

import os
import sqlite3
from Cryptodome.Cipher import AES
from Cryptodome.Protocol.KDF import PBKDF2
from Cryptodome.Hash import SHA1

'AP171348_HEADER_APP_SERVICE_COOKIE', 'analytics_id', 'bm_ss', 'mboxEdgeCluster'

fid_cookie_spec = {
    '_cs_c': 1,
    '_svsid': 32,
    '_ldvid': 36,
    'AMCVS_EDCF01AC512D2B770A490D4C%40AdobeOrg': 1,
    'portsum_.csrf': 24,
    '_brkg.ap122489.equitytradeticket.csrf': 24,
    '_tradecontainer.csrf': 24,
    '_upeapp-neo.csrf': 24,
    '_fvl_neo.csrf': 24,
    'ap180806_neo.csrf': 24,
    '_ap126216-pwe.csrf': 24,
    '_neo.csrf': 24,
    '_neo_ap182051.csrf': 24,
    '_pr000132-mutual-fund-trade-ticket.csrf': 24,
    '_neo_ap185145.csrf': 24,
    '_pr110448-quick-quote.csrf': 24,
    '_ap130058-res-exp.csrf': 24,
    'AP179893_neo.csrf': 24,
    '_ga': 27,
    '_ga_GL9JN8SMCE': 47,
    '_gcl_au': 25,
    '_.csrf': 24,
    '_perfaa_neo.csrf': 24,
    'PERFAA-XSRF-TOKEN': 36,
    '_uetsid': 32,
    '_uetvid': 32,
    'dmt_x': 32,
    'FC': 409,
    'MC': 159,
    'PIT': 577,
    'RC': 206,
    'RtAzC': 748,
    'RtEntC': 162,
    'SC': 270,
    'PORTSUM_XSRF-TOKEN': 36,
    'UPEAPP-XSRF-TOKEN': 36,
    'FVL-XSRF-TOKEN': 36,
    'dmt_d': 3,
    'ap180806-XSRF-TOKEN': 36,
    'session_sctx': 32,
    'XSRF-TOKEN': 36,
    '_cs_ex': 10,
    'bm_mi': 399,
    'cvi': 152,
    'AMCV_EDCF01AC512D2B770A490D4C%40AdobeOrg': 286,
    'OptanonConsent': 251,
    'ak_bmsc': 604,
    'at_check': 4,
    'mbox': 103,
    'npt': 0,
    's_sess': 17,
    'bm_sz': 641,
    'bm_lso': 555,
    'bm_so': 541,
    'QSI_HistorySession': 655,
    'AP185145-XSRF-TOKEN': 36,
    'bm_s': 1000,
    'AP179893-XSRF-TOKEN': 36,
    'AP182051-XSRF-TOKEN': 36,
    'ATC': 43,
    'ATT': 10,
    '_abck': 1228,
    '_dd_s': 136,
    '_dd_s_v2': 127,
    'ajs_anonymous_id': 36,
    'ajs_user_id': 34,
    'bm_sv': 299,
    'dmt_g': 2,
    'dmt_t': 2,
    's_pers': 146
}

def check_assumptins(cursor):
    cursor.execute("SELECT value FROM cookies")
    assert all(row[0] == '' for row in cursor.fetchall())
    cursor.execute("SELECT encrypted_value FROM cookies")
    assert all(row[0][:3] == b'v10' for row in cursor.fetchall())

def fetch_fid_cookie_rows(cursor):
    domain = '.fidelity.com'
    cursor.execute("SELECT name, encrypted_value, host_key FROM cookies WHERE host_key LIKE ?", (f"%{domain}%",))
    rows = []
    for name, enc, h_key in cursor.fetchall():
        if h_key not in [domain, 'digital' + domain, '.digital' + domain]:
            print(f'# Ignore host {h_key}: {name}')
            continue
        if name in ['AMURCC', 'SESSION_CTX']:
            continue
        rows.append((name, enc[3:], h_key))
    return rows

def validate_fid_cookie_rows(rows):
    assert all(len(row[1])%16==0 for row in rows)

def decrypt_v10(blob):
    password=b'peanuts'
    salt = b'saltysalt'
    bsize = 16
    key_length = 16
    # value_charset = '#$%&()+-./0123456789=ABCDEFGHIJKLMNOPQRSTUVWXYZ^_abcdefghijklmnopqrstuvwxyz|~'

    derived_key = PBKDF2(password, salt, key_length, count=1, hmac_hash_module=SHA1)
    iv = blob[:bsize]
    cipher = AES.new(derived_key, AES.MODE_CBC, IV=iv)
    decrypted = cipher.decrypt(blob[bsize:])

    # PKCS#7 Padding Check
    padding_len = decrypted[-1]
    if padding_len < 1 or padding_len > bsize:
        print(f'bad padding_len {padding_len}')
        return None

    raw_data = decrypted[:-padding_len]

    # Get rid of the leading garbage blocks
    for i in range(0, len(raw_data), bsize):
        if any(x < 35 or x > 126 for x in raw_data[i:i+bsize]):
            continue
        return raw_data[i:].decode('utf-8', errors='ignore')
    return ''

def get_fid_cookies(rows):
    nv_pairs = []
    name2vlen = {}
    for name, blob, h_key in rows:
        val = decrypt_v10(blob)
        print(h_key, name, len(val), 'bytes')
        nv_pairs.append(f'{name}={val}')
        name2vlen[name] = len(val)
    s0 = set(fid_cookie_spec)
    s1 = set(name2vlen)
    missing = s0 - s1
    unknown = s1 - s0
    print('Cookie spec:', len(s0), 'names, found:', len(s0.intersection(s1)))
    print('Missing:', missing, 'Unknown:', unknown)
    defects = len(missing)
    for name in sorted(s0.intersection(s1)):
        if fid_cookie_spec[name] != name2vlen[name]:
            if name == 'QSI_HistorySession' and fid_cookie_spec[name] - name2vlen[name] <= 29:
                continue
            if name == 's_pers' and fid_cookie_spec[name] - name2vlen[name] <= 10:
                continue
            print(name, fid_cookie_spec[name], 'vs', name2vlen[name])
            defects += 1
    if defects >= 5:
        print('Unusable cookie:', defects, 'defects')
        return []
    return nv_pairs

def save_cookie_files(cookie_nv_pairs):
    if len(cookie_nv_pairs) > 0:
        cookie_list_file = os.path.expanduser('~/data/cookie.list')
        with open(cookie_list_file, 'w') as wfo:
            wfo.write('\n'.join(sorted(cookie_nv_pairs)) + '\n')
        cookie_txt_file = os.path.expanduser('~/data/cookie.txt')
        with open(cookie_txt_file, 'w') as wfo:
            wfo.write('; '.join(cookie_nv_pairs))
        return cookie_txt_file

def main(dest_dir):
    import time
    db_path = os.path.expanduser("~/snap/chromium/common/chromium/Default/Cookies")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cursor = conn.cursor()
    check_assumptins(cursor)
    rows = fetch_fid_cookie_rows(cursor)
    validate_fid_cookie_rows(rows)
    nv_pairs = get_fid_cookies(rows)
    cookie_txt_file = save_cookie_files(nv_pairs)
    if not (cookie_txt_file and os.path.exists(cookie_txt_file) and os.path.getsize(cookie_txt_file) > 1000):
        print('Failedd to extract cookies')
        return
    if time.time() - os.path.getmtime(cookie_txt_file) <= 5:
        os.system(f'/usr/bin/scp {cookie_txt_file} {dest_dir}')
        return True
    else:
        print(f'{cookie_txt_file} is stale')
        return

if __name__ == '__main__':
    import sys
    main(sys.argv[1])
