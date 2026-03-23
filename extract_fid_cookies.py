#!/home/ana/conda/bin/python3

import os
import sqlite3
from Cryptodome.Cipher import AES
from Cryptodome.Protocol.KDF import PBKDF2
from Cryptodome.Hash import SHA1

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
    for name, blob, h_key in rows:
        val = decrypt_v10(blob)
        print(h_key, name, len(val), 'bytes')
        nv_pairs.append(f'{name}={val}')
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