#!/home/ana/conda/bin/python3

import os
import sqlite3
import secretstorage
from Cryptodome.Cipher import AES
from Cryptodome.Protocol.KDF import PBKDF2
from Cryptodome.Hash import SHA1

def decrypt_v10(blob, password):
    if not blob or len(blob) < 3 or not blob.startswith(b'v10'):
        return None

    # Constants for Chromium Linux v10
    salt = b'saltysalt'
    bsize = 16
    key_length = 16
    iv = b' ' * bsize
    # value_charset = '#$%&()+-./0123456789=ABCDEFGHIJKLMNOPQRSTUVWXYZ^_abcdefghijklmnopqrstuvwxyz|~'

    try:
        # 1. Derive Key
        derived_key = PBKDF2(password, salt, key_length, count=1, hmac_hash_module=SHA1)
        
        # 2. Cipher Setup
        cipher = AES.new(derived_key, AES.MODE_CBC, IV=iv)
        
        # 3. Decrypt the payload (stripping the 'v10' 3-byte header)
        decrypted = cipher.decrypt(blob[3:])

        # --- CRITICAL PADDING & STRUCTURE LOGIC ---
        # Chromium v10 often stores a 'Length-Prefix' or a 'Signature' in the first block.
        # If the first block is garbled, it's often because the IV in the DB 
        # is actually the first 16 bytes of the payload.
        
        # Let's try the 'First Block is IV' strategy if standard IV fails
        if not all(32 <= c <= 126 for c in decrypted[16:20]):
            # Use the first 16 bytes of the payload as the IV for the rest
            new_iv = blob[3:3+bsize]
            new_payload = blob[3+bsize:]
            cipher = AES.new(derived_key, AES.MODE_CBC, IV=new_iv)
            decrypted = cipher.decrypt(new_payload)

        # 4. PKCS#7 Padding Check
        padding_len = decrypted[-1]
        if padding_len < 1 or padding_len > bsize:
            return None
        
        raw_data = decrypted[:-padding_len]

        # 5. Get rid of the leading garbage blocks
        for i in range(0, len(raw_data), bsize):
            if any(x < 35 or x > 126 for x in raw_data[i:i+bsize]):
                continue
            return raw_data[i:].decode('utf-8', errors='ignore')
        return ''

    except Exception:
        return None

def extract_fid_cookies():
    # Standard Ubuntu Snap Path
    db_path = os.path.expanduser("~/snap/chromium/common/chromium/Default/Cookies")
    if not os.path.exists(db_path):
        db_path = os.path.expanduser("~/snap/chromium/common/chromium/Default/Network/Cookies")

    # Get Key
    bus = secretstorage.dbus_init()
    collection = secretstorage.get_default_collection(bus)
    password = b'peanuts' # Default Snap fallback
    for item in collection.get_all_items():
        if "chromium" in item.get_label().lower():
            password = item.get_secret()
            print(f'Got password {password} from secretstorage.')
            break

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cursor = conn.cursor()
    domain = '.fidelity.com'
    cursor.execute("SELECT name, encrypted_value, value, host_key FROM cookies WHERE host_key LIKE ?", (f"%{domain}%",))

    cookie_nv_pairs = []
    for name, enc, plain, h_key in cursor.fetchall():
        if h_key not in [domain, 'digital' + domain, '.digital' + domain]:
            print(f'Ignore host {h_key}: {name}')
            continue
        if name in ['AMURCC', 'SESSION_CTX']:
            continue
        blob = enc if (enc and enc.startswith(b'v10')) else plain
        val = decrypt_v10(blob, password)
        print(f"host {h_key}: {name}")
        cookie_nv_pairs.append(f'{name}={val}')
    conn.close()
    if len(cookie_nv_pairs) > 0:
        cookie_list_file = os.path.expanduser('~/data/cookie.list')
        with open(cookie_list_file, 'w') as wfo:
            wfo.write('\n'.join(sorted(cookie_nv_pairs)) + '\n')
        cookie_txt_file = os.path.expanduser('~/data/cookie.txt')
        with open(cookie_txt_file, 'w') as wfo:
            wfo.write('; '.join(cookie_nv_pairs))
        return cookie_txt_file

if __name__ == "__main__":
    import time
    import sys
    dest_dir = sys.argv[1]
    cookie_txt_file = extract_fid_cookies()
    if not (cookie_txt_file and os.path.exists(cookie_txt_file) and os.path.getsize(cookie_txt_file) > 1000):
        print('Failedd to extract cookies')
        sys.exit(1)
    if time.time() - os.path.getmtime(cookie_txt_file) <= 5:
        os.system(f'/usr/bin/scp {cookie_txt_file} {dest_dir}')
    else:
        print(f'{cookie_txt_file} is stale')
        sys.exit(1)
