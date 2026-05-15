"""Quick connectivity test for MIT Data Warehouse (DWRHS). Run with: uv run python test_dwrhs.py"""
import os
import oracledb

host = os.environ["DWRHS_HOST"]
user = os.environ["DWRHS_USER"]
password = os.environ["DWRHS_PASSWORD"]
sid = os.environ["DWRHS_SID"]

# thick mode required — MIT Warehouse enforces Oracle Native Network Encryption
# install ARM64 Instant Client DMG from:
# https://www.oracle.com/database/technologies/instant-client/macos-arm64-downloads.html
oracledb.init_oracle_client(lib_dir="/opt/oracle/instantclient_23_26")

print(f"Connecting to {user}@{host}/{sid} ...")
conn = oracledb.connect(user=user, password=password, host=host, sid=sid)
print("Connected.")

with conn.cursor() as cur:
    cur.execute("SELECT SYSDATE FROM DUAL")
    row = cur.fetchone()
    print(f"SYSDATE: {row[0]}")

conn.close()
print("Done.")
