"""
Recover orders for clients 1-5 from corrupted DB into binroute_new.db
Uses chunked rowid reads to skip corrupted pages.
"""
import sqlite3
import os

SRC = '/opt/binroute/data/binroute.db'
DST = '/opt/binroute/data/binroute_new.db'
CHUNK = 5000
CLIENTS = [1, 2, 3, 4, 5]

src = sqlite3.connect(SRC)
src.execute('PRAGMA writable_schema = ON')
dst = sqlite3.connect(DST)
dst.execute('PRAGMA journal_mode=WAL')
dst.execute('PRAGMA synchronous=NORMAL')

# Get column count from a working row
sample = src.execute('SELECT * FROM orders LIMIT 1').fetchone()
ncols = len(sample)
placeholders = ','.join(['?'] * ncols)
print(f'Orders table has {ncols} columns')

# Recover orders for clients 1-5
total = 0
failed = 0
for start in range(0, 8000000, CHUNK):
    try:
        rows = src.execute(
            f'SELECT * FROM orders WHERE rowid BETWEEN {start} AND {start + CHUNK - 1} AND client_id IN (1,2,3,4,5)'
        ).fetchall()
        if rows:
            dst.executemany(f'INSERT OR IGNORE INTO orders VALUES ({placeholders})', rows)
            total += len(rows)
    except Exception as e:
        failed += 1

    if start % 200000 == 0 and start > 0:
        dst.commit()
        print(f'  rowid {start}: recovered {total:,} orders, {failed} failed chunks')

    # Stop scanning after rowid 3M if we haven't found data in a while
    if start > 3000000 and total > 0:
        # Check if last 100 chunks were all empty
        pass

dst.commit()
print(f'\nOrders recovery complete: {total:,} rows, {failed} failed chunks')

# Verify per client
for cid in CLIENTS:
    count = dst.execute(f'SELECT COUNT(*) FROM orders WHERE client_id = {cid}').fetchone()[0]
    print(f'  Client {cid}: {count:,} orders')

# Now recover products_catalog
print('\nRecovering products_catalog...')
pc_total = 0
pc_failed = 0
for start in range(0, 500000, CHUNK):
    try:
        rows = src.execute(
            f'SELECT * FROM products_catalog WHERE rowid BETWEEN {start} AND {start + CHUNK - 1}'
        ).fetchall()
        if rows:
            ph = ','.join(['?'] * len(rows[0]))
            dst.executemany(f'INSERT OR IGNORE INTO products_catalog VALUES ({ph})', rows)
            pc_total += len(rows)
    except:
        pc_failed += 1
    if start > 100000 and pc_total == 0:
        break

dst.commit()
print(f'products_catalog: {pc_total:,} rows, {pc_failed} failed chunks')

# Also recover product_cogs_match if it exists
try:
    dst.execute('CREATE TABLE IF NOT EXISTS product_cogs_match (client_id INTEGER, product_id TEXT, product_name TEXT, cogs_product_name TEXT, cogs REAL, match_type TEXT, match_score REAL, matched_at TEXT, PRIMARY KEY (client_id, product_id))')
    for start in range(0, 100000, CHUNK):
        try:
            rows = src.execute(f'SELECT * FROM product_cogs_match WHERE rowid BETWEEN {start} AND {start + CHUNK - 1}').fetchall()
            if rows:
                ph = ','.join(['?'] * len(rows[0]))
                dst.executemany(f'INSERT OR IGNORE INTO product_cogs_match VALUES ({ph})', rows)
        except:
            pass
    dst.commit()
    print(f'product_cogs_match recovered')
except Exception as e:
    print(f'product_cogs_match: {e}')

src.close()
dst.close()
print(f'\nNew DB size: {os.path.getsize(DST) / 1e9:.2f} GB')
print('Done!')
