#!/usr/bin/env python3
"""Extract Parquet file stats (file_size_bytes, footer_size) for DuckLake registration."""
import struct, os, glob, csv

with open('_parquet_stats.csv', 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['table_name', 'file_size_bytes', 'footer_size'])
    for path in sorted(glob.glob('data/main/*/data_0.parquet')):
        size = os.path.getsize(path)
        with open(path, 'rb') as pf:
            pf.seek(-8, 2)
            footer_len = struct.unpack('<i', pf.read(4))[0]
        table_name = path.split('/')[2]
        w.writerow([table_name, size, footer_len])
