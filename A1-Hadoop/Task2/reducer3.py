#!/usr/bin/env python3

import sys

current_product = None
quantity_agg = 0
for line in sys.stdin:
    fields = line.strip().split('\t')
    if fields[0]==current_product:
        quantity_agg+=int(fields[1])
    else:
        if current_product:
            print(f'{current_product}\t{quantity_agg}')
        current_product=fields[0]
        quantity_agg=int(fields[1])
if current_product:
    print(f'{current_product}\t{quantity_agg}')