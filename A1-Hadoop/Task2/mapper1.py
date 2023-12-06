#!/usr/bin/env python3

import sys

for line in sys.stdin:
    fields = line.strip().split('\t')
    if fields[0]=='order':
        c_id = int(fields[2])
        p_id=fields[3]
        quantity=fields[4]
        print(f'{p_id},{c_id},O,{quantity}')
    elif fields[0]=='review':
        p_id=fields[2]
        c_id=int(fields[3])
        rating=fields[4]
        print(f'{p_id},{c_id},R,{rating}')
    else:
        continue
