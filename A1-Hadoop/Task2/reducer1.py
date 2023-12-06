#!/usr/bin/env python3

import sys

current_product = None
current_customer = None
quantity = 0
rating=0

for line in sys.stdin:
    fields = line.strip().split(',')
    if fields[2]=="O":
        current_customer = fields[1]
        current_product = fields[0]
        quantity = fields[3]
    elif fields[2]=="R":
        current_customer = fields[1]
        current_product = fields[0]
        rating = fields[3]
    else:
        continue
    if current_product and rating and quantity:
        print(f'{current_product}\t{current_customer}\t{rating}\t{quantity}')
        current_customer=None
        current_product=None
        rating=0
        quantity=0
    
