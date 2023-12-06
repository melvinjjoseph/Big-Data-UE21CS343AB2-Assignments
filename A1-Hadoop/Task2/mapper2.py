#!/usr/bin/env python3

import sys

for line in sys.stdin:
    fields = line.strip().split('\t')
    if int(fields[2])<3:
        print(f'{fields[0]}\t{fields[1]}\t{fields[2]}\t{fields[3]}')
    else:
        continue
