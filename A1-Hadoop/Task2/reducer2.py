#!/usr/bin/env python3

import sys

for line in sys.stdin:
    fields = line.strip().split('\t')
    print(f'{fields[0]}\t{fields[1]}\t{fields[3]}')
