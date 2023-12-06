#!/usr/bin/env python3

import sys
import json

for line in sys.stdin:
    
    try:
        obj = line.strip().lstrip("[").strip("]").rstrip(",")
        json_obj = json.loads(obj)
    except json.decoder.JSONDecodeError:
        continue

    if json_obj["balls"] == 0:
        strike_rate = 0
    else:
        strike_rate = round((json_obj["runs"] / json_obj["balls"]) * 100, 3)

    output= json_obj["name"]+","+f"{strike_rate:.3f}"
    print(output)