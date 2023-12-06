#!/usr/bin/env python3

import sys
import json

name=""
strike_rate=0
count=0
for line in sys.stdin:
    new_name,new_strike_rate = line.strip().split(',')
    new_strike_rate=float(new_strike_rate)
    if new_name==name:
        count+=1
        strike_rate+=new_strike_rate
    else:
        if count!=0:
            strike_rate=round(strike_rate/count,3)
            print(json.dumps({"name": name, "strike_rate": strike_rate}))
        name=new_name
        count=1
        strike_rate=new_strike_rate
if count!=0:    
    strike_rate=round(strike_rate/count,3)
    print(json.dumps({"name": name, "strike_rate": strike_rate}))