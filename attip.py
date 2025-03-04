#!/bin/env python3
# get public (WAN) IP from the ATT gateway

from argparse import ArgumentParser
from pathlib import Path
import requests
import re
from html5lib import parse
from pprint import pprint
from io import StringIO
import logging
from xml.etree import ElementTree

def get_args():
    ap = ArgumentParser()
    ap.add_argument('--verbose', '-v', action='store_true')
    ap.add_argument('details', nargs='*')
    return ap.parse_args()
    

def get_att_info() -> str:
    rsp = requests.get('http://192.168.1.254/cgi-bin/broadbandstatistics.ha', timeout=8, proxies={'http://192.168.1.254': ''})

    if rsp.status_code >= 300:
        raise RuntimeError(rsp.content)
    return rsp.content

def parse_att_info(html: bytes) -> ElementTree:
    t = parse(html)
    return t

def find_ipv4(xml: ElementTree) -> str:
    out, next_state = {}, ""
    fields = {
        "Broadband IPv4 Address": "IPv4",
        "Gateway IPv4 Address": "Gateway4",
        "MAC Address": "MAC",
        "Primary DNS": "DNS.1",
        "Secondary DNS": "DNS.2",
        "MTU": "mtu",
    }
    for txt in xml.itertext():
        if not (txt:= txt.strip()):
            continue
        
        if next_state and next_state in fields.values():
            logging.info(f"{next_state} : {txt:.20}")
            out[next_state] = txt
            next_state = ""
            continue

        for rex, state in fields.items():
            if re.match(rex, txt) and state not in out:
                next_state = state
                logging.info(f"-> {next_state:.20}")
                continue
    return out

def main(args):
    if args.verbose:
        logging.root.setLevel(logging.INFO)
    content = get_att_info()
    info = parse_att_info(content)
    parsed = find_ipv4(info)
    if args.details:
        for nfo in parsed:
            if nfo.lower() in args.details:
                print(f"{nfo}:{parsed[nfo]}")
    else:
        print(parsed)


if __name__ == '__main__':
    main(get_args())
