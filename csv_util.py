#!/usr/bin/env python3
import csv
from typing import List, Iterable
from pathlib import Path
from argparse import ArgumentParser, Namespace
import locale
import re
import logging

locale.setlocale(locale.LC_ALL, '')


def get_args() -> Namespace:
    ap = ArgumentParser(description="fixups for csv files")
    ap.add_argument('filenames', metavar='N', type=str, nargs="+", help="Files to fix")
    ap.add_argument('--recurse', '-r', action='store_true', help='traverse directories')
    ap.add_argument('--verbose', '-v', action='store_true', help='be loud')
    ap.add_argument('--skiplines', '-s', type=int, default=0, help='number of lines to skip')
    return ap.parse_args()


def read(filenames: list, skiplines: int = 2) -> List[dict]:
    rv = []
    prepend = []

    def skip_reader(file, _skip: int, fieldnames=None) -> csv.DictReader or None:
        _reader = None
        nonlocal prepend
        while not _reader or len(_reader.fieldnames) < 2:
            file.seek(0)
            prepend = []
            for _ in range(_skip):
                prepend.append(file.readline())
            _reader = csv.DictReader(file, fieldnames=fieldnames)
            _skip += 1
        return _reader

    for fn in filenames:
        name = str(fn)
        try:
            with open(fn, 'rt') as f:
                reader = skip_reader(f, skiplines)
                columns = is_columns(reader.fieldnames)
                if columns:
                    logging.warning(f"Found columns")
                    reader = skip_reader(f, skiplines, fieldnames=columns)
                rv.append({'name': name,
                           'prepend': prepend,
                           'fields': reader.fieldnames,
                           'rows': list(reader)})
        except FileNotFoundError as e:
            rv.append({'name': name,
                       'error': str(e),
                       'prepend': None,
                       'fields': None,
                       'rows': None})
    return rv


def to_number(val: str) -> (float, str):
    if not val or val == '--':
        return val, None
    strips = ''.maketrans('', '', '$%')
    _CSN_RE = '[+-]?\s*\d{1,3}(,\d\d\d)*(\.\d\d)?'
    categories = [
        {'test': re.compile(_CSN_RE),
         'cat': 'general',
         'fn': lambda v: locale.atof(v.translate(strips))},
        {'test': re.compile(r'[+-]?\$'+_CSN_RE),
         'cat': 'currency',
         'fn': lambda v: f"${locale.atof(v.translate(strips))}"},
        {'test': re.compile(_CSN_RE+'%'),
         'cat': 'percent',
         'fn': lambda v: str(locale.atof(v.translate(strips)))+'%'},
        {'test': re.compile(r'\d stars|dropping coverage'),
         'cat': 'stars',
         'fn': lambda v: v[0].upper()},
        {'test': re.compile(r'None|Narrow|Wide'),
         'cat': 'Moat',
         'fn': lambda v: ['None', 'Narrow', 'Wide'].index(v)},
        {'test': re.compile(r'Poor|Standard|Exemplary'),
         'cat': 'Stewardship',
         'fn': lambda v: ['Poor', 'Standard', 'Exemplary'].index(v)},
    ]
    val = val.strip()
    for di in categories:
        test = di['test']
        if type(test) is re.Pattern and test.fullmatch(val):
            return di['fn'](val), di['cat']
        if callable(test) and test(val):
            return di['fn'](val), di['cat']
    return val, None


def is_columns(ls: Iterable[str]) -> List[str]:
    # Renames columns
    columns = {
        "Symbol": "symbol",
        "Description": "desc",
        "Debt to Equity (MRQ)": "Debt/Eq MRQ",
        "Return on Equity (TTM)": "RoE TTM",
        "Return on Assets (MRFY)": "RoA MRFY",
        "Return on Invested Capital (TTM)": "RoIC TTM",
        "Payout Ratio - TTM": "Payout% TTM",
        "Payout Ratio - 5 Yr Avg": "Payout% 5y",
        "Yield - 5 Yr Avg": "Yield 5y",
        "3 Year Dividend Growth Rate": "DivGrowthRate 3y",
        "5 Year Dividend Growth Rate": "DivGrowthRate 5y",
        "Morningstar Rating": "Ms Rating",
        "Morningstar Economic Moat": "Ms Moat",
        "Morningstar Stewardship": "Ms steward",
        "Morningstar Fair Value Estimate": "Ms FairValue",
        "Revenue Growth Rate Last 3 Years": "RevGr 3y",
        "Revenue Growth Rate Last 5 Years": "RevGr 5y",
        "EPS Growth History Last 3 Years": "EPSGr 3y",
        "EPS Growth History Last 5 Years": "EPSGr 5y",
        "Est. EPS Growth Long Term (3 to 5 Years)": "EPSGr 3-5y",
        "Price/Earnings/Growth (PEG) (TTM)": "P/E/G TTM",
        "Price/Cash Flow (MRFY)": "P/CF MRFY",
        "Price/Earnings (TTM)": "P/E TTM",
        "Quick Ratio (MRQ)": "Quick MRQ",
        "Current Ratio (MRQ)": "Current MRQ",
        "Cash Flow Per Share (TTM)": "CF/share TTM",
    }
    try:
        rv = list(map(lambda s: columns[s] or s, ls))
    except KeyError as e:
        return []
    return rv



def fixup(name, rows, **kwargs):
    """ find numbers in rows and make them numbers"""
    # FUTURE: remember columns and warn/apply the same
    for row in rows:
        for k in row.keys():
            val, cat = to_number(row[k])
            if val and cat:
                row[k] = val


def save(name: str, prepend: List[str], fields: List[str], rows: Iterable[dict]):
    path = Path(name)
    with open(path, 'wt', newline='') as f:
        f.writelines(prepend)
        writer = csv.DictWriter(f, fields, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = get_args()
    filedatas = read(args.filenames, args.skiplines)
    for d in filedatas:
        if 'error' in d:
            logging.error(f"{d['name']}: {d['error']}")
            continue
        fixup(**d)  # name=d['name'], fields=d['fieldnames'], rows=d['rows'])
        save(**d)   # name=d['name'], fields=d['fieldnames'], rows=d['rows'])
    return 0


if __name__ == '__main__':
    logging.basicConfig()
    rc = main()
    exit(rc)
