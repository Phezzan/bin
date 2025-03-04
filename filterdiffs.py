#!/usr/bin/env python3

import re
from pprint import pprint

r'copy pub/video/info/pol/Episode_1174_Scott_Adams_-_Election_Day_in_America_and_What_to_Expect.\[f2Ru5jF_1vI\].mkv -> pub/video/info/pol/Episode_1174_Scott_Adams_-_Election_Day_in_America_and_What_to_Expect.\[f2Ru5jF_1vI\].mkv'

_file_re = r'(?:\\.|\S)+'
_line_re = (r'^(?P<act>copy|add|remove|update|move)\s+'
            r'(?P<src>{file})(?: -> (?P<dst>{file}))?'.format(file=_file_re))


def parse_line(di: dict, line: str):
    global _line_re
    m = re.match(_line_re, line)
    if not m:
        return
    dst = m['dst']
    src = m['src']
    act = m['act']
    if dst:
        v = di.setdefault(dst, set())
        v.add(act)
    if src:
        v = di.setdefault(src, set())
        v.add(act)
    if di[src]:
        v = di[src]
        if 'copy' in v and 'remove' in v:
            v ^= {'moved', 'copy', 'remove'}
        elif 'add' in v and 'remove' in v:
            v ^= {'moved', 'add', 'remove'}
    return


def read_diff(fn: str):
    from pathlib import Path
    di = {}
    with open(fn, 'rb') as f:
        data = f.read()
        for line in data.split(b'\n'):
            try:
                parse_line(di, line.decode())
            except UnicodeDecodeError:
                print(line)
    files = {}
    for k, v in di.items():
        name = Path(k).name
        f = files.setdefault(name, {'actions': set(), 'locations': set()})
        f['actions'].update(v)
        f['locations'].add(k)
    return files


def get_args():
    from argparse import ArgumentParser
    ap = ArgumentParser()
    ap.add_argument('-m', '--moved', action='store_true', help='show moved')
    ap.add_argument('-a', '--added', action='store_true', help='show added')
    ap.add_argument('-u', '--updated', action='store_true', help='show updated')
    ap.add_argument('-c', '--copied', action='store_true', help='show copied')
    ap.add_argument('-r', '--removed', action='store_true', help='show removed')
    ap.add_argument('diff_file', help='snapraid diff output')
    return ap.parse_args()


def main():
    args = get_args()

    files = read_diff(args.diff_file)

    req = set()
    if args.moved:
        req.add('move')
    if args.added:
        req.add('add')
    if args.removed:
        req.add('remove')
    if args.copied:
        req.add('copy')
    if args.updated:
        req.add('update')

    def filter_fn(k):
        return bool(req & files[k]['actions'])

    filtered = {k: files[k] for k in filter(filter_fn, files.keys())}
    pprint(filtered)


if __name__ == "__main__":
    main()
