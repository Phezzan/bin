#!/usr/bin/env python3
""" Corrects the modified time of a directory based on contents
    This is needed when entire directories are restored via snapraid
    each directory under (recurse) provided path will inspect its files and take the newest (mtime or ctime)
"""
from datetime import datetime, timezone
from pathlib import Path
from os import utime

cache = {}


def work(path: Path, glob: str = '*', recurse: bool = True, mtime=False) -> int:
    _time:int
    if _time := cache.get(path.absolute()):
        return _time

    max_time: int = 0
    min_time: int = datetime.now(tz=timezone.utc).timestamp()
    for fp in path.glob(glob):
        if not fp.exists():
            continue
        if fp.name in ('.', '..'):
            continue
        if fp.is_dir():
            if recurse:
                _time = cache[fp.absolute()] = work(fp.resolve())
            else:
                continue
        else:
            _time = fp.stat().st_ctime_ns if mtime else fp.stat().st_mtime_ns
        max_time = max(_time, max_time)
        min_time = min(_time, min_time)

    a_time = path.stat().st_atime_ns
    print(f"setting {path} to {datetime.fromtimestamp(max_time/(10**9))}")
    utime(path.absolute(), ns=(a_time, max_time))
    return max_time


def get_args():
    from argparse import ArgumentParser
    ap = ArgumentParser()
    ap.add_argument('-r', '--recurse', default=True, action='store_true', help='Recurse [default]')
    ap.add_argument('-s', '--subdir', default=False, action='store_true',  help='update includes subdir calculated times')
    ap.add_argument('-m', '--mtime', default=False,  action='store_true', help='use mtime instead of ctime')
    ap.add_argument('-g', '--glob', default='*', help='glob for files to inspect')
    ap.add_argument('path', default=Path('.'), type=Path, help="Path to update")
    return ap.parse_args()


if __name__ == '__main__':
    args = get_args()
    path = [args.path]
    if args.recurse:
        path = set(p.absolute() for p in args.path.glob('**/*') if p.is_dir()) | {args.path}
    for p in path:
        work(path=p, glob=args.glob, recurse=args.subdir, mtime=args.mtime)
