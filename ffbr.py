#!/usr/bin/env python3
import re
import os
import logging
from wcwidth import wcswidth  # Far from perfect.
from typing import Union
from pathlib import Path
from argparse import ArgumentParser, Namespace
from subprocess import Popen, PIPE
from datetime import timedelta
from shutil import get_terminal_size


VIDEO_EXTS = "avi,mkv,mp4,webm,ogv,ogg,mpeg,mpg,asf,divx,rm"
AUDIO_EXTS = "mp3,mp4a,mp4,ogg,wav,flac,oga,m3a,riff,opus"
SUFFIXES = {
    "video": VIDEO_EXTS.split(','),
    "audio": AUDIO_EXTS.split(','),
    "all": (VIDEO_EXTS+AUDIO_EXTS).split(',')
}


def get_args():
    ap = ArgumentParser(description="Calculate the bitrate of a video file")
    ap.add_argument('filenames', metavar='N', type=str, nargs="*", help="Files to calculate", default=['.'])
    ap.add_argument('--verbose', '-v', action='store_true', help='just the one number')
    ap.add_argument('--quiet', '-q', action='store_true', help='dont print dots')
    ap.add_argument('--simple', '-s', action='store_true', help='just the one number')
    ap.add_argument('--recurse', '-r', action='store_true', help='traverse directories')
    ap.add_argument('--suffixes', '-x', type=str, help='comma separated extensions or [video, audio, all]', default='all')
    return ap.parse_args()

   
_args: Namespace = get_args()
cwd = Path('.').resolve()
max_width = 0
results = {}


def print_name(p: Path) -> str:
    global cwd
    try: 
        return str(p.relative_to(cwd))
    except ValueError:
        return f"{p.parent.name:1}/{p.name}"


def get_duration(filename: Path) -> float:
    HOURS_RE = r'(?P<hours>\d\d)'
    MINS_RE = r'(?P<mins>[0-6]\d)'
    SECS_RE = r'(?P<secs>[0-6]\d)'
    DURATION_RE = re.compile(rf'DURATION\s*:\s*{HOURS_RE}:{MINS_RE}:{SECS_RE}\.\d*\D.*', flags=re.IGNORECASE)
    if filename.is_reserved():
        print(f'reserved {filename.name}')
    process = Popen(["ffprobe", filename.absolute()], stdout=PIPE, stderr=PIPE, text=True)
    stdout, stderr = process.communicate()
    delta = None
    for line in stderr.split('\n'):
        duration = DURATION_RE.search(line)
        if not duration:
            continue
        hours, mins, secs = [int(i) for i in duration.groups()]
        delta = timedelta(hours=hours, minutes=mins, seconds=secs)
        break
    if type(delta) != timedelta:
        raise ValueError(f"Unsupported file: {filename.relative_to(Path.cwd())}")
    return delta and delta.total_seconds() or None


def get_bitrate(filename: Path) -> tuple:
    secs = get_duration(filename)
    size = filename.stat().st_size
    return secs, size, size/secs


def read_dir(directory: Union[str, Path], suffixes: set, filterFN=None) -> set:
    filepath = Path(directory) if type(directory) is not Path else directory
    filepath = filepath.resolve()

    def _filter(path: Path):
        try:
            logging.debug(path, path.is_dir(), path.is_file(), path.suffix)
        except (OSError, FileNotFoundError):
            return False
        return (path.is_dir() or
                (path.is_file() and path.suffix[1:] in suffixes)
                )

    return set(filter(_filter, filepath.iterdir()))


def scan(files: set):
    global _args, max_width, results
    for f in files:
        if os.path.isdir(f):
            if _args.recurse:
                dir = read_dir(f, suffixes=_args.suffixes)
                logging.info(f'adding: {f.name}/ [{len(dir)}]')
                _args.filenames = _args.filenames.union(dir)
            _args.filenames.discard(f)
            logging.info(f'discard: {f.name}')
            continue
        max_width = max(max_width, wcswidth(print_name(f)))
        try:
            duration, size, br = get_bitrate(f)
            results[f] = (br, duration, size)
            if not _args.quiet and not _args.verbose:
                print('.', end='', flush=True)
        except ValueError as e:
            _args.filenames.discard(f)
            logging.info(f"skipping: {f.name}")

        except TypeError as e:
            _args.filenames.discard(f)
            logging.exception(f"{f.name}: {e}")

    if not _args.quiet:
        print()


def prep_args():
    global _args
    if _args.suffixes:
        _args.suffixes = set(_args.suffixes.split(',')) or {}
        for suffix in list(_args.suffixes):
            if suffix in SUFFIXES:
                _args.suffixes.discard(suffix)
                _args.suffixes.update(SUFFIXES[suffix])


def main():
    global _args, max_width, results, AUDIO_EXTS, VIDEO_EXTS
    prep_args()
    term_width = get_terminal_size((120,50)).columns
    print(f"Term size: {term_width}")
    results = {}
    _args.filenames = set([Path(f).resolve() for f in _args.filenames])
    logging.root.setLevel(logging.INFO if _args.verbose else logging.WARNING)
    if len(_args.filenames) == 1:
        filename = _args.filenames.pop()
        if filename.is_dir():
            _args.filenames = read_dir(filename, suffixes=_args.suffixes)
        else:
            _args.filenames.add(filename)

    if _args.simple:
        duration, size, br = get_bitrate(_args.filenames.pop())
        print(int(br))
        return 0

    while len(results) < len(_args.filenames):
        scan(_args.filenames - set(results.keys()))
        logging.info(f'[{len(results)}] / [{len(_args.filenames)}]')

    if not _args.quiet:
        print(f"Bits/s {'Name':{term_width - 15}} {'Secs':>6}")
    for path, v in sorted(results.items(), key=lambda t: t[1][0], reverse=False):
        br, duration, size = v
        name = print_name(path)
        if (over := wcswidth(name) - (term_width - 17)) and over > 0:
            name = f"{name:.{term_width-17}}.."
        print(f"{int(round(br/1024)):>5}k {name:{term_width - 15}} {int(duration):>6}s")


if __name__ == '__main__':
    main()
