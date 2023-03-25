#!/usr/bin/env python
"""
Synchronizes two directories of manga
Directories in the destination are scanned for 'Manga' objects

 comparing 'Manga' via several methods
"""

import logging
import yaml
import os
import shutil
import time
import re
import curses
import grp
import zipfile as zf
from shutil import copy2
from contextlib import contextmanager
from re import Pattern
from typing import List, Union, Set, Dict, Iterable, Callable
from pathlib import Path
from pprint import pformat
from argparse import ArgumentParser, Namespace
from math import floor

logging.basicConfig(level=logging.INFO)
_args: Namespace


@contextmanager
def pushd(path: Path):
    pwd = Path.cwd()
    os.chdir(path)
    try:
        yield pwd
    finally:
        os.chdir(pwd)


def reprint(*args, **kwargs):
    print('\r\033[K\r', end='')
    return print(*args, **kwargs)


def rsync(src: Path, dest: 'Manga', failed):
    import subprocess
    rsync_out_re = {
        'files': r'Number of files: (?P<files>[,0-9]+)',
        'created': r'Number of created files: (?P<created>[,0-9]+)',
        'bytes': r'Literal data: (?P<bytes>[,0-9]+)',
        'sent': r'Total bytes sent: (?P<sent>[,0-9]+)',
        'received': r'Total bytes received: (?P<received>[,0-9]+)'
    }
    rsync_errors = {
        1:  "Syntax or usage error",
        2:  "Protocol incompatibility",
        3:  "Errors selecting input / output files, dirs",
        4:  "Requested action not supported",
        5:  "Error starting client - server protocol",
        6:  "Daemon unable to append to log - file",
        10: "Error in socket I / O ",
        11: "Error in file I / O",
        12: "Error in rsync protocol data stream",
        13: "Errors with program diagnostics",
        14: "Error in IPC code",
        20: "Received SIGUSR1 or SIGINT",
        21: "Some error returned by waitpid()",
        22: "Error allocating core memory buffers",
        23: "Partial transfer due to error",
        24: "Partial transfer due to vanished source files",
        25: "The --max-delete limit stopped deletions",
        30: "Timeout in data send / receive",
        35: "Timeout waiting for daemon connection",
    }
    cmd = f'rsync -a --whole-file --min-size={_args.min_size} --ignore-existing ' \
          f'--omit-dir-times --no-perms --timeout={_args.timeout} --stats'.split()
    dst = dest.path.parent
    cmd.extend([f'{src}', f'{dst}/.'])
    cwd = src
    c = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
    if c.returncode != 0:
        err = f"{rsync_errors.get(c.returncode, '')}- {c.stderr}"
        logging.debug(f"{src.name} -> {dst.name} [{c.returncode}]: {err}")
        if failed:
            failed[src] = f"{c.returncode}: {err}"
        return None, None
    m = re.search(r'(?:.*\n)+'.join(rsync_out_re.values()), c.stdout)
    if m:
        dest.created = int(m.group('created').replace(',', ''))
        dest.bytes = int(m.group('bytes').replace(',', ''))
        dest.transferred = m.groupdict()
        return dest.created, dest.bytes


class Times:
    _times = {}
    _lastName = None

    @classmethod
    def start(cls, name: str):
        cls._times[name] = -1.0 * time.perf_counter()
        cls._lastName = name

    @classmethod
    def stop(cls, name: str = None):
        """ Returns the elapsed time since start,
        infers the name from previous start
        blows up if you didn't start the given string
        """
        if not name:
            name = cls._lastName
            cls._lastName = None
        cls._times[name] += time.perf_counter()
        return cls._times[name]

    @classmethod
    def clear(cls, index: Union[str, int]):
        if type(index) is int:
            # remove the times faster than the top X
            li = sorted([(v, k) for k, v in cls._times.items()], reverse=True)[index:]
            for _, k in li:
                del cls._times[k]
        elif index not in cls._times:
            return
        del cls._times[index]

    @classmethod
    def top10(cls):
        return sorted([(v, k) for k, v in cls._times.items()], reverse=True)[:10]


def spin(progress: float = None, text: str = ''):
    import shutil
    cols = shutil.get_terminal_size((130, 1))[0] - 2
    chars = r'\|/-' * 2
    if progress is None:
        msg = '\n'
    elif progress == 0:
        msg = '. 0% '
    elif progress > 0.99:
        msg = f'100% '
    else:
        p = int(progress * 100)
        c = int(progress * 400) % len(chars)
        msg = f'{chars[c]}{p:2d}% {text}'
    reprint(msg[:cols], end='', flush=True)


def zip_path(src: Path, zipfile: Path, **kwargs) -> (int, int):
    files = bytes = 0
    if zipfile.exists() and not _args.overwrite:
        raise FileExistsError(f"{zipfile}")
    with zf.ZipFile(file=zipfile, mode='w', **kwargs) as f:
        for p in src.rglob('*'):
            size = p.stat().st_size
            if size < _args.min_size or _args.file_filter.match(p.name):
                logging.info(f"skipping(<{_args.min_size}): {p}")
                continue
            logging.debug(f"zipped: {src.name} -> {zipfile}")
            f.write(filename=p)
            bytes += size
            files += 1
    return files, bytes


class Scans:
    """ perform os.scandir but cache results """
    _dirs = {}

    @classmethod
    def scandir(cls, path: Path) -> list[Path]:
        if path not in cls._dirs and path.exists():
            if not path.is_dir():
                raise NotADirectoryError(path)
            with os.scandir(path) as it:
                # noinspection PyTypeChecker
                cls._dirs[path] = [Path(p) for p in it]
        return cls._dirs.get(path, [])


def bookends(nums: Set[float or int]) -> (float, float):
    """ condenses set of numbers into ranges: [1,2,3,5] -> [(1,3), (5,5)]"""
    nums = list(sorted(nums))
    start, end, step = 0, 0, 1
    pairs = []
    for i in nums:
        if not start or i > round(end + step, 3):
            if end:
                pairs.append((start, end))
            start = end = i
            step = round(start - int(start), 3) or 1
        else:
            end = i
    if end:
        pairs.append((start, end))
    return pairs


def ranges(start, *ends):
    """ yield chapter numbers (either integers or fixed point(1 digit)) from start to end
    :param start: lowest chapter number
    :param ends: list of higher chapter numbers
    :return: yields each chapter number in a range
    # (1,3) -> 1, 2, 3
    # (1, 4, 4.4) -> 1, 2, 3, 4.1, 4.2, 4.3 - NOTE: no 4, only 4.1-4.3
    """
    def sub_range(_start: float, _end: float):
        if _start > _end:
            raise ValueError(f"smart_range start[{_start}] > end[{_end}]")
        whole = floor(_start), floor(_end)
        sub = max(0, round(10 * (_start - whole[0])), round(10 * (_end - whole[1])))
        for x in range(whole[0], whole[1]+1):
            if sub > 1:
                for s in range(1, sub+1):
                    y = x + s/10.0
                    if y < _start or y > _end:
                        continue
                    yield y
            else:
                yield x

    start = float(start)
    # noinspection PyTypeChecker
    start = max(int(start), start)
    last, v = None, start
    for end in ends:
        end = float(end)
        for v in sub_range(start, end):
            if v != end:        # duplicates: don't yield the end if it will be the next start
                yield v
        start = v
    yield v                     # yield the final end


class Manga:
    all: Dict[Path, 'Manga'] = {}
    _directories: Dict[Path, dict] = {}
    SUFFIXES = {
        'ARCHIVE': {'tgz', 'tar', 'cbz', 'tbz', 'gz', 'rar', 'zip', '7z', 'xz'},
        'IMAGE': {'jpg', 'jpeg', 'png', 'gif'}
    }

    def __init__(self,
                 path: Path or str,
                 name: str = None,
                 aliases: Iterable[Union[str, Pattern]] = None,
                 group: str = None,
                 dirty=False, disabled=False, **_):
        if type(path) is not Path:
            path = Path(path)
        path = path.resolve()
        self.path = path.joinpath('manga.yml') if path.suffix != '.yml' else path
        self.seasons = {re.compile(k): v for k, v in _.get('seasons', {}).items()}
        self.name = name or self.path.parent.name
        self.aliases = set(re.compile(p, flags=re.IGNORECASE) for p in aliases or [])
        parent = self.path.parent
        self.group = self.group_name(group)
        self.created = 0
        self.bytes = 0
        self.errors = {}
        self._disabled = disabled
        self._dirty = dirty
        # TODO - try to guess aliases?
        parent_name = parent.name
        if parent_name != self.name and not self._check_aliases(parent_name):
            logging.debug(f"?? Adding alias {parent_name}")
            self.aliases.add(re.compile(parent_name))
        if parent in Manga.all:
            logging.error(f"{self.name} exists already! at [{parent} - {Manga.all[parent].path}]")
        else:
            parent.mkdir(mode=0o750, parents=True, exist_ok=True)

        self.chapters: Dict[float, Set[Chapter]] = {}
        for c in self._find_chapters(parent=parent):
            for n in c.numbers:
                self.chapters.setdefault(n, set()).add(c)

        Manga.all[parent] = self

    def _find_chapters(self, parent: Path) -> list:
        chapters = []
        if self._disabled:
            return chapters
        for path in Scans.scandir(parent):
            if _args.file_filter.match(path.name):
                self.errors.setdefault('Filter', set()).add(f"{path.parent}/{path.name}")
                continue
            if path.is_file():
                try:
                    t = path.name.rsplit('.', 1)
                    if len(t) != 2 or t[1] not in self.SUFFIXES['ARCHIVE']:
                        continue    # skip yml, txt, etc
                except ValueError as e:
                    self.errors.setdefault('Read', set()).add(f"{path.parent}/{path.name}")
                    continue
            try:
                c = Chapter.build(self, path)
                chapters.append(c)
            except (TypeError, ValueError) as e:
                self.errors.setdefault('Parser', set()).add(f"{path.parent}/{path.name}")
        return chapters

    def _all_chapters(self):
        return set(self.chapters.values())

    @property
    def key(self):
        return self.path

    def __hash__(self):
        return hash(self.key)

    def asDict(self, relative: Path = None) -> dict:
        di = {'name': self.name,  # in case we rename the directory then we'll still have the name
              'aliases': [str(a.pattern) for a in self.aliases],
              'group': self.group,
              'seasons': {k: repr(v) for k, v in self.seasons.items()},
              'disabled': self._disabled,
              # chapters not saved - presently they're just a dumb list of directory strs
              }
        di = {k: v for k, v in di.items() if v or k not in ['chapters']}
        if relative:
            # path is only useful when relative to a directory metadata
            if relative.is_file():
                relative = relative.parent.resolve()
            di['path'] = str(self.path.relative_to(relative))
        return di

    def set_group(self):
        if not self.path.parent.exists():
            pass
        manga_dir = self.path.parent
        logging.info(f"Set {manga_dir.name} to {self.group}")
        group = grp.getgrnam(self.group)
        os.chown(manga_dir.absolute(), uid=-1, gid=group.gr_gid, follow_symlinks=False)
        manga_dir.chmod(mode=0o750, follow_symlinks=False)
        if self.path.exists():
            os.chown(self.path.absolute(), uid=-1, gid=group.gr_gid, follow_symlinks=False)
        for child in manga_dir.glob(r'*.cbz'):
            os.chown(child.absolute(), uid=-1, gid=group.gr_gid, follow_symlinks=False)
            child.chmod(mode=0o640)

    def group_name(self, group):
        path = self.path
        while not path.exists():
            path = path.parent
        return path.group()

    def save(self, force=False) -> bool:
        if not force and (self._disabled or not self._dirty):
            return False
        with open(self.path, 'w') as f:
            # path is not saved, as it _is_always_ the filesystem path to this saved file
            yaml.dump(self.asDict(), stream=f)
            return True

    @classmethod
    def load(cls, path: Path) -> List['Manga']:
        if path.is_dir():
            path = path.joinpath('manga.yml')
        if not path.exists():
            return []
        parent = path.parent
        if parent in cls.all:
            return [cls.all[parent]]
        with open(path, 'r') as f:
            data = yaml.safe_load(f) or {}   # empty file returns None
            return [cls(path=path, dirty=bool(not data), **data)]

    @classmethod
    def save_directory(cls, path: Path, mangas: Iterable['Manga'] = None, recurse: bool = True) -> int:
        """Save an entire directory's metadata file to speed up loading"""
        if path.is_dir():
            path = path.joinpath('directory.yml')
        path_dir = path.parent

        if path.exists() and path not in cls._directories:
            logging.warning(f"{path} exists, but never loaded - saving would overwrite customizations")
            return 0

        mangas = cls.directory(path_dir, mangas=mangas, recurse=recurse)     # a list of Manga objects to save

        _directory = list([m.asDict(relative=path_dir)
                           for m in sorted(mangas, key=lambda v:v.name)])
        if not _args.dry:
            with open(path, 'wt') as f:
                yaml.dump(_directory, stream=f)
            logging.info(f"Saved directory[{len(_directory)}] to <{path_dir}>")
        else:
            logging.info(f"DRY!! directory[{len(_directory)}] to <{path_dir}>")
        return len(_directory)

    @classmethod
    def save_directories(cls, path: Path):
        """ Save all loaded directories relative to given path """
        for directory_path in cls._directories:
            if not directory_path.is_relative_to(path):
                continue
            cls.save_directory(directory_path.parent)

    @classmethod
    def get_path(cls, path: Path) -> List['Manga']:
        # Find all loaded manga that are children of the provided path.
        path = path.resolve()
        if not path.is_dir():
            return []
        path_str = str(path)
        found = []
        for manga_path in cls.all.keys():
            if str(manga_path).startswith(path_str):
                found.append(cls.all[manga_path])
        return found

    @classmethod
    def load_directory(cls, path: Path) -> List['Manga'] or None:
        if path.is_symlink():
            path = path.resolve()
        if path.is_dir():
            path = path.joinpath('directory.yml')
        if not path.exists() or not path.is_file():
            return

        path_dir = path.parent
        with open(path, 'r') as f:
            data = yaml.unsafe_load(f)
        cls._directories[path] = data

        def Manga_from(path: str, **kwargs) -> Manga or None:
            path = path_dir.joinpath(path)
            if not path.parent.exists():
                logging.error(f"{path} doesn't exist")
                return None
            return cls(path=path, **kwargs)

        if type(data) is dict:
            return list(Manga_from(name=k, **v) for k, v in data.items())
        return list(Manga_from(**d) for d in data)

    @classmethod
    def load_all(cls, path: Path, ignores: Set[Path] = None) -> List['Manga']:
        """ Load any manga.yml in path, return list """
        path = path.resolve()
        logging.debug(f"load_all: {path.name}")
        if not path or path.is_file() or not path.exists():
            return []
        ignores = ignores or set()
        if path in ignores:
            return []
        ignores.add(path)

        if path.is_dir():
            result = cls.load_directory(path) or cls.load(path)
            if result:
                return result

            loaded = []
            paths = Scans.scandir(path)
            for _dir in filter(lambda e: not e.is_symlink() and e.is_dir(), paths):
                loaded.extend(cls.load_all(_dir, ignores))
            return loaded
        return []

    @classmethod
    def create(cls, path: Path, **kwargs) -> 'Manga' or None:
        path = path.resolve()
        if not path or path.is_file() or not path.exists():
            return
        if path in cls.all:
            return
        return cls(path=path, **kwargs)

    @classmethod
    def create_all(cls, path: Path, ignores: Set[Path] = None, do_load=True, spinner: Callable = None) -> List['Manga'] or None:
        spinner = spinner or (lambda p: None)
        path = path.resolve()
        if not path or path.is_file() or not path.exists():
            return
        created = []    # Manga objects we create (infer from files) or load (from manga.yml)

        logging.debug(f"create_all: {path.name}")
        if ignores is None:
            ignores = set()
        ignores |= set(p for p in cls.all.keys())
        if do_load:
            created.extend(cls.load_all(path, ignores=ignores.copy()))
            ignores |= set(p for p in cls.all.keys())
        if path in ignores:
            return created
        ignores.add(path)

        Times.start(f"scan: {path.name}")
        paths = Scans.scandir(path)
        Times.stop()

        # if subdir is a manga, ignore me
        for p, _dir in enumerate(filter(lambda e: not e.is_symlink() and e.is_dir(), paths)):
            spinner(p / len(paths), _dir.name)
            sub_spin = lambda _p, _d: spinner((p + _p) / len(paths), f"{_dir.name}/{_d}")
            added = cls.create_all(_dir, ignores, do_load, spinner=sub_spin)
            if added:
                created.extend(added)
                if path in cls.all:     # we were created via recursion on a chapter dir
                    break
        spinner(1.0, '')
        if created:
            return created

        ignores.remove(path)    # no recursion left, unclutter the ignore list
        archives = 0
        images = 0
        for file in filter(lambda e: e.is_file(), paths):
            suffix = file.name.rsplit('.', 1)[-1]
            if suffix in cls.SUFFIXES['ARCHIVE']:
                archives += 1
            if suffix in cls.SUFFIXES['IMAGE']:
                images += 1
                if images > 3:
                    break
        if archives > images and archives >= 1:
            return [cls.create(path)]
        if images > 3:
            return [cls.create(path.parent)]

    @classmethod
    def all_errors(cls) -> dict:
        rv = {}
        for manga in cls.all.values():
            for k, v in manga.errors.items():
                s = rv.setdefault(k, set())
                s |= v
        return rv

    @classmethod
    def directory(cls, path: Path, mangas: Iterable['Manga'] = None, recurse: bool = False) -> Set['Manga']:
        # returns all manga under path, including manga in subdirs iff recurse
        # ex: path/mangaTitle{1..10}/ but not path/subpath/mangaTitleRecurse/ and never path/manga.yml
        mangas = mangas if mangas else cls.all.values()
        rv = set()
        for manga in mangas:
            manga_dir = manga.path.parent
            if manga in rv:
                logging.error(f"Directory found a duplicate? {manga}")
                continue
            if not manga_dir.is_relative_to(path) or manga_dir == path:
                # find manga _under_ path. not manga _at_ path or _above_ path
                continue
            if manga_dir.parent != path or not recurse:
                # path/x/manga_dir belongs to x/directory.yml rather than path/directory.yml
                continue
            rv.add(manga)
        return rv

    def sync_shutil(self, dst: 'Manga'):
        shutil.copy2(self.path.parent, dst.path.parent, follow_symlinks=True)

    def sync(self, dest: 'Manga', fails: dict = None) -> (int, int) or (None, None):
        """ sync chapters from self.path into dest.path skipping empty files and existing files """
        if dest._disabled:
            return (None, None)
        global _args
        fails = fails or {}
        files, bytes, count = 0, 0, 0

        def _to_cbz(chapter: Chapter, dst: Path) -> (int, int):
            # dst is parent directory
            nonlocal files, bytes
            if chapter.path.is_file():
                dst_file = dst.joinpath(f"{chapter}.{chapter.path.name.rsplit('.')[-1]}")
                if dst_file.exists() and not _args.overwrite:
                    raise FileExistsError(dst_file)
                dst_file = copy2(chapter.path, dst_file, follow_symlinks=True)
                logging.debug(f"copied: {chapter.path.name} -> {dst_file}")
                files += 1
                bytes += Path(dst_file).stat().st_size
            elif chapter.path.is_dir():
                cbz_file = dst.joinpath(f"{chapter}.cbz")
                addFile, addBytes = zip_path(src=chapter.path, zipfile=cbz_file, compression=zf.ZIP_BZIP2)
                files += addFile
                bytes += addBytes

        # only sync missing chapters
        missing = []
        for m in dest.missing(set(self.chapters)):
            missing.extend(self.chapters[m])
        for miss in missing:
            if _args.filter.fullmatch(miss.path.name):
                continue
            try:
                logging.info(f"sync: {miss} -> {dest.name}")
                _to_cbz(miss, dest.path.parent)
                count += 1
            except FileExistsError as e:
                fails[miss] = e
                pass
            except FileNotFoundError as e:
                fails[miss] = e
                logging.error(f"sync:FATAL {e}")
                break
            if len(fails) > _args.give_up:
                logging.error(f"sync give-up: ")
                break

        logging.info(f"sync {self.name} -> {dest.name} [{count}]:{', '.join(str(m) for m in missing)}")
        return files, bytes

    def missing(self, source: Set[float]) -> Set[float or int]:
        """Returns the NAMEs of chapters present in source but not self"""
        if self._disabled:
            return set()
        return source - set(self.chapters)

    def gaps(self) -> Set[float or int]:
        if not self.chapters:
            return set()
        chapters = set(self.chapters) | {1}
        start, end = min(chapters), max(self.chapters)
        return self.missing(set(ranges(start, end)))

    def gaps2(self) -> (list, List[float or int]):
        # if 1 not in chapters and 1.1 not in chapters:
        #    chapters += {1}
        def _pair(c):
            subc = c - int(c)
            while subc != int(subc):
                subc *= 10
            return (c, subc)

        def _pairrange(si, se, ei, ee):
            subc_max = max(se, ee)
            decimal = len(str(subc_max))
            if si == ei:
                return [si + round(x / (10 ^ decimal),decimal) for x in range(se + 1, ee)]
            else:
                rv = []
                for _ in range(si + 1, ei+1):
                    rv.extend(_pairrange(_, 0, _, ee))
                return set(rv)

        prev = 0
        missing: List[(float, float)] = []
        dups: list = []
        for chp in sorted(self.chapters):
            c0, cs = _pair(chp)
            p0, ps = _pair(prev)
            # d0, ds = c0 - p0, cs - ps

            missing.extend(_pairrange(c0, cs, p0, ps))

        return missing, dups

    @property
    def gap_str(self):
        gaps = [f"{a}-{b}" if a != b else str(a) for a, b in bookends(self.gaps())]
        return f"{self.name}: {', '.join(gaps)}"

    def rename_all(self):
        chapters = set()
        for s in self.chapters.values():
            chapters.update(s)
        for c in chapters:
            c.rename()

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f"{ self.__dict__ }"

    def _check_aliases(self, name: str) -> bool:
        for a in self.aliases:
            if type(a) is Pattern:
                if a.fullmatch(name):
                    return True
            if type(a) is str:
                if a == name:
                    return True
        return False

    def __eq__(self, other: Union[str, Path, 'Manga']):
        otherType = type(other)
        otherName = None

        if otherType is str:
            otherName = other
        elif otherType is Path:
            otherName = other.name
        elif isinstance(other, Manga):
            otherName = other.name

        if self.name == otherName:
            return True

        return self._check_aliases(otherName)


class _RX:                                          # Regular expression components
    SEP = r'\.'
    SPC = r'[ _.]'
    HEX = r'[0-9A-Fa-f]'
    CLEAN_HEX = fr'\[{HEX}{{8}}]'
    CLEAN_SEP = fr'{SEP}+'
    CLEAN_SPC = fr'{SPC}+'
    QUOTE = r'[`"\'](?![sm]\b)'
    CLEAN_DASH = r'\.*-?\.+'
    EPISODE = r'(?:e|E|ep|EP|episode(.)?|[Xx#.])(?P<episode>\d{2,3}(-\d{2,3})?([vp.]\d)?)' \
              r'(?=(\.|\[[^]]+\]|\([^)]+\)|[A-Za-z_]\w*)*$)'
    SEASON_EP = fr'(?:S|s|season)?(?P<season>[0-3][0-9]?){EPISODE}'
    VOLUME = r'(?:^|[.])(?i:v|vol|volume)\.?(?P<volume>\d{1,2})'
    CHAPTER_NUM = r'(?:[0-9]+([.p][0-9])?)|oneshot|bonus'
    CHAPTER = fr'(\b(?i:c|ch|chapter|\.*#))[#. -]*(?P<number>{CHAPTER_NUM}(-{CHAPTER_NUM})?)(v[1-9])?'
    NUMBER = fr'(:?^|[.#])(?P<number>{CHAPTER_NUM}(-{CHAPTER_NUM})?)(v[1-9])?'
    VERSION= r'(?:[1-9](?:\.[0-9])?|0(?:\.[0-9]{1,3}))'
    _TITLE= r'(?P<title>[a-zA-Z0-9].+)'            # a title - very broad
    TITLE  = rf'-[.]{_TITLE}'                      # a title after a dash - commonly: {group} vol ## ch ## - {title}
    TITLE_Q= rf'({QUOTE}){_TITLE}\1'               # a quoted title using **identical** quote glyphs
    TITLE_G= rf'[“「]{_TITLE}[」”]'                 # a quoted title using different (rare) glyphs
    TITLE_E= r'(?P<title>Episode.\d+(\.?[^[]+)*)'  # a title that starts like: Episode 27 the cat in the hat
    GROUP_B= r'\[(?P<group>[a-zA-Z0-9][^]]*)\]'    # a group name in square brackets
    GROUP  = r'(?i:uploaded.by.)?(?P<group>[a-zA-Z0-9][^-]+)'   # Match a group name called out with 'uploaded by'
    YEAR   = r'(?:\W)(?P<year>(19|20)\d{2})(?:\W)'


class Chapter:
    _RE = [(re.compile(r), f) for r, f in [         # a list of RX used to clean and extract info from filenames
        (_RX.CLEAN_SPC, None),
        (_RX.VOLUME, 'volume'),
        (_RX.GROUP_B, 'group'),
        (_RX.TITLE_Q, 'title'),
        (_RX.TITLE_G, 'title'),
        (_RX.CHAPTER, 'number'),
        (_RX.NUMBER, 'number'),
        (_RX.CLEAN_SPC, None),
        (_RX.TITLE_E, 'title'),
        (_RX.TITLE, 'title'),
        (_RX.CLEAN_SPC, None),
        (_RX.GROUP, 'group'),
    ]]

    __slots__ = ['title', 'numbers', 'group', 'volume', 'path']

    def __init__(self,
                 number: set or str or int or float,
                 path: Path = None,
                 group: str = None,
                 volume: int = None,
                 title: str = None):
        trim = lambda s: re.sub(r'(^[. _-]+|[. _-]+$)', '', s) if s else None
        self.title = trim(title)
        if not number and not volume:
            raise ValueError(f"number or volume is required: [{number}]")
        num = number
        if type(number) is str:
            if re.match('oneshot|bonus', number, re.IGNORECASE):
                num = [0]
            else:
                num = list(ranges(*number.split('-')))
        elif type(number) in (int, float):
            num = [number]
        self.numbers = set(num)
        self.group = trim(group)
        self.volume = volume
        self.path = path

    @staticmethod
    def name_to_pattern(name: str):
        return re.sub('[. ]+', name.lower(), '.')

    @classmethod
    def build(cls, manga: Manga, path: Path) -> 'Chapter':
        """ builds a chapter from a file path using regular expressions
        :param manga: The mange to which this file belongs - removed from the filename to
        :param path:
        :return:
        """
        if path.is_file() and '.' in path.name:
            name, ext = path.name.rsplit('.', 1)
        else:
            name = path.name
        name = re.sub(_RX.CLEAN_DASH, name, '.')
        name = name.replace(cls.name_to_pattern(manga.name), '.')
        parts = {'path': path}
        for regex, field in cls._RE:
            if field and field in parts:
                continue
            match = regex.search(name)
            if match:
                name = regex.subn('.', name, count=bool(field))[0]
                parts.update({k: v for k, v in match.groupdict().items() if v})
        if 'volume' in parts and 'number' not in parts:
            parts['number'] = 0
        if 'number' not in parts:
            raise ValueError("Unable to parse chapter number")
        return cls(**parts)

    @property
    def key(self) -> tuple:
        if self.numbers == {0}:
            return self.volume, self.group
        return min(self.numbers), max(self.numbers), self.group, self.volume

    def __contains__(self, item):
        if type(item) in (int, float):
            return item in self.numbers

    def __hash__(self):
        return hash(self.key)

    def __str__(self):
        nums = min(self.numbers), max(self.numbers)
        out = ''
        if self.volume and (not self.numbers or self.numbers == {0} or _args.volume):
            out = f"v{self.volume}"
        if not self.numbers or self.numbers == {0}:
            pass
        elif nums[0] == nums[1]:
            out +=f"c{nums[0]:05.1f}"
        else:
            out +=f"c{nums[0]:05.1f}-{nums[1]:05.1f}"
        if self.group:
            out += f".[{self.group}]"
        if self.title:
            out += f'."{self.title}"'
        return out

    def rename(self):
        name, ext = self.path.name, ''
        if self.path.is_file():
            name, ext = name.rsplit('.', 1)

        if name != str(self):
            new = self.path.parent.joinpath(f"{self}.{ext}")
            if new.exists():
                logging.error(f"RENAME: {name:.66} -> {new.name:66} EXISTS!")
            elif _args.dry:
                logging.info(f"RENAME: {name:66} -> {new.name:66} DRY!")
            else:
                self.path = self.path.rename(new)


def get_args():
    global _args
    parser = ArgumentParser()
    parser.add_argument('-S', '--save_each', help="Write metadata to each directory -- deprecated", action='store_true')  # save_each
    parser.add_argument('-V', '--save', help="Write directory metadata for destination", action='store_true', default=1)
    parser.add_argument('-c', '--create', help="Copy unmatched sources into destination", action='store_true', default=1)
    parser.add_argument('-n', '--dry', help="Don't sync sources to destination", action='store_true')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('-e', '--errors', help="Show missing chapters and other errors", action='count', default=0)
    parser.add_argument('-i', '--ignore', action='extend', nargs='+', default=['.*'])
    parser.add_argument('--full_sync', help="Don't skip sources based on existing chapter directories", action='store_true')
    parser.add_argument('-f', '--filter', type=str, help="a RegEx that ignores source chapters", default='\..*|.*_tmp|~.*')
    parser.add_argument('-F', '--file_filter', type=str, help="a RegEx that ignores individual files", default='\..*|.*_tmp|~.*')
    parser.add_argument('-M', '--min_size', type=int, help="min-size (skip tiny or empty files)", default=100)
    parser.add_argument('-O', '--overwrite', help="overwrite existing chapters", action='store_true')
    parser.add_argument('-s', '--source', type=str)
    parser.add_argument('-d', '--dest', type=str, default=Path('.').absolute())
    parser.add_argument('-T', '--timeout', type=float, help="rsync timeout", default=2.0)
    parser.add_argument('-G', '--give_up', type=int, default=3, help='give up error limit')
    parser.add_argument('-p', '--permission', help='set group permissions')
    parser.add_argument('-g', '--group', type=str, default=None, help='default group to assign on "created" manga')
    parser.add_argument('-r', '--rename', help='rename chapters to ensure sorting', action='store_true')
    parser.add_argument('--volume', help='keep volume when renaming', action='store_true')
    _args = parser.parse_args()
    _args.filter = re.compile(_args.filter)
    _args.file_filter = re.compile(_args.file_filter)
    return _args


def build_ignores(ignores=None) -> set:
    glob_re = re.compile(r'(\*|.*[^\\][*]).*')
    if type(ignores) is str:
        i = Path(ignores)
        if i.exists():
            if i.is_dir():
                return {i}
            return {i.parent}
        elif glob_re.match(ignores):
            ignores = set(i.parent.glob(i.name))
        else:
            raise ValueError(f"Bad ignores <{ignores}>")
    elif type(ignores) is list:
        ignores = set()
        [ignores.update(build_ignores(i)) for i in ignores.copy()]

    ignores |= set(Manga.all)
    return ignores


def read(source: Path, ignores=None, load_metadata=True) -> list:
    global _args
    if source is None:
        return []
    Times.start("LOAD")

    srcs = Manga.create_all(source.expanduser(), ignores=ignores, do_load=load_metadata, spinner=spin)
    srcs = list(filter(lambda v: bool(v), srcs))
    spin()
    logging.debug("Found: {}".format(pformat({v.name: v.path for v in srcs})))
    if not srcs:
        raise RuntimeError(f"{source} nothing found in source")
    else:
        for k, v in Manga.all_errors().items():
            for msg in v:
                logging.error(f"{k} - {msg}")
    print(f"Loaded {source.name}: {Times.stop('LOAD'):4.2f}s")
    return srcs


def do_all_permission(path: Path, mangas: List[Manga] = None):
    global _args
    if not _args.permission or _args.dry:
        logging.info("Skipping permissions")
        return
    if not mangas:
        mangas = [m for m in Manga.all.values() if m.path.parent.is_relative_to(path)]
    for m in mangas:
        m.set_group()


def print_result(src: Manga, dst: Manga, files: int = -1, _bytes: int = -1):
    if files is None or _bytes is None:
        pass  # handled by sync logging.error ## print(f"!{src.name} {copy_fails.get(src.path.parent)}")
    elif files >= 0 and _bytes >= 0:
        print(f"files:{files} {_bytes / (1024 ** 2):.1f}MB {src.name:.50} -> {dst.path.parent.name:.50}")
    elif files == 0:
        logging.debug(f"{_bytes / (1024**2):.1f}MB {src.name:.75} -> {dst.path.parent.name:.75}")
    else:
        logging.error(f"!! {src.name:.66} -> {dst.path.parent.name:.66}")


def do_sync(srcs: List[Manga], dsts: List[Manga], copy_fails: dict = None) -> Dict[str, Manga]:
    """Sync directories from srcs into matching dsts - returns dict of manga.name -> Manga
        if --dry, matching is still performed, but no actual file copy is done
    """
    global _args
    missing, f, b = {}, 0, 0
    Times.start('SYNC')
    if not srcs or not dsts:
        raise ValueError(f"sync requires srcs and dsts: {srcs}/{dsts}")
    for src in srcs:
        dst = None
        try:
            dst = dsts[dsts.index(src)]
            if not _args.full_sync and not dst.missing(set(src.chapters)):
                logging.debug(f"{src.name}[{len(src.chapters)}] ~= [{len(dst.chapters)}]{dst.name}")
                continue
        except ValueError:
            missing[src.name] = src
            if _args.create:
                dst = Manga(path=_args.dest.joinpath(src.name))     # Create Manga object based on the destination
            else:
                logging.warning(f"{src.name} no destination")
        try:
            if not _args.dry and dst:
                f, b = src.sync(dst, copy_fails)
                print_result(src, dst, files=f or 0, _bytes=b or 0)
        except FileExistsError as e:
            copy_fails[src.name] = e
        except FileNotFoundError as e:
            copy_fails[src.name] = e
        except NotADirectoryError as e:
            copy_fails[src.name] = e
    print(f"Sync took {Times.stop('SYNC'):4.2f}s")
    return missing


def work():
    global _args
    get_args()
    logging.root.setLevel(max(0, logging.WARNING - _args.verbose * 10))

    os.umask(0o07)
    source = Path(_args.source).expanduser() if _args.source else None
    dest = Path(_args.dest).resolve() if _args.dest else None

    build_ignores(ignores=_args.ignore)
    # Infer source manga directories, no metadata files exist so skip loading
    srcs = read(source, load_metadata=False)
    # Infer (or load) destination manga directories
    dsts = read(dest, ignores={source})

    if _args.errors:
        for s in (*dsts, *srcs):
            if s.gaps():
                print(s.gap_str)

    if _args.rename:
        for s in dsts:
            s.rename_all()

    def check_bad_params():
        # detect bad parameters and stop
        if not _args.dry or _args.create:
            if not source or not dest:
                raise ValueError(f"Sync requires valid Source and Dest: {source.name}/{dest.name}")
            if dest.is_relative_to(source):
                raise ValueError(f"{dest.name} is within {source}")
        if _args.save or _args.save_each:
            if not dest:
                raise ValueError(f"Save requires Dest: {dest}")

    # find matching src/dst and rsync src -> dst
    copy_fails = {}
    if srcs and dsts:
        missing = do_sync(srcs, dsts, copy_fails)
        # copy_missing(source, dest, missing, copy_fails)

    # write metadata
    Manga.save_directories(path=dest)
    do_all_permission(dest)

    # report errors
    if copy_fails:
        print("---ERROR failed to copy these sources: ---")
        logging.error(pformat(copy_fails))


# ============== MAIN =====================
if __name__ == "__main__":
    work()

#################    #################
class AYBABTU:
    """ Attempt ncurses interface
    """
    screen = None
    win_bar = None

    @classmethod
    def start(cls):
        cls.screen = curses.initscr()
        x, y = cls.screen.getmaxyx()
        cls.win_bar = cls.screen.subwin(y - 1, 0)

    @classmethod
    def refresh(cls, state: str = "Scanning", counter: int = 0, color=curses.COLOR_YELLOW):
        cls.win_bar.addstr(0, 0, f"{state:<15}{counter:>3}", attr=color)
        cls.screen.refresh()


def copy_missing(source: Path, dest: Path, missing: dict, copy_fails: dict):
    # DEPRECATED
    """Syncs from missing into dest:Path by creating hollow Manga objects with dest/name
    skipped if no --create
    """
    global _args
    # Unmatched src get created or reported
    if missing and _args.create:
        Times.start('CREATE')
        # src: Manga or None = None
        for name, src in missing.items():
            try:
                dir = dest.joinpath(name)
                dst = Manga(path=dir)     # Create Manga object based on the destination
                f, b = src.sync(dst, copy_fails)
                print_result(src=src, dst=dst, files=f, _bytes=b)
            except FileExistsError as e:
                copy_fails[src.name] = e
            except FileNotFoundError as e:
                copy_fails[src.name] = e
            except NotADirectoryError as e:
                copy_fails[src.name] = e
        print(f"Creates completed in {Times.stop('CREATE'):4.2f}s")
    elif missing:
        logging.error(f"These sources lacked destination and were not copied:")
        for name, src in missing.items():
            print(f"{name}@{src.path.relative_to(source)}")
    elif not missing:
        print("No New Mangas detected")


