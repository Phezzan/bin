#!/usr/bin/env python3

# Classifiers for media files
#   Classes: [ [Regex, CLASS] ... ]

# Choose rename rules based on class
# Extract strings from filename (Title, year, episode, ...)
# Compose new filename based on class rules
# rename(file, NewName)

# TODO: learn to import Classes, rules, regexes from... ? yaml ?
#   use pickle or yaml or json
# TODO: Extract composition rules from a string? (or just use
from typing import Type, Iterable
import json
import argparse
import os
from stat import S_IRUSR, S_IWUSR, S_IRGRP, S_IXUSR, S_IXGRP, S_ISDIR
import re
import logging
import grp
import pwd



SEP       = r'.'
SEP_RX    = r'[ _.]'
EPISODE_RX = r'(?:e|E|ep|EP|episode(.)?|[Xx#.])(?P<episode>\d{2,3}(-\d{2,3})?([vp.]\d)?)'\
        r'(?=(\.|\[[^]]+\]|\([^)]+\)|[A-Za-z_]\w*)*$)'
SEASON_EP_RX = rf'(?:S|s|season)?(?P<season>[0-3][0-9]?){EPISODE_RX}'

VERSION_RX= r'(?:[1-9](?:\.[0-9])?|0(?:\.[0-9]{1,3}))'
GROUP_RX  = r'(?P<group>\[[a-zA-Z0-9][^]]+\])'
TITLE_RX  = r'(?P<title>[a-zA-Z0-9](.?[a-zA-Z0-9!@&+,:-]+)*[+a-zA-Z0-9]+)'
YEAR_RX   = r'(?:\W)(?P<year>(19|20)\d{2})(?:\W)'

RIP_RX    = r'(?i:multi|webrip|web-dl|brrip|bluray|hdtv|hevc|10bit)'
VIDEO_RX  = r'(?:\d{3,4}x\d{3,4}|720p|480p|1080p|x26[45])' 
SOUND_RX  = rf'(?:(?:6|2|5|5.1)CH|AAC({VERSION_RX})?)'
SUB_RX    = r'(?i:(en|fr|jp|jpn|ger)[. -]sub)'
ENCODE_RX = rf'\b(?:{VIDEO_RX}|{SOUND_RX}|{RIP_RX})\b'

LOG_FORMAT= r'%(message)s'
KB        = 1024
MB        = KB*KB
GB        = MB*KB

# CLEAN lists are regex replace patterns that eliminate uninteresting bits
HEX_RX = r'[0-9A-Fa-f]'
MANGA_CLEAN_RX = [
    rf'\[{HEX_RX}{{8}}]',
    rf'{SEP_RX}+',
    r'\.*-?\.+',
]
MANGA_CLEAN = [re.compile(r) for r in MANGA_CLEAN_RX]

VIDEO_CLEAN_RX = [
    rf'{SEP_RX}+',
    rf'[(\[][^\])]*{ENCODE_RX}[^])]*[])]',
    ENCODE_RX,
    rf'\[{HEX_RX}{{8}}\]',
    r'\.*-?\.+',
]
VIDEO_CLEAN = [re.compile(r) for r in VIDEO_CLEAN_RX]

BOOK_REG = [
    r'((?P<group>\[[^]]+\])[ _.-])?' +
    r'(?P<title>[a-zA-Z0-9 ._!@&+,-]*)[ _.-]' +
    r'((?P<volume>(v)?\d+)[ _.-]*)?' + 
    r'(?:#|CH|ch|Ch|c|[ .])(?P<chapter>\d\d+)' +
    r'((?P<group2>\[[^]]+\])[ _.-])?',
]

IGNORE_EXT = ['.txt', '.mp3', '.flac', '.jpg', '.png', '.nfo']

VALID_GROUPS = {grp_st.gr_name: grp_st for grp_st in grp.getgrall() if re.match(r'tv|rating_\d+', grp_st.gr_name)}

# VALID_USERS = { user : id in VALID_GROUPS.members }
VALID_USERS = set()
[VALID_USERS.update(gr.gr_mem) for gr in VALID_GROUPS.values()]


class Renamer(object):
    def __init__(self, name: str, clean=None, rx=None, parts=None, nameFmt=None, size=None, ext=None):
        """
        clean - a list of patterns that will be replaced by SEP ('.')
        rx - a list of patterns that create a dictionary of 'parts' that describe the file.
        parts - a list initializer
        """
        self.name = name
        self.clean = clean or []
        self.rx = rx or []
        self.parts = parts or {}
        self.nameFmt = nameFmt or None
        self.size = size or (KB, 9 * GB)    # less than 1KB is probably a link
        self.ext = ext or []

    @staticmethod
    def load(name, path='.'):
        with open(path + '/' + name) as renameCfg:
            me = Renamer(name, **json.load(renameCfg))
        me.compile()
        return me

    @staticmethod
    def fromDict(d):
        me = Renamer(**d)
        me.compile()
        return me

    def compile(self):
        rx = list(self.rx)
        self.rx = []
        for r in rx:
            logging.debug("Compiling: %s" % r)
            self.rx.append(re.compile(r))

    def toJson(self):
        return json.dumps(self.__dict__)

    def save(self, name, path = '.'):
        with open(path + '/' + name, mode='w') as renameCfg:
            json.dump(self.__dict__, renameCfg)

    def canName(self, fn):
        name, ext = os.path.splitext(fn)
        ext = ext.lower()
        if not ext in self.ext:
            logging.debug("%s not in %s" %(ext, self.ext))
            return None

        statinfo = os.stat(fn)
        size = statinfo.st_size
        if size > self.size[1] or size < self.size[0]:
            logging.info("%s bad size: %d<%s<%d" %(self.name, self.size[0], name, self.size[1]))
            return None

        for reg in self.clean:
            name = reg.sub(SEP, name)   # replace everything matched by clean patterns with SEP

        found = self.rx[0].search(name)
        if not found:
            logging.warning("%s not found: %s" %(self.name, self.rx[0]))
            return None

        # isn't this check redundant?
        #for k in found.groupdict().keys():
        #    if not k in self.parts.keys():
        #        return None
        return found

    def rename(self, fn):
        name, ext = os.path.splitext(fn)
        ext = ext.lower()
        if not ext in self.ext:
            return None

        parts = self.parts.copy()
        for reg in self.clean:
            name = reg.sub(SEP, name)

        for reg in self.rx:
            match = reg.search(name)
            if match:
                gdict = match.groupdict()
                for k, v in gdict.items():
                    if v:
                        parts[k] = v
                logging.debug("%-8s:%45s gdict: %s" % (self.name, name, gdict))
                name = reg.sub('.', name)
            else:
                logging.debug("%-8s:%45s fail: %s" % (self.name, name, reg))

        for v in parts.values():
            if v is None:
                return None
        for f in self.nameFmt:
            try:
                name = f.format(**parts)
            except:
                logging.debug("missing bits: %s", f)
                name = None
                continue
            else:
                break

        if name:
            name += ext

        return name


def main():
    global config
    config   = Config()
    renamers = config.renamers
    choice   = None
    files = {}
    for name in config.files:
        base     = os.path.basename(name)
        path     = name[:-len(base)]
        newBase  = None
        choice   = None
        ext      = os.path.splitext(name)[1]   

        if ext in config.ignore:
            logging.debug("ignore: %s: %s" % (path, base))
            continue
        if ext == '.txt' and os.path.exists(name) and os.stat(name).st_size < 100000:
            with open(name) as f:
                config.add_files(f.readlines())
            continue

        if path and not os.path.exists(path):
            print("Not found: %s: %s\n" % (path, base))
            continue

        for r in renamers:
            newBase = r.rename(base)
            if not newBase:
                logging.debug("%s can't rename %s" % (r.name, base))
                continue
            choice = r
            break


        if not choice or not newBase:
            logging.warning("No conversion for '%s'" % base)
        elif newBase == base:
            files[name] = name
        elif os.path.exists(path + newBase):
            logging.warning("exists: %s" % (path + newBase))
        else:
            newName = path + newBase
            logging.log(99, "%-8s %60s > %-40s" % (choice.name, name, newName))
            if newName in files.values():
                logging.error("%-8s %60s > %-40s", "Collide", name, newName)
            else:
                files[name] = path + newBase

#    if files and (not config.nochange and not config.noperm):
#        for name in config.files:
#            if name in self.files or path_in_files(name, config.files)
#                set_auth(name, config.owner, config.group)

    for k,v in files.items():
        if config.nochange :
            print("%-8s %40s > %-40s" %("dryRun", k, v))
        elif k == v:
            print("%-8s %40s = %-40s" %("unchanged", k, v))
            if config.perm or config.group or config.owner:
                set_group(k, config.owner, config.group)
        elif os.path.exists(v):
            print("%-8s %40s > %-40s" %("collide", k, v))
        else:
            if config.perm or config.group or config.owner:
                set_group(k, config.owner, config.group)
            if k != v:
                os.replace(k,v)
            print("%-8s %40s > %-40s" %("renamed", k, v))
    # End of Main


def path_in_files(path, files):
    path_len = len(path)
    for f in files:
        if f[:path_len] == path:
            return True


def init_renamers(force: Type) -> Iterable[Renamer]:
    renamers = []

    s = {
        "name": "Series",
        "rx": [SEASON_EP_RX,
               #r'(?:S|s|season)?(?P<season>\d+)'
               #r'(?:e|E|ep|EP|episode(.)?|[Xx#.])(?P<episode>\d{2,3}(-\d{2,3})?(v\d)?)'
               #r'(?=(\[[^]]+\]|\([^)]+\)|\.|[^0-9])*$)',
               YEAR_RX,
               GROUP_RX,
               TITLE_RX,
               ],
        'clean': VIDEO_CLEAN,
        "parts": {"title": None, "season": None, "episode": None},
        "nameFmt": [
            "{title}"+SEP+"({year})"+SEP+"S{season:0>2}E{episode:0>2}"+SEP+"{group}",
            "{title}"+SEP+"S{season:0>2}E{episode:0>2}"+SEP+"{group}",
            "{title}"+SEP+"S{season:0>2}E{episode:0>2}",
            ],
        "ext": [ ".mkv",".mp4", ".avi", ".m4v", ".ogv", ".ogm"],
        "size": (80*MB, 2.5*GB)
    }

    ova         = s.copy()
    ova["parts"]= {"title": None, "season": None, "episode": None}

    ova["nameFmt"] = [
                "{title}"+SEP+"({year})"+SEP+"OVA.E{episode:0>2}"+SEP+"{group}",
                "{title}"+SEP+"OVA.E{episode:0>2}"+SEP+"{group}",
                "{title}"+SEP+"OVA.E{episode:0>2}",
                ]
    ova["cleanFmt"] = [
                "{title}"+SEP+"({year})"+SEP+"OVA.E{episode:0>2}"+SEP+"{group}",
                "{title}"+SEP+"OVA.E{episode:0>2}"+SEP+"{group}",
                "{title}"+SEP+"OVA.E{episode:0>2}",
                ]

    sub         = s.copy()
    sub["ext"]  = [".srt", ".ass", ".ssa", ".sub", ".idx"]
    sub["size"] = (KB, 5*MB)
    sub['name'] = 'Subs'

    m           = s.copy()
    m['rx']     = [ YEAR_RX, GROUP_RX, TITLE_RX ]
    m['parts']  = {"title":None}
    m['nameFmt']= ["{title}"+SEP+"({year})"+SEP+"{group}",
                   "{title}"+SEP+"({year})",
                   "{title}"+SEP+"{group}",
                   "{title}"]
    m['size']   = (250*MB, 10*GB)
    m['name']   = 'Movie'

    e           = s.copy()
    e["rx"]     = s['rx'][:]
    e['rx'][0]  = EPISODE_RX
                #(  r'(e|E|ep|EP|episode.|\b[Xx#.])(?P<episode>\d{2,3}(-\d{2,3})?(v\d)?)'
                #   r'(?=(\.|\[[^]]+\]|\([^)]+\)|[^0-9])*$)')
    e["parts"]  = {"title":None, "episode":None }
    e["nameFmt"]= [
                "{title}"+SEP+"({year})"+SEP+"E{episode:0>2}"+SEP+"{group}",
                "{title}"+SEP+"({year})"+SEP+"E{episode:0>2}",
                "{title}"+SEP+"E{episode:0>2}"+SEP+"{group}",
                "{title}"+SEP+"E{episode:0>2}",
                ]
    e["name"]   = "Episode"

    s2b         = e.copy()
    s2b["ext"]  = [".srt", ".ass", ".ssa", ".sub", ".idx"]
    s2b["size"] = (KB, MB)
    s2b['name'] = 'Subs2'

    vc = {"name" : "Manga.Chapter",
           "rx": [
               r'((?<=\D)(?P<v>v|V|vol|Vol|volume|Volume)?(?P<volume>\d{1,2})[.]?)?'
               r'(?:c|C|ch|CH|Ch|(Chapter\.)|[x#. ])(?P<chapter>\d{2,3}(-\d{2,3}|\.[1-5])?(\.?v\d)?)',
               GROUP_RX,
               TITLE_RX,
               ],
           'clean': MANGA_CLEAN,
           "parts": {"title": None, "v": "v", "volume": None, "chapter": None},
           "nameFmt": ["{title}"+SEP+"{v}{volume:0>2}ch{chapter:0>2}"+SEP+"{group}",
                       "{title}"+SEP+"ch{chapter:0>2}"+SEP+"{group}",
                       "{title}"+SEP+"{v}{volume:0>2}ch{chapter:0>2}",
                       "{title}"+SEP+"ch{chapter:0>2}"],
           "ext": [".zip", ".rar", ".tar", ".tgz", ".tbz", '.7z' ],
           "size": (1*MB, 250*MB)  
           }
    c          = vc.copy()
    c['rx'][0] = r'(?:c|C|ch|CH|Ch|(Chapter\.)|[x#. ])(?P<chapter>\d{2,3}(-\d{2,3}|\.[1-5])?(\.?v\d)?)'
    c["parts"] = {"title": None, "chapter": None}

    v          = vc.copy()
    v["rx"]    = list(c["rx"])
    v['rx'][0] = r'(?<=\D)(?:v|V|vol|Vol|volume|Volume)(?P<volume>\d+(extra|Extra)?)'
    v["name"]  = "Manga.Volume"
    v["parts"] = {"title": None, "v":"v", "volume": None}
    v["nameFmt"] =["{title}"+SEP+"v{volume}"+SEP+"{group}",
                   "{title}"+SEP+"v{volume}"]

    b           = m.copy()
    b['clean']  = MANGA_CLEAN
    b["ext"]    = [".pdf", ".epub", ".mobi"]
    b['rx']     = [ YEAR_RX, TITLE_RX ]
    b['nameFmt']= ["{title}"+SEP+"({year})",
                   "{title}"]
    b['size']   = (10*KB, 80*MB)
    b['name']   = 'Book'

    renamers.append(s)
    renamers.append(e)
    renamers.append(sub)
    renamers.append(s2b)
    renamers.append(m)
    renamers.append(c)
    renamers.append(v)
    renamers.append(vc)
    renamers.append(b)

    rT = []
    for r in renamers:
        rT.append(Renamer.fromDict(r))
    return rT


def user_name(uid):
    return pwd.getpwuid(uid).pw_name


def group_name(gid):
    return grp.getgrgid(gid).gr_name


def get_group(path=None):
    path = path or os.getcwd()
    gid  = os.stat(path).st_gid
    return grp.getgrgid(gid)


def get_groupid(name):
    return grp.getgrnam(name)[2]


class Config(object):
    def __init__(self, arg_v=None):
        ap = argparse.ArgumentParser(description="Automatically rename video and comics")
        ap.add_argument("-v", "--verbose", help="enable logging", action='count')
        ap.add_argument("-f", "--force", help="enable logging", type=str)
        ap.add_argument("-e", "--ext", help="extensions to rename")
        ap.add_argument("-i", "--ignore", help="extensions to ignore (overrides --ext)")
        ap.add_argument("-n", "--nochange", help="no changes", action='store_true')
        ap.add_argument("-c", "--clean", help="does name.number.etc.ext", action='store_true')
        ap.add_argument("-g", "--group", default=None, help="set group to value or dir group")
        ap.add_argument("-o", "--owner", default=None, help="set owner to value or dir owner")
        ap.add_argument("-p", "--permission", default=None, help="set permissions regardless",action='store_true')
        ap.add_argument("files", nargs="+", default=[], help="files to rename")

        args = ap.parse_args(arg_v)
        if not args.verbose or args.verbose < 1:
            logging.basicConfig(level=logging.WARNING, format=LOG_FORMAT)
        elif args.verbose > 1:
            logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
        else:
            logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
        self.verbose = args.verbose or 0
        self.ext = args.ext or []
        self.ignore = args.ignore or []
        self.nochange = args.nochange or False
        self.clean = args.clean or False
        self.files = {}
        self.add_files(args.files)
        self.renamers = init_renamers(args.force)
        for ext in self.ignore:
            if ext in self.ext:
                self.ext.remove(ext)
        for ext in self.ext:
            if os.path.exists("%s.json"):
                pass    # load saved renamers

        if args.group:
            try: args.group = "rating_%d" % (int(args.group))
            except ValueError: pass
        self.owner = args.owner
        self.group = args.group
        self.perm  = args.permission
        self.validate()

    def validate(self):
        if self.owner not in VALID_USERS and self.owner not in [None, True]:
            logging.error("invalid user: %s" % (self.owner))
            self.nochange = True
            self.owner = None
    
        if self.group not in VALID_GROUPS and self.group not in [None, True]:
            logging.error("invalid group: %s" % (self.group))
            self.nochange = True
            self.group = None

    def add_files(self, li):
        c = 0
        for f in li:
            f = f.strip()
            f = os.path.abspath(f)
            if f in self.files or re.match(r'\s*#.*', f):
                continue
            try:
                stats = os.stat(f)
                if not stats:
                    logging.error("Not Found: %s"%(f))
                    continue
                mode  = stats.st_mode
                self.files[f] = None
                if S_ISDIR(mode):
                    self.files[f] = True
                    for root, dirs, files in os.walk(f):
                        self.files.update({os.path.join(root,dn):True for dn in dirs})
                        self.files.update({os.path.join(root,fn):None for fn in files})
            except:
                c += 1
                logging.exception("exception at %s"%(f))
            if c > 5:
                logging.error("Aborting add_files")
                break

if __name__ == "__main__":
    main()
