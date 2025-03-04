#!/usr/bin/env python3
"""
pgen - Generate password permutations containing a base string

Lots of people have a 'base' password and generate variants from it...
But what if you forgot which variant you used for a particular vault?

This tool builds a list of passwords for your base and you can slowly try each in turn on a schedule until you find the right one.
Provide your base, characters to permute, and the min and max length.

"""

import re
import argparse
import itertools
import string
from string import ascii_letters, ascii_lowercase, ascii_uppercase, printable, digits, hexdigits, printable, whitespace

def get_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('max', help='maximum total length', type=int)
    ap.add_argument('min', help='minimum total length', default=1, type=int)
    ap.add_argument('-b', '--base', help='a string to use in all generated passwords', type=str, default='')
    ap.add_argument('-p', '--pos', help='lock the start position of base in all output strings', default=None, type=int)
    ap.add_argument('-c', '--chars', help='characters to use in permutations', default=set(printable) - set(whitespace))
    return ap.parse_args()

def lexico_permute_string(s):
    ''' Generate all permutations in lexicographic order of string `s`

        This algorithm, due to Narayana Pandita, is from
        https://en.wikipedia.org/wiki/Permutation#Generation_in_lexicographic_order

        To produce the next permutation in lexicographic order of sequence `a`

        1. Find the largest index j such that a[j] < a[j + 1]. If no such index exists, 
        the permutation is the last permutation.
        2. Find the largest index k greater than j such that a[j] < a[k].
        3. Swap the value of a[j] with that of a[k].
        4. Reverse the sequence from a[j + 1] up to and including the final element a[n].
    '''

    a = sorted(s)
    n = len(a) - 1
    while True:
        yield ''.join(a)

        #1. Find the largest index j such that a[j] < a[j + 1]
        for j in range(n-1, -1, -1):
            if a[j] < a[j + 1]:
                break
        else:
            return

        #2. Find the largest index k greater than j such that a[j] < a[k]
        v = a[j]
        for k in range(n, j, -1):
            if v < a[k]:
                break

        #3. Swap the value of a[j] with that of a[k].
        a[j], a[k] = a[k], a[j]

        #4. Reverse the tail of the sequence
        a[j+1:] = a[j+1:][::-1]


def each_base_pos(pstr: str, base: str, pos: int = None):
    if pstr in ('', None):
        return base
    
    all_positions = list(range(len(pstr)+1)) if pos is None else [pos]

    for pos in all_positions:
        yield f"{pstr[:pos]}{base}{pstr[pos:]}"
    return

def each_len(len: int, args:argparse.Namespace):
    for c in itertools.combinations_with_replacement(args.chars, len):
        for p in lexico_permute_string(c):
            for out in each_base_pos(pstr=p, base=args.base, pos=args.pos):
                yield out
    return


def main():
    args = get_args()

    for l in range(args.min, args.max+1):
        for s in each_len(len=l, args=args):
            print(s)


if __name__ == "__main__":
    main()
