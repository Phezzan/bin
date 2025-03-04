#!/bin/env python
"""
Wait in a system-wide queue until the system is idle
Designed to allow workloads to wait in a queue to execute one at a time.
"""
import argparse
import os
import pickle
import logging
import random
from datetime import datetime, UTC
from pathlib import Path
from time import sleep

import fcntl
import psutil

logging.basicConfig(level=logging.WARNING)

PPID = os.getppid()
QUEUE_PATH = Path('/tmp/.idle_queue.pickle')
SLEEP_TIME = 1.0
IDLE_PCT = 20


def wait_until_idle(idle_pct:int, interval: float):
	assert 5 <= idle_pct <= 95, "idle_pct must be between 5 and 95"
	assert 0.2 < interval < 9.0
	cpu: float = 0.0
	while (cpu := psutil.cpu_percent(interval=interval, percpu=False)) > idle_pct:
		continue
	return


class open_locked_file:
	_locks = {
		'r': (fcntl.LOCK_SH, 'r: shared'),
		'rb': (fcntl.LOCK_SH, 'rb: shared'),
	}
	
	def __init__(self, path: Path, mode:str = 'rb'):
		self.path = path
		self.mode = mode
		self.file = None
		self.lock_mode = self._locks.get(mode, (fcntl.LOCK_EX, f'{mode}: exclusive'))
		logging.debug(f"Open and lock: {path} ...")

	def __enter__(self):
		while self.file is None or self.file.closed:
			try:
				if self.file is None or self.file.closed:
					logging.debug(f"Open: {self.path} {self.lock_mode[1]}")
					self.file = open(self.path, self.mode)
				if self.lock_mode:
					logging.debug(f"Locking: {self.path} {self.lock_mode[1]}")
					fcntl.lockf(self.file, self.lock_mode[0])
				break
			except PermissionError as err:
				logging.debug(f"Failed to open {self.path}\n{err}")
				sleep(random.random() + 0.5)
			except OSError as err:
				logging.debug(f"Failed to lock {self.path}\n{err}")
				sleep(random.random() + 0.5)
		return self.file

	def __exit__(self, type, value, traceback): 
		if not self.file or self.file.closed:
			return
		fcntl.lockf(self.file, fcntl.LOCK_UN)
		self.file.close()
		self.file = None


def add_to_queue(path: Path, ppid: int) -> tuple[int, list[int]]:
	# add 
	with open_locked_file(path, 'r+b') as f:
		queue = pickle.load(f)
		try:
			position = queue.index(ppid)
		except ValueError as err:
			position = len(queue)
			queue.append(ppid)
			f.seek(0)
			pickle.dump(queue, f)
	logging.info(f"Added {PPID} at {position}")
	return position, queue


def queue_position(path: Path, ppid: int) -> tuple[int, list[int]] | tuple[None, None]:
	with open_locked_file(path) as f:
		queue = pickle.load(f)
	try:
		return queue.index(ppid), queue
	except: 
		return None, None


def remove_process(path: Path, ppid: int) -> list[int] | None:
	with open_locked_file(path, 'r+b') as f:
		queue = pickle.load(f)
		try:
			ndx = queue.index(ppid)
		except IndexError as err:
			logging.error(f"unable to remove: {ppid} from >{queue}<")
			return None
		assert ndx == 0, f"removing ndx == 0... but its {ndx}"
		queue.pop(ndx)
		f.seek(0)
		pickle.dump(queue, f)
		f.truncate()
	return queue


def wait_idle_queue(idle_pct: int = IDLE_PCT, interval: float = SLEEP_TIME, queue_file: Path = QUEUE_PATH):
	"""
if queue exists, append parent PID and wait for IDLE.  
On IDLE check:
    block on FLOCK 
	await idle
	read queue
	if position > 1 UNLOCK, sleep (position * SLEEP_TIME) await IDLE CHECK
	assert position == 1
	"""
	start = datetime.now(tz=UTC)
	position = None

	if not queue_file.exists():
		queue = [PPID,]
		queue_file.write_bytes(pickle.dumps(queue))
		queue_file.chmod(0o666)
		position = 0
	else:
		position, queue = add_to_queue(queue_file, PPID)

	while position > 1:
		logging.debug(f"waiting for idle [{position}]")
		wait_until_idle(idle_pct, position * interval * 2)
		position, queue = queue_position(PPID)
	
	assert position < 2

	while position == 1:
	# if queue[0] has subprocesses (create set compare previous, warn if same), then loop: await idle
	# no subprocess then pop(top), UNLOCK, exit(0)
		try: 
			current = psutil.Process(queue[0])
		except psutil.NoSuchProcess as err:
			current = None

		if current is None or not (children := current.children()):
			queue = remove_process(queue_file, queue[0])
			position = queue.index(PPID)
			break
		else:
			logging.debug(f"watching {children[0].name}[{len(children)}] for completion")
			wait_until_idle(idle_pct, interval)

	wait_until_idle(idle_pct, 0.5)
	end = datetime.now(tz=UTC)
	logging.info(f"waited: {round((end - start).total_seconds())} seconds")
	return position


def get_args():
	ap = argparse.ArgumentParser(description=__doc__)
	ap.add_argument('-d', '--delay', action='store', type=int, default=1, help='# of seconds to wait between checks')
	ap.add_argument('-v', '--verbose', action='count', help="'-v' for info, '-vv' for debug")
	ap.add_argument('pct', type=int, nargs='?', default=16, help='What usage %% is considered idle')
	return ap.parse_args()

if __name__ == "__main__":
	args = get_args()
	if args.verbose:
		logging.root.setLevel(max(logging.DEBUG, logging.WARN - 10 * args.verbose))
	wait_idle_queue(max(5, args.pct), max(0.5, args.delay), queue_file=QUEUE_PATH)
	exit(0)

# vim: ts=4 sw=4
