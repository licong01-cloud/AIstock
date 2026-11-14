"""Bounded per-process TTL cache, coalesced identical reads, two concurrent builds."""
from collections import OrderedDict
from threading import BoundedSemaphore, Event, Lock
from time import monotonic

from .projection import WorkbenchError


class ComparisonCache:
    def __init__(self, ttl=90, capacity=8, concurrency=2, clock=monotonic):
        self.ttl, self.capacity, self.clock = ttl, capacity, clock
        self.lock, self.slots = Lock(), BoundedSemaphore(concurrency)
        self.values, self.pending = OrderedDict(), {}

    def get(self, key, build, *, force=False):
        with self.lock:
            if force:
                self.values.pop(key, None)
            cached = self.values.get(key)
            if cached and cached[0] > self.clock():
                self.values.move_to_end(key)
                return cached[1]
            event = self.pending.get(key)
            if event is None:
                event = Event()
                self.pending[key] = event
                owner = True
            else:
                owner = False
        if not owner:
            if not event.wait(20):
                raise WorkbenchError('COMPARISON_BUSY', '对比正在计算，请稍后刷新。', 503)
            # Explicit refresh must really re-read sources after an older in-flight read.
            return self.get(key, build, force=force)
        try:
            if not self.slots.acquire(timeout=2):
                raise WorkbenchError('COMPARISON_BUSY', '对比请求较多，请稍后刷新。', 503)
            try:
                value = build()
            finally:
                self.slots.release()
            with self.lock:
                self.values[key] = (self.clock() + self.ttl, value)
                self.values.move_to_end(key)
                while len(self.values) > self.capacity:
                    self.values.popitem(last=False)
            return value
        finally:
            with self.lock:
                self.pending.pop(key, None)
                event.set()


comparison_cache = ComparisonCache()
