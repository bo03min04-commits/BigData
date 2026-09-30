"""추가 라이브러리 없이 읽고 수정할 수 있는 교육용 구현."""
import math
import random
from array import array
from .common import stable_hash

def reservoir(stream, k, seed=42):
    if k < 0:
        raise ValueError('k must be nonnegative')
    rng, sample = random.Random(seed), []
    for i, item in enumerate(stream):
        if i < k:
            sample.append(item)
        else:
            j = rng.randrange(i + 1)
            if j < k:
                sample[j] = item
    return sample

class BloomFilter:
    def __init__(self, m=16384, k=5):
        if m <= 0 or k <= 0:
            raise ValueError('m and k must be positive')
        self.m, self.k, self.bits = m, k, bytearray((m + 7) // 8)
    def add(self, value):
        for seed in range(self.k):
            i = stable_hash(value, seed) % self.m
            self.bits[i // 8] |= 1 << (i % 8)
    def __contains__(self, value):
        return all(self.bits[(i := stable_hash(value, seed) % self.m) // 8] & (1 << (i % 8))
                   for seed in range(self.k))

class CountMinSketch:
    def __init__(self, width=256, depth=5):
        if width <= 0 or depth <= 0:
            raise ValueError('width and depth must be positive')
        self.width, self.depth = width, depth
        self.table = [array('Q', [0]) * width for _ in range(depth)]
    def add(self, value):
        for seed, row in enumerate(self.table):
            row[stable_hash(value, seed) % self.width] += 1
    def count(self, value):
        return min(row[stable_hash(value, seed) % self.width] for seed, row in enumerate(self.table))

class HyperLogLog:
    def __init__(self, p=10):
        if not 4 <= p <= 16:
            raise ValueError('p must be between 4 and 16')
        self.p, self.m = p, 1 << p
        self.registers = bytearray(self.m)
    def add(self, value):
        h = stable_hash(value)
        index = h >> (64 - self.p)
        tail_bits = 64 - self.p
        tail = h & ((1 << tail_bits) - 1)
        rank = tail_bits - tail.bit_length() + 1
        self.registers[index] = max(self.registers[index], rank)
    def count(self):
        alpha = {16: .673, 32: .697, 64: .709}.get(self.m, .7213 / (1 + 1.079 / self.m))
        estimate = alpha * self.m ** 2 / sum(2.0 ** -r for r in self.registers)
        empty = self.registers.count(0)
        if estimate <= 2.5 * self.m and empty:
            estimate = self.m * math.log(self.m / empty)
        return estimate
