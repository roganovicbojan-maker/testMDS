import random
import unittest
from collections import Counter
from unittest.mock import Mock, patch

import app
from file_pipeline import BYTES_PER_MB, FileItem, FileBatchService, NextFitBucketing, FirstFitDecreasingBucketing


class StrategyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        print(f"\nTEST: {self._testMethodName}", flush=True)

    async def test_real_strategies_can_be_swapped_without_changing_service(self):
        files = [FileItem(str(i), size * BYTES_PER_MB) for i, size in enumerate((6, 6, 4, 4))]
        source = Mock()
        source.collect.return_value = files
        next_fit = FileBatchService(source, NextFitBucketing()).prepare_buckets()
        ffd = FileBatchService(source, FirstFitDecreasingBucketing()).prepare_buckets()
        self.assertEqual(len(next_fit), 3)
        self.assertEqual(ffd, [[files[0], files[2]], [files[1], files[3]]])
        print("  Input: 6, 6, 4, 4 MB; actual: Next Fit 3 buckets, FFD 2 buckets", flush=True)

    async def test_ffd_sorts_copy_and_preserves_original_input(self):
        files = [FileItem("small", 4), FileItem("large", 6), FileItem("other", 4)]
        original = files.copy()
        buckets = FirstFitDecreasingBucketing(10).pack(files)
        self.assertEqual(files, original)
        self.assertEqual(buckets, [[files[1], files[0]], [files[2]]])
        print("  Actual: FFD starts with largest file; caller's input remains unchanged", flush=True)

    async def test_empty_exact_limit_oversized_and_zero_size(self):
        strategy = FirstFitDecreasingBucketing(10)
        self.assertEqual(strategy.pack([]), [])
        big, exact, zero = FileItem("big", 20), FileItem("exact", 10), FileItem("zero", 0)
        self.assertEqual(strategy.pack([zero, big, exact]), [[big], [exact, zero]])
        print("  Actual: oversized file alone, exact-limit bucket accepts zero bytes, empty input stays empty", flush=True)

    async def test_seeded_invariants_for_both_strategies(self):
        for seed in range(30):
            rng = random.Random(seed)
            limit = rng.randint(1, 100)
            # Identical metadata is allowed; identity checks detect lost/replaced objects.
            files = [FileItem("same-name", rng.randint(0, 3 * limit)) for _ in range(100)]
            for strategy_type in (NextFitBucketing, FirstFitDecreasingBucketing):
                with self.subTest(seed=seed, strategy=strategy_type.__name__):
                    buckets = strategy_type(limit).pack(files)
                    self.assertEqual(Counter(map(id, files)), Counter(id(f) for b in buckets for f in b))
                    for bucket in buckets:
                        self.assertTrue(bucket)
                        total = sum(f.size_bytes for f in bucket)
                        if total > limit:
                            self.assertEqual(len(bucket), 1)
                            self.assertGreater(bucket[0].size_bytes, limit)
        print("  Actual: 30 seeded collections per strategy; all files exactly once and all bucket limits respected", flush=True)

    async def test_app_selects_requested_strategy(self):
        for name, expected in (("next-fit", NextFitBucketing), ("ffd", FirstFitDecreasingBucketing)):
            with self.subTest(name=name), patch.object(app, "FileBatchService", wraps=FileBatchService) as service:
                await app.run(10, 300, count=0, bucketing=name)
                self.assertIsInstance(service.call_args.args[1], expected)
        with self.assertRaisesRegex(ValueError, "Unknown bucketing"):
            await app.run(10, 300, count=0, bucketing="unknown")
        print("  Actual: app selects both strategies correctly and rejects an unknown name", flush=True)

    async def test_ffd_rejects_nonpositive_limit(self):
        for limit in (0, -1):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                FirstFitDecreasingBucketing(limit)
        print("  Actual: zero and negative limits rejected", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
