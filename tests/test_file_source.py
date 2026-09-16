import random
import unittest
from unittest.mock import Mock

from file_pipeline import BYTES_PER_MB, ExponentialFileSource


class FileSourceTests(unittest.TestCase):
    def setUp(self):
        print(f"\nTEST: {self._testMethodName}", flush=True)

    def test_default_collection_has_100_distinct_names(self):
        files = ExponentialFileSource(rng=random.Random(42)).collect()
        print(f"  Expected: 100 files with distinct names; actual: {len(files)} files", flush=True)
        self.assertEqual(len(files), 100)
        self.assertEqual(len({file.name for file in files}), 100)
        self.assertTrue(all(file.size_bytes >= 0 for file in files))

    def test_known_random_values_become_file_sizes(self):
        rng = Mock(spec=random.Random)
        rng.expovariate.side_effect = [1_000_000.9, 12_000_000.2]
        files = ExponentialFileSource(count=2, mean_size_bytes=3 * BYTES_PER_MB, rng=rng).collect()
        sizes = [file.size_bytes for file in files]
        print(f"  Mock values: 1.00 MB, 12.00 MB; actual bytes: {sizes}", flush=True)
        self.assertEqual(sizes, [1_000_000, 12_000_000])
        self.assertEqual(rng.expovariate.call_count, 2)
        for call in rng.expovariate.call_args_list:
            self.assertEqual(call.args, (1 / (3 * BYTES_PER_MB),))

    def test_same_seed_reproduces_collection(self):
        first = ExponentialFileSource(rng=random.Random(42)).collect()
        second = ExponentialFileSource(rng=random.Random(42)).collect()
        print("  Comparing collections from two new generators with seed 42", flush=True)
        self.assertEqual(first, second)

    def test_empty_collection_does_not_sample(self):
        rng = Mock(spec=random.Random)
        files = ExponentialFileSource(count=0, rng=rng).collect()
        print(f"  Expected: empty collection, no random calls; actual: {files}", flush=True)
        self.assertEqual(files, [])
        rng.expovariate.assert_not_called()

    def test_invalid_configuration_is_rejected(self):
        for mean in (0, -1, float("inf"), float("nan")):
            with self.subTest(mean=mean):
                print(f"  Mean {mean}: expecting ValueError", flush=True)
                with self.assertRaises(ValueError):
                    ExponentialFileSource(mean_size_bytes=mean)
        print("  Count -1: expecting ValueError", flush=True)
        with self.assertRaises(ValueError):
            ExponentialFileSource(count=-1)


if __name__ == "__main__":
    unittest.main(verbosity=2)

