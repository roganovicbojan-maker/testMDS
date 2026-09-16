import unittest

from file_pipeline import BYTES_PER_MB, FileItem, NextFitBucketing


def describe_files(files):
    return ", ".join(
        f"{file.name}: {file.size_bytes} bytes ({file.size_bytes / BYTES_PER_MB:.2f} MB)"
        for file in files
    ) or "(empty)"


def describe_buckets(buckets):
    return " | ".join(
        f"[{describe_files(bucket)}]" for bucket in buckets
    ) or "(no buckets)"


class NextFitBucketingTests(unittest.TestCase):
    def setUp(self):
        print(f"\nTEST: {self._testMethodName}", flush=True)

    def check_packing(self, files, expected, limit=10 * BYTES_PER_MB):
        print(f"  Input: {describe_files(files)}", flush=True)
        print(f"  Limit: {limit} bytes ({limit / BYTES_PER_MB:.2f} MB)", flush=True)
        actual = NextFitBucketing(max_size_bytes=limit).pack(files)
        print(f"  Expected: {describe_buckets(expected)}", flush=True)
        print(f"  Actual:   {describe_buckets(actual)}", flush=True)
        self.assertEqual(actual, expected)
        print("  Check passed: bucket contents and order match.", flush=True)

    def test_exact_limit_partial_and_oversized_file(self):
        files = [
            FileItem("a.csv", 3 * BYTES_PER_MB),
            FileItem("b.csv", 5 * BYTES_PER_MB),
            FileItem("c.csv", 2 * BYTES_PER_MB),
            FileItem("d.csv", 4 * BYTES_PER_MB),
            FileItem("e.csv", 20 * BYTES_PER_MB),
        ]
        self.check_packing(files, [files[:3], [files[3]], [files[4]]])

    def test_empty_input_creates_no_buckets(self):
        self.check_packing([], [])

    def test_file_after_oversized_file_goes_into_new_bucket(self):
        large = FileItem("large.csv", 20 * BYTES_PER_MB)
        small = FileItem("small.csv", BYTES_PER_MB)
        self.check_packing([large, small], [[large], [small]])

    def test_custom_limit_is_respected(self):
        files = [FileItem("a.csv", 3 * BYTES_PER_MB), FileItem("b.csv", 3 * BYTES_PER_MB)]
        self.check_packing(files, [[files[0]], [files[1]]], limit=5 * BYTES_PER_MB)

    def test_zero_size_file_is_preserved(self):
        empty_file = FileItem("empty.csv", 0)
        self.check_packing([empty_file], [[empty_file]])

    def test_invalid_sizes_are_rejected(self):
        print("  Input: file size -1 byte; expected: ValueError", flush=True)
        with self.assertRaises(ValueError) as caught:
            FileItem("invalid.csv", -1)
        print(f"  Actual: ValueError: {caught.exception}", flush=True)

        for limit in (0, -1):
            with self.subTest(limit=limit):
                print(f"  Input: bucket limit {limit}; expected: ValueError", flush=True)
                with self.assertRaises(ValueError) as caught:
                    NextFitBucketing(max_size_bytes=limit)
                print(f"  Actual: ValueError: {caught.exception}", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)

