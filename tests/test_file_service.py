import unittest
from unittest.mock import Mock

from file_pipeline import (
    BYTES_PER_MB,
    BucketingStrategy,
    FileBatchService,
    FileItem,
    FileSource,
    NextFitBucketing,
)
from tests.test_bucketing import describe_buckets


class FileBatchServiceTests(unittest.TestCase):
    def setUp(self):
        print(f"\nTEST: {self._testMethodName}", flush=True)

    def test_mock_source_with_real_strategy(self):
        files = [FileItem("a.csv", 6 * BYTES_PER_MB), FileItem("b.csv", 5 * BYTES_PER_MB)]
        source = Mock(spec=FileSource)
        source.collect.return_value = files
        service = FileBatchService(source, NextFitBucketing())

        actual = service.prepare_buckets()
        expected = [[files[0]], [files[1]]]

        print("  Mock source provides 6 MB and 5 MB; real strategy uses 10 MB limit", flush=True)
        print(f"  Expected: {describe_buckets(expected)}", flush=True)
        print(f"  Actual:   {describe_buckets(actual)}", flush=True)
        self.assertEqual(actual, expected)
        source.collect.assert_called_once_with()

    def test_strategy_can_be_replaced(self):
        files = [FileItem("a.csv", BYTES_PER_MB), FileItem("b.csv", BYTES_PER_MB)]
        source = Mock(spec=FileSource)
        source.collect.return_value = files
        replacement = Mock(spec=BucketingStrategy)
        replacement.pack.return_value = [[files[0]], [files[1]]]

        actual = FileBatchService(source, replacement).prepare_buckets()

        print("  Replacement strategy returns each file in a separate bucket", flush=True)
        print(f"  Expected: {describe_buckets([[files[0]], [files[1]]])}", flush=True)
        print(f"  Actual:   {describe_buckets(actual)}", flush=True)
        self.assertEqual(actual, [[files[0]], [files[1]]])
        replacement.pack.assert_called_once_with(files)

    def test_failed_source_does_not_call_strategy(self):
        source = Mock(spec=FileSource)
        source.collect.side_effect = OSError("Source unavailable")
        strategy = Mock(spec=BucketingStrategy)
        service = FileBatchService(source, strategy)

        print("  Source fails; expected: visible OSError and no grouping", flush=True)
        with self.assertRaisesRegex(OSError, "Source unavailable") as caught:
            service.prepare_buckets()
        strategy.pack.assert_not_called()
        print(f"  Actual: {caught.exception}; strategy was not called", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)


