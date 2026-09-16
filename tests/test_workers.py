import unittest

from file_pipeline import BYTES_PER_MB, FileItem, SimulatedFileProcessor


class WorkerTests(unittest.TestCase):
    def setUp(self):
        print(f"\nTEST: {self._testMethodName}", flush=True)

    def test_processor_returns_total_bytes(self):
        files = (FileItem("a.csv", 3 * BYTES_PER_MB), FileItem("b.csv", 2 * BYTES_PER_MB))
        actual = SimulatedFileProcessor(delay_seconds=0).process(1, files)
        print(f"  Expected: 5 MB processed; actual: {actual / BYTES_PER_MB:.2f} MB", flush=True)
        self.assertEqual(actual, 5 * BYTES_PER_MB)


if __name__ == "__main__":
    unittest.main(verbosity=2)


