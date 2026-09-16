import unittest

from message_batching import Message, TimeBatcher


class TimeBatcherTests(unittest.TestCase):
    def setUp(self):
        print(f"\nTEST: {self._testMethodName}", flush=True)

    def test_first_message_sets_deadline_and_later_messages_do_not_extend_it(self):
        batcher = TimeBatcher()
        first, second = Message(1, "a"), Message(2, "b")
        self.assertIsNone(batcher.add(first, 20))
        self.assertIsNone(batcher.add(second, 150))
        self.assertEqual(batcher.deadline, 320)
        self.assertIsNone(batcher.close_if_due(319.9))
        batch = batcher.close_if_due(320)
        print(f"  Arrivals: 20s, 150s; expected deadline: 320s; actual: {batch.deadline}", flush=True)
        self.assertEqual(batch.messages, (first, second))
        self.assertEqual(batch.opened_at, 20)

    def test_timer_closes_batch_without_another_message(self):
        batcher = TimeBatcher()
        message = Message(1, "only message")
        batcher.add(message, 0)
        batch = batcher.close_if_due(300)
        print("  One arrival at 0s; check at 300s closes it without another arrival", flush=True)
        self.assertEqual(batch.messages, (message,))
        self.assertIsNone(batcher.deadline)
        self.assertIsNone(batcher.close_if_due(600))

    def test_boundary_message_opens_next_batch(self):
        batcher = TimeBatcher()
        first, boundary = Message(1, "a"), Message(2, "boundary")
        batcher.add(first, 0)
        closed = batcher.add(boundary, 300)
        print("  Arrival at exactly 300s: old batch has message 1; new batch has message 2", flush=True)
        self.assertEqual(closed.messages, (first,))
        self.assertEqual(batcher.deadline, 600)
        self.assertEqual(batcher.flush().messages, (boundary,))

    def test_idle_gap_does_not_create_empty_windows(self):
        batcher = TimeBatcher()
        self.assertIsNone(batcher.close_if_due(100))
        batcher.add(Message(1, "a"), 200)
        batcher.close_if_due(500)
        self.assertIsNone(batcher.close_if_due(1000))
        batcher.add(Message(2, "b"), 1200)
        print(f"  Arrival after idle gap: 1200s; expected deadline: 1500s; actual: {batcher.deadline}", flush=True)
        self.assertEqual(batcher.deadline, 1500)

    def test_shutdown_flushes_once_and_preserves_closed_batch(self):
        batcher = TimeBatcher()
        first = Message(1, "a")
        batcher.add(first, 0)
        closed = batcher.flush()
        self.assertIsNone(batcher.flush())
        batcher.add(Message(2, "b"), 10)
        print("  Early flush returns message 1 once; later arrivals do not change it", flush=True)
        self.assertEqual(closed.messages, (first,))
        self.assertEqual(closed.deadline, 300)

    def test_invalid_window_and_backwards_time_are_rejected(self):
        for window in (0, -1, float("nan"), float("inf")):
            with self.subTest(window=window):
                with self.assertRaises(ValueError):
                    TimeBatcher(window)
        batcher = TimeBatcher()
        batcher.add(Message(1, "a"), 20)
        with self.assertRaises(ValueError):
            batcher.add(Message(2, "b"), 19)
        self.assertEqual(batcher.flush().messages, (Message(1, "a"),))
        print("  Invalid windows and backwards time rejected without adding message 2", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
