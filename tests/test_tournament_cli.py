"""Exercise the public bonus command using the current Python interpreter."""

import subprocess
import sys
import unittest
from pathlib import Path


class TournamentCliTests(unittest.TestCase):
    def setUp(self):
        print(f"\nTEST: {self._testMethodName}", flush=True)

    def run_cli(self, *arguments):
        project = Path(__file__).resolve().parents[1]
        return subprocess.run(
            [sys.executable, "-B", str(project / "tournament.py"), *arguments],
            cwd=project,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

    def test_schedule_only_prints_rounds_without_game_results(self):
        result = self.run_cli("--players", "4", "--tables", "2",
                              "--group-size", "2", "--rounds", "3")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        self.assertEqual(sum(line.startswith("Round ") for line in result.stdout.splitlines()), 3)
        self.assertIn("optimality certified: True", result.stdout)
        self.assertIn("Pre-announced tie-break draw order:", result.stdout)
        self.assertNotIn("Simulated winners:", result.stdout)
        self.assertNotIn("Champion:", result.stdout)
        print("  Actual: CLI prints three rounds and tie-break; no results without --simulate", flush=True)

    def test_simulation_prints_results_and_one_champion(self):
        result = self.run_cli("--players", "4", "--tables", "2",
                              "--group-size", "2", "--rounds", "3", "--simulate")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        self.assertEqual(result.stdout.count("Simulated winners:"), 3)
        self.assertIn("Standings (player, wins):", result.stdout)
        self.assertEqual(result.stdout.count("Champion:"), 1)
        self.assertRegex(result.stdout, r"Champion: [1-4] \(wins, then pre-announced draw order\)")
        print("  Actual: --simulate prints all three round results, standings and one champion", flush=True)

    def test_invalid_arguments_exit_with_usage_error_without_traceback(self):
        cases = (
            (("--group-size", "1"), "Each game needs at least two players"),
            (("--players", "3", "--tables", "2", "--group-size", "2"), "N must be at least T * G"),
            (("--rounds", "0"), "N, T, G and rounds must be positive integers"),
            (("--budget", "0"), "Search budget must be a positive integer"),
            (("--players", "abc"), "invalid int value"),
            (("--unknown-option",), "unrecognized arguments"),
        )
        for arguments, message in cases:
            with self.subTest(arguments=arguments):
                result = self.run_cli(*arguments)
                self.assertEqual(result.returncode, 2)
                self.assertIn("usage:", result.stderr)
                self.assertIn(message, result.stderr)
                self.assertNotIn("Traceback", result.stderr)
                self.assertEqual(result.stdout, "")
        print("  Actual: six invalid inputs exit with code 2 and a clear error before printing a schedule", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
