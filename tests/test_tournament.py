import unittest

from tournament import SocialScheduler, Tournament, social_score


class TournamentTests(unittest.TestCase):
    def test_full_tables_and_each_player_once_per_round(self):
        plan = SocialScheduler(search_budget=20).build(12, 3, 4, 3)
        for round_ in plan.rounds:
            self.assertEqual(len(round_.tables), 3)
            self.assertTrue(all(len(table) == 4 for table in round_.tables))
            self.assertEqual(sorted(p for table in round_.tables for p in table), list(range(1, 13)))

    def test_small_round_robin_reaches_proven_bound(self):
        plan = SocialScheduler().build(4, 2, 2, 3)
        self.assertEqual(plan.score, (3, 0, 3, 6))
        self.assertEqual(plan.score, plan.upper_bound)
        self.assertTrue(plan.optimal)

    def test_exhaustive_search_can_prove_unattainable_bound(self):
        # Two groups of three must repeat opponents in round 2 (pigeonhole principle).
        plan = SocialScheduler().build(6, 2, 3, 2)
        self.assertTrue(plan.optimal)
        self.assertEqual(plan.method, "exhaustive search")
        self.assertLess(plan.score, plan.upper_bound)
        self.assertEqual(plan.evaluated, 10)

    def test_extra_players_rest_with_balanced_opportunities(self):
        plan = SocialScheduler().build(5, 2, 2, 2)
        self.assertEqual(plan.score[:2], (1, -1))
        for round_ in plan.rounds:
            seated = [p for table in round_.tables for p in table]
            self.assertEqual(len(round_.resting), 1)
            self.assertEqual(sorted(seated + list(round_.resting)), [1, 2, 3, 4, 5])

    def test_insufficient_players_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "N must be at least"):
            SocialScheduler().build(7, 2, 4, 3)

    def test_invalid_configuration_is_rejected(self):
        for values in ((0, 1, 2, 1), (4, 0, 2, 1), (4, 2, 1, 1), (4, 2, 2, 0), (4.0, 2, 2, 1)):
            with self.subTest(values=values), self.assertRaises(ValueError):
                SocialScheduler().build(*values)
        with self.assertRaises(ValueError):
            SocialScheduler(search_budget=0)

    def test_bounded_search_does_not_claim_unproven_optimality(self):
        plan = SocialScheduler(search_budget=2).build(12, 3, 4, 3)
        self.assertFalse(plan.optimal)
        self.assertEqual(plan.method, "bounded heuristic")
        self.assertEqual(plan.evaluated, 2)
        self.assertEqual(plan.score, social_score(plan.rounds, 12))

    def test_seed_reproduces_heuristic_schedule(self):
        first = SocialScheduler(search_budget=10, seed=17).build(12, 3, 4, 3)
        second = SocialScheduler(search_budget=10, seed=17).build(12, 3, 4, 3)
        self.assertEqual(first, second)

    def test_invalid_results_do_not_partially_change_standings(self):
        game = Tournament(SocialScheduler().build(4, 2, 2, 2))
        with self.assertRaises(ValueError):
            game.record_round((1, 1))
        self.assertEqual(sum(game.wins.values()), 0)
        self.assertEqual(game.played_rounds, 0)

    def test_winners_and_unique_champion_after_all_rounds(self):
        game = Tournament(SocialScheduler().build(4, 2, 2, 3))
        with self.assertRaises(ValueError):
            _ = game.champion
        for round_ in game.schedule.rounds:
            game.record_round(tuple(min(table) for table in round_.tables))
        self.assertEqual(game.champion, 1)
        self.assertEqual(sum(game.wins.values()), 6)
        with self.assertRaises(ValueError):
            game.record_round((1, 2))

    def test_tie_is_resolved_by_preannounced_draw(self):
        game = Tournament(SocialScheduler().build(4, 2, 2, 1), tie_seed=7)
        winners = tuple(table[0] for table in game.schedule.rounds[0].tables)
        game.record_round(winners)
        expected = next(p for p in game.tie_order if p in winners)
        self.assertEqual(game.champion, expected)


if __name__ == "__main__":
    unittest.main(verbosity=2)
