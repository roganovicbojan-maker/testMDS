"""Fixed-round tournament with full tables, social scheduling and ranked results."""

import argparse
import itertools
import math
import random
from dataclasses import dataclass


@dataclass(frozen=True)
class Round:
    tables: tuple[tuple[int, ...], ...]
    resting: tuple[int, ...] = ()


@dataclass(frozen=True)
class Schedule:
    players: int
    table_count: int
    group_size: int
    rounds: tuple[Round, ...]
    score: tuple[int, int, int, int]
    upper_bound: tuple[int, int, int, int]
    optimal: bool
    method: str
    evaluated: int


def social_score(rounds: tuple[Round, ...], players: int) -> tuple[int, int, int, int]:
    appearances = [0] * players
    opponents = [set() for _ in range(players)]
    for round_ in rounds:
        for table in round_.tables:
            for player in table:
                appearances[player - 1] += 1
                opponents[player - 1].update(set(table) - {player})
    return (
        min(appearances),
        -(max(appearances) - min(appearances)),
        min(map(len, opponents)),
        sum(map(len, opponents)) // 2,
    )


def partitions(players: tuple[int, ...], group_size: int):
    """Unlabelled tables: anchor the smallest remaining player to remove symmetry."""
    if not players:
        yield ()
        return
    for others in itertools.combinations(players[1:], group_size - 1):
        table = (players[0],) + others
        remaining = tuple(p for p in players if p not in table)
        for tail in partitions(remaining, group_size):
            yield (table,) + tail


class SocialScheduler:
    def __init__(self, search_budget: int = 2000, seed: int = 42):
        if type(search_budget) is not int or search_budget < 1:
            raise ValueError("Search budget must be a positive integer")
        self.search_budget = search_budget
        self.seed = seed

    def build(self, players: int, tables: int, group_size: int, rounds: int) -> Schedule:
        for value in (players, tables, group_size, rounds):
            if type(value) is not int or value < 1:
                raise ValueError("N, T, G and rounds must be positive integers")
        if group_size < 2:
            raise ValueError("Each game needs at least two players")
        capacity = tables * group_size
        if players < capacity:
            raise ValueError("N must be at least T * G to fill every table")
        ids = tuple(range(1, players + 1))
        first = Round(tuple(tuple(ids[i:i + group_size]) for i in range(0, capacity, group_size)), ids[capacity:])
        appearances, remainder = divmod(rounds * capacity, players)
        upper = (
            appearances, -int(bool(remainder)),
            min(players - 1, appearances * (group_size - 1)),
            min(math.comb(players, 2), rounds * tables * math.comb(group_size, 2)),
        )
        # Count unlabelled full-table partitions with one unlabelled resting group.
        choices = math.factorial(players) // (
            math.factorial(players - capacity) * math.factorial(group_size) ** tables * math.factorial(tables)
        )
        # Stop arithmetic early: we only need to know whether exhaustive search fits.
        count = 1
        for _ in range(rounds - 1):
            count *= choices
            if count > self.search_budget:
                break
        exact = count <= self.search_budget
        rng = random.Random(self.seed)
        best, best_score, evaluated = None, None, 0
        if exact:
            candidates = []
            if rounds > 1:
                for seated in itertools.combinations(ids, capacity):
                    resting = tuple(p for p in ids if p not in seated)
                    candidates.extend(Round(groups, resting) for groups in partitions(seated, group_size))
            schedules = ((first,) + tail for tail in itertools.product(candidates, repeat=rounds - 1))
        else:
            schedules = (self._candidate(ids, tables, group_size, rounds, first, rng) for _ in range(self.search_budget))
        hit_bound = False
        for candidate in schedules:
            evaluated += 1
            score = social_score(candidate, players)
            if best_score is None or score > best_score:
                best, best_score = candidate, score
            if score == upper:
                hit_bound = True
                break
        assert best is not None and best_score is not None
        method = "upper bound reached" if hit_bound else "exhaustive search" if exact else "bounded heuristic"
        return Schedule(players, tables, group_size, best, best_score, upper, exact or hit_bound, method, evaluated)

    def _candidate(self, ids, tables, group_size, rounds, first, rng):
        result = [first]
        appearances = {p: 0 for p in ids}
        met = {p: set() for p in ids}
        for index in range(rounds):
            if index:
                # Least-played participants get seats first; random ties diversify search.
                available = list(ids)
                rng.shuffle(available)
                available.sort(key=appearances.get)
                seated, resting = available[:tables * group_size], available[tables * group_size:]
                groups = []
                for _ in range(tables):
                    table = [seated.pop()]
                    while len(table) < group_size:
                        repeats = [sum(other in met[p] for other in table) for p in seated]
                        minimum = min(repeats)
                        choices = [i for i, cost in enumerate(repeats) if cost == minimum]
                        table.append(seated.pop(rng.choice(choices)))
                    groups.append(tuple(sorted(table)))
                result.append(Round(tuple(sorted(groups)), tuple(sorted(resting))))
            for table in result[-1].tables:
                for p in table:
                    appearances[p] += 1
                    met[p].update(set(table) - {p})
        return tuple(result)


class Tournament:
    """One point per win; equal scores use a pre-announced seeded draw order."""

    def __init__(self, schedule: Schedule, tie_seed: int = 42):
        self.schedule = schedule
        self.wins = {p: 0 for p in range(1, schedule.players + 1)}
        self.played_rounds = 0
        order = list(self.wins)
        random.Random(tie_seed).shuffle(order)
        self.tie_order = tuple(order)
        self._tie_rank = {p: i for i, p in enumerate(order)}

    def record_round(self, winners: tuple[int, ...]) -> None:
        if self.played_rounds >= len(self.schedule.rounds):
            raise ValueError("Tournament is already complete")
        tables = self.schedule.rounds[self.played_rounds].tables
        if len(winners) != len(tables) or any(w not in table for w, table in zip(winners, tables)):
            raise ValueError("Provide exactly one seated winner for each table, in table order")
        # Validate every result before mutating standings.
        for winner in winners:
            self.wins[winner] += 1
        self.played_rounds += 1

    def standings(self) -> tuple[int, ...]:
        return tuple(sorted(self.wins, key=lambda p: (-self.wins[p], self._tie_rank[p])))

    @property
    def champion(self) -> int:
        if self.played_rounds != len(self.schedule.rounds):
            raise ValueError("Champion is only available after all rounds")
        return self.standings()[0]


def main():
    parser = argparse.ArgumentParser(description="Bonus: full-table social tournament scheduler")
    parser.add_argument("--players", type=int, default=8)
    parser.add_argument("--tables", type=int, default=4)
    parser.add_argument("--group-size", type=int, default=2)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--budget", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--simulate", action="store_true", help="Simulate game results; otherwise print only the schedule")
    args = parser.parse_args()
    try:
        schedule = SocialScheduler(args.budget, args.seed).build(args.players, args.tables, args.group_size, args.rounds)
    except ValueError as error:
        parser.error(str(error))
    print(f"Method: {schedule.method}; optimality certified: {schedule.optimal}; evaluated: {schedule.evaluated}")
    print(f"Score (min games, -game spread, min distinct opponents, distinct pairs): {schedule.score}")
    print(f"Upper bound: {schedule.upper_bound}")
    tournament = Tournament(schedule, args.seed)
    print(f"Pre-announced tie-break draw order: {tournament.tie_order}")
    rng = random.Random(args.seed + 1)
    for number, round_ in enumerate(schedule.rounds, 1):
        print(f"Round {number}: tables={round_.tables}; resting={round_.resting}")
        if args.simulate:
            winners = tuple(rng.choice(table) for table in round_.tables)
            tournament.record_round(winners)
            print(f"  Simulated winners: {winners}")
    if args.simulate:
        print(f"Standings (player, wins): {[(p, tournament.wins[p]) for p in tournament.standings()]}")
        print(f"Champion: {tournament.champion} (wins, then pre-announced draw order)")


if __name__ == "__main__":
    main()
