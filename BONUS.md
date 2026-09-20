# Bonus: social tournament scheduling

The original problem does not specify a round count, advancement rule, tie-break,
or what "optimal" measures. This implementation adopts an explicit fixed-round,
points-based tournament. It is not a knockout bracket and does not claim to solve
all possible interpretations of the problem.

## Rules

- Inputs are N participants, T tables, G participants per game and R rounds.
- N must be at least T*G; otherwise filling every table is impossible.
- Every round uses all T tables with exactly G distinct players at each table.
- When N > T*G, surplus participants rest; the scheduler prioritizes balanced
  appearances. No player is eliminated, including those who have not won a game.
- If R*T*G < N, there are not enough seats across the entire tournament for
  everyone to play even once. Such inputs are accepted, but some participants
  necessarily never play. Choose enough rounds if everyone must participate;
  R*T*G >= N is necessary, and the reported minimum game count confirms
  whether the generated schedule actually seats everyone.
- Every table reports exactly one winner. Each victory awards one point. Resting
  awards none. Results are validated for the entire round before standings change.
- After R rounds, most wins determines the champion. Ties use a seeded draw order
  published before play. This guarantees a unique administrative result; it does
  not pretend that a draw proves one equally scoring player is more skilled.
  A playoff would be a different format requiring additional rounds and rules.

## Social objective and optimality

The scheduler compares complete schedules lexicographically, maximizing:

1. The minimum number of games played by any participant.
2. The negative gap between the largest and smallest appearance count.
3. The minimum distinct-opponent count across participants.
4. The total number of distinct unordered pairs that share a table.

With N=T*G the first two measures are constant, so social diversity decides.
The schedule is decided before results: diversity is protected for all players,
including eventual non-winners. It does not optimize retrospectively for a known
set of losers, because that set is not available when planning.

The social part is a variant of the Social Golfer Problem:
https://cgi.cse.unsw.edu.au/~tw/csplib/prob/prob010/spec.html

For small search spaces we enumerate all round partitions, fixing the first round
by player/table symmetry. This is exhaustive for the stated symmetric objective.
For larger spaces we evaluate a bounded number of seeded, greedy randomized
schedules. This is a heuristic, not a general proof of optimality.

Let C=T*G and q=floor(R*C/N). A component-wise upper bound on the score is:

(q, -(R*C mod N != 0), min(N-1, q*(G-1)), min(N*(N-1)/2, R*T*G*(G-1)/2)).

Reaching all these bounds certifies optimality even during heuristic search.
The bound need not be attainable: exhaustive search can also prove an optimum
strictly below it. The CLI reports method, evaluated candidates, score and bound.
It marks optimality only for an exhaustive result or a bound-reaching result.

Exact enumeration grows combinatorially. The budget limits evaluated complete
schedules; it is not a hard wall-clock or memory limit for arbitrary huge inputs.
The heuristic first balances seating opportunities, then discourages repeated
opponents. Equal candidate scores keep the first result encountered.

The objective measures diversity over the whole tournament, not how early
new opponents are met or how evenly repeated meetings are spaced. For example,
N=4, T=2, G=2, R=5 can repeat (1,2)/(3,4) in the first three rounds, then
use (1,3)/(2,4) and (1,4)/(2,3). Everyone still meets all three opponents,
so this is optimal for the stated objective. Avoiding consecutive repeats
would require an additional scheduling criterion. Certification applies
only to the objective above, not to every desirable tournament property.

## Run

```sh
python tournament.py --players 8 --tables 4 --group-size 2 --rounds 3 --simulate
python tournament.py --players 6 --tables 2 --group-size 3 --rounds 2 --budget 2000
python tournament.py --players 10 --tables 2 --group-size 4 --rounds 4 --budget 2000 --simulate
```

Without --simulate only a schedule and the pre-announced tie-break are printed.
--simulate supplies random example game winners; real callers use Tournament.record_round.
The bonus is independent of the data pipeline and uses only the standard library.

Docker:

```sh
docker run --rm --entrypoint python testmds tournament.py --simulate
```
