# Ravand — Social Feed Design on Apache Cassandra

A hands-on distributed-systems project that designs the home feed of a social network ("Ravand") on a 3-node Apache Cassandra cluster. It starts from a naive read-time feed and evolves it, measuring each trade-off, into a hybrid fan-out feed with personalized ranking and click-through-rate analytics stored in Cassandra.

Built as part of a data-engineering workshop on distributed systems.

## What it demonstrates

- **Query-first data modeling** in Cassandra: partition and clustering keys, day buckets to bound partition growth, deterministic ordering.
- **Fan-out on Read vs Fan-out on Write**, and the cost of each (read amplification vs write amplification), measured.
- **Hybrid fan-out** for high-follower ("celebrity") authors, with merge, de-duplication and deterministic tie-breaking.
- **Idempotent writes** (primary-key upserts) so replayed events never create duplicates.
- **Candidate generation → ranking** with a deterministic scorer, and tests proving ineligible posts can never leak into the feed.
- **Impression/click modeling** in Cassandra where duplicate events cannot inflate CTR.
- **Graceful degradation**: if one feed source fails, a partial feed is returned with an `is_partial` flag.
- Consistency reasoning: reads and writes at `LOCAL_QUORUM` with `RF=3`.

## Stages

| Stage | Files | Topic |
| --- | --- | --- |
| 1 | `q1.sql`, `q1.py`, `q1-postgres.sql` | Fan-out on Read baseline (plus a PostgreSQL reference model) |
| 2 | `q2.py` | Read cost as followed authors grow; bounded concurrency |
| 3 | `q3.sql`, `q3.py` | Fan-out on Write with a precomputed `user_feed` table |
| 4 | `q4.py` | Celebrity burst: write amplification and head-of-line blocking |
| 5–6 | `q5_q6.py` | Hybrid feed, personalization, impression/click tracking and CTR report |

## Selected results (stages 5–6, real 3-node cluster)

Publishing one post from an author with 10,000 followers:

| Approach | Logical writes | Fan-out time | DB reads to open a feed |
| --- | --- | --- | --- |
| Pure Fan-out on Write | 10,001 | ~4.27 s | 3 |
| Hybrid | 1 | none | 7 |

The cost moves from publish time to read time. Timings come from a single laptop running three Docker containers, so they are indicative only.

CTR is computed as distinct impressions with at least one click divided by distinct impressions. In the test run, duplicate impressions and repeated clicks did not change it (3 of 16 impressions clicked = 0.1875, while counting raw click rows would wrongly give 0.25).

## Run it

Requirements: Docker with the Compose plugin, Python 3, and the `cassandra-driver` package.

```bash
# 1. Start the 3-node cluster and wait until all nodes are healthy
docker compose up -d
docker compose ps

# 2. Create the keyspace and tables
docker exec -i cassandra-node1 cqlsh < q1.sql
docker exec -i cassandra-node1 cqlsh < q3.sql

# 3. Install the driver (a virtual environment is recommended)
python -m pip install cassandra-driver

# 4. Run stages 5-6 (prints results and runs all assertions)
python q5_q6.py
```

A successful run ends with `ALL CHECKS PASSED`. Note that `q5_q6.py` truncates the tables in the `ravand` keyspace at the start so it is repeatable. Each stage script re-seeds its own data.

## Notes

- `q1-postgres.sql` is PostgreSQL syntax and is not meant to be run on Cassandra.
- The "celebrity" set is a fixed configuration value; the numeric threshold for what counts as a celebrity is deliberately not hard-coded, since it should be derived from measured delivery capacity (follower count, posting rate and reader activity).
