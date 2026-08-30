-- Tags: no-fasttest
-- mergeOrderbook: insert/change/cancel depth ops + row-level snapshot; ephemeral zero drop.

DROP TABLE IF EXISTS mob_src;
CREATE TABLE mob_src
(
    prices Array(Decimal128(19)),
    sizes Array(Tuple(Bool, Decimal128(19), Decimal128(19))),
    ops Array(Enum8('insert' = 0, 'change' = 1, 'cancel' = 2)),
    ts DateTime64(6, 'UTC'),
    kind Enum8('delta' = 0, 'snapshot' = 1)
) ENGINE = Memory;

-- A: insert → cancel → insert → cancel  ⇒ ephemeral, drop
-- A2: insert → change → cancel ⇒ ephemeral (change must not force eb), drop
-- B: cancel → insert → change → cancel ⇒ leading cancel then rebirth then cancel:
--     after insert eb cleared; final cancel drops (new life closed)
-- E: change → cancel ⇒ keep tombstone (first sighting is change)
INSERT INTO mob_src VALUES
    ([1], [(true, 10, 10)], ['insert'], toDateTime64(100, 6, 'UTC'), 'delta'),
    ([1], [(true, 0, 0)], ['cancel'], toDateTime64(110, 6, 'UTC'), 'delta'),
    ([1], [(true, 5, 5)], ['insert'], toDateTime64(120, 6, 'UTC'), 'delta'),
    ([1], [(true, 0, 0)], ['cancel'], toDateTime64(130, 6, 'UTC'), 'delta'),
    ([4], [(true, 1, 1)], ['insert'], toDateTime64(140, 6, 'UTC'), 'delta'),
    ([4], [(false, 2, 2)], ['change'], toDateTime64(150, 6, 'UTC'), 'delta'),
    ([4], [(true, 0, 0)], ['cancel'], toDateTime64(160, 6, 'UTC'), 'delta'),
    ([2], [(true, 0, 0)], ['cancel'], toDateTime64(200, 6, 'UTC'), 'delta'),
    ([2], [(true, 7, 7)], ['insert'], toDateTime64(210, 6, 'UTC'), 'delta'),
    ([2], [(false, 8, 8)], ['change'], toDateTime64(220, 6, 'UTC'), 'delta'),
    ([2], [(true, 0, 0)], ['cancel'], toDateTime64(230, 6, 'UTC'), 'delta'),
    ([3], [(false, 9, 9)], ['change'], toDateTime64(300, 6, 'UTC'), 'delta'),
    ([3], [(true, 0, 0)], ['cancel'], toDateTime64(310, 6, 'UTC'), 'delta');

SELECT
    arraySort(arrayMap(x -> tupleElement(x, 1), tupleElement(r, 2))) AS prices,
    arraySort(arrayMap(x -> (tupleElement(x, 1), tupleElement(tupleElement(x, 3), 2)), tupleElement(r, 2))) AS price_base
FROM (SELECT mergeOrderbook(prices, sizes, ops, ts, kind) AS r FROM mob_src);

-- Snapshot = clean slate: cancel of snap level drops (no tombstone); insert→cancel drops;
-- only remaining live snap levels stay.
DROP TABLE IF EXISTS mob_snap;
CREATE TABLE mob_snap
(
    prices Array(Decimal128(19)),
    sizes Array(Tuple(Bool, Decimal128(19), Decimal128(19))),
    ops Array(Enum8('insert' = 0, 'change' = 1, 'cancel' = 2)),
    ts DateTime64(6, 'UTC'),
    kind Enum8('delta' = 0, 'snapshot' = 1)
) ENGINE = Memory;

INSERT INTO mob_snap VALUES
    ([10, 20], [(true, 1, 1), (false, 2, 2)], ['insert', 'insert'], toDateTime64(500, 6, 'UTC'), 'snapshot'),
    ([10], [(true, 0, 0)], ['cancel'], toDateTime64(600, 6, 'UTC'), 'delta'),
    ([20], [(false, 5, 5)], ['change'], toDateTime64(605, 6, 'UTC'), 'delta'),
    ([20], [(true, 0, 0)], ['cancel'], toDateTime64(608, 6, 'UTC'), 'delta'),
    ([30], [(true, 3, 3)], ['insert'], toDateTime64(610, 6, 'UTC'), 'delta'),
    ([30], [(true, 0, 0)], ['cancel'], toDateTime64(620, 6, 'UTC'), 'delta');

SELECT
    tupleElement(r, 1) AS snapshot_ts,
    arraySort(arrayMap(x -> (tupleElement(x, 1), tupleElement(tupleElement(x, 3), 2)), tupleElement(r, 2))) AS price_base
FROM (SELECT mergeOrderbook(prices, sizes, ops, ts, kind) AS r FROM mob_snap);

DROP TABLE mob_src;
DROP TABLE mob_snap;
