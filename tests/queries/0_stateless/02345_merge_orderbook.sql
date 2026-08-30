-- Tags: no-fasttest
-- Smoke test for mergeOrderbook: LWW, snapshot prune, zero drop, sorted merge.

DROP TABLE IF EXISTS mob_src;
CREATE TABLE mob_src
(
    prices Array(Decimal128(19)),
    sizes Array(Tuple(Bool, Decimal128(19), Decimal128(19))),
    ts DateTime64(6, 'UTC'),
    kind Enum8('delta' = 0, 'snapshot' = 1)
) ENGINE = Memory;

-- snapshot at t=100: prices 1,2 live; then delta zeros out 1 and updates 2; then delta adds 3
INSERT INTO mob_src VALUES
    ([1, 2], [(true, 10, 10), (false, 20, 20)], toDateTime64(100, 6, 'UTC'), 'snapshot'),
    ([1, 2], [(true, 0, 0), (false, 25, 25)], toDateTime64(200, 6, 'UTC'), 'delta'),
    ([3], [(true, 30, 30)], toDateTime64(300, 6, 'UTC'), 'delta');

-- Finalize: expect snapshot_ts=100, levels at 2 and 3 only (1 dropped as zero)
SELECT
    tupleElement(r, 1) AS snapshot_ts,
    arrayMap(x -> (tupleElement(x, 1), tupleElement(tupleElement(x, 3), 2)), tupleElement(r, 2)) AS price_base
FROM
(
    SELECT mergeOrderbook(prices, sizes, ts, kind) AS r
    FROM mob_src
);

-- State round-trip via -State / -Merge
SELECT
    tupleElement(r, 1) AS snapshot_ts,
    arrayMap(x -> (tupleElement(x, 1), tupleElement(tupleElement(x, 3), 2)), tupleElement(r, 2)) AS price_base
FROM
(
    SELECT mergeOrderbookMerge(s) AS r
    FROM
    (
        SELECT mergeOrderbookState(prices, sizes, ts, kind) AS s
        FROM mob_src
    )
);

-- Newer snapshot must prune older levels (price 3 from before snap is gone if only in prior state)
DROP TABLE IF EXISTS mob_snap;
CREATE TABLE mob_snap
(
    prices Array(Decimal128(19)),
    sizes Array(Tuple(Bool, Decimal128(19), Decimal128(19))),
    ts DateTime64(6, 'UTC'),
    kind Enum8('delta' = 0, 'snapshot' = 1)
) ENGINE = Memory;

INSERT INTO mob_snap VALUES
    ([1, 3], [(true, 10, 10), (true, 30, 30)], toDateTime64(100, 6, 'UTC'), 'delta'),
    ([1, 2], [(true, 11, 11), (false, 22, 22)], toDateTime64(500, 6, 'UTC'), 'snapshot');

SELECT
    arraySort(arrayMap(x -> tupleElement(x, 1), tupleElement(r, 2))) AS prices
FROM
(
    SELECT mergeOrderbook(prices, sizes, ts, kind) AS r
    FROM mob_snap
);

DROP TABLE mob_src;
DROP TABLE mob_snap;
