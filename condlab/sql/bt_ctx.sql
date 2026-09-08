CREATE OR REPLACE TABLE btctx AS
SELECT
    iid,
    d,
    AVG(c) OVER w_base AS base_prev,
    AVG(c) OVER w_full AS prev_ma,
    LAG(c) OVER pw AS prev_c,
    LAG(v) OVER pw AS prev_v,
    LAG(amt) OVER pw AS prev_amt,
    COUNT(*) OVER w_base AS n_base,
    COUNT(*) OVER w_full AS n_full
FROM read_parquet('{DAILY}')
WINDOW
    pw AS (PARTITION BY iid ORDER BY d),
    w_base AS (PARTITION BY iid ORDER BY d ROWS BETWEEN {MA_M1} PRECEDING AND 1 PRECEDING),
    w_full AS (PARTITION BY iid ORDER BY d ROWS BETWEEN {MA} PRECEDING AND 1 PRECEDING);
