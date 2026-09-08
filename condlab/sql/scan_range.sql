SELECT
    x.d AS d,
    i.code,
    i.name,
    i.market,
    x.c AS close_px,
    round(x.base_prev, 1) AS breakout_px,
    round((x.c / x.prev_c - 1) * 100, 2) AS chg_pct,
    round(x.v / nullif(x.prev_v, 0), 2) AS vol_ratio,
    round((x.c / x.base_prev - 1) * 100, 2) AS over_pct
FROM ctx x
JOIN read_parquet('{INST}') i ON i.iid = x.iid
WHERE x.d BETWEEN CAST($d_from AS DATE) AND CAST($d_to AS DATE)
  AND x.n_base = $ma_period - 1
  AND x.n_full = $ma_period
  AND ($sec_class = 'ALL' OR i.sec_class = $sec_class)
  AND ($market = 'ALL' OR i.market = $market)
  AND i.d_first <= x.d
  AND i.d_last >= x.d
  AND x.prev_c < x.prev_ma
  AND x.c > x.base_prev
  AND ($bull = 0 OR x.c > x.o)
  AND x.c > x.prev_c
  AND x.c / x.prev_c - 1 BETWEEN $chg_min AND $chg_max
  AND x.v > x.prev_v * $vol_mult
  AND x.c >= $min_price
  AND x.amt >= $min_amt
  AND (x.c / x.base_prev - 1) * 100 <= $over_max
ORDER BY x.d, over_pct;