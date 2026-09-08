"""수급 필터가 호가단위 비용을 상쇄하는지 측정. min1 09:30까지만 사용."""
import pandas as pd
from condlab import api, config

D0, D1 = "2026-03-02", "2026-09-07"      # 6개월
T_REF, T_END = "09:30:00", "15:20:00"
FEE_RT, TAX = 0.03, 0.15

con = api._con()
con.execute(f"CREATE OR REPLACE TABLE feat AS SELECT * FROM "
			f"read_parquet('{config.FEAT_PQ.as_posix()}')")

con.execute(f"""CREATE OR REPLACE TABLE d5 AS
SELECT iid, d, avg(amt) OVER (PARTITION BY iid ORDER BY d
	ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS amt5
FROM read_parquet('{config.DAILY_PQ.as_posix()}')""")

MIN1 = f"{config.MIN1_DIR.as_posix()}/*/*.parquet"
con.execute(f"""CREATE OR REPLACE TABLE mb AS
SELECT iid, CAST(d AS DATE) AS d, CAST(ts AS TIME) AS t, o, h, l, c, v
FROM read_parquet('{MIN1}', hive_partitioning = 1)
WHERE CAST(d AS DATE) BETWEEN DATE '{D0}' AND DATE '{D1}'
  AND v > 0 AND CAST(ts AS TIME) <= TIME '{T_END}'""")

con.execute(f"""CREATE OR REPLACE TABLE pre AS
SELECT iid, d,
	sum((o + h + l + c) / 4.0 * v)                  AS cum_amt,
	count(*)                                        AS n_bars,
	avg(CASE WHEN h = l THEN 1.0 ELSE 0 END) * 100  AS flat_pct,
	avg(h - l)                                      AS bar_rng_won,
	arg_max(c, t)                                   AS px,
	max(h)                                          AS pre_h
FROM mb WHERE t <= TIME '{T_REF}' GROUP BY 1, 2""")

con.execute(f"""CREATE OR REPLACE TABLE post AS
SELECT iid, d, max(h) AS hi, min(l) AS lo, arg_max(c, t) AS eod
FROM mb WHERE t > TIME '{T_REF}' GROUP BY 1, 2""")

TICK = """CASE WHEN f.market = 'KOSPI' THEN
	CASE WHEN p.px < 2000 THEN 1 WHEN p.px < 5000 THEN 5 WHEN p.px < 20000 THEN 10
		 WHEN p.px < 50000 THEN 50 WHEN p.px < 200000 THEN 100
		 WHEN p.px < 500000 THEN 500 ELSE 1000 END
ELSE CASE WHEN p.px < 2000 THEN 1 WHEN p.px < 5000 THEN 5 WHEN p.px < 20000 THEN 10
		  WHEN p.px < 50000 THEN 50 ELSE 100 END END"""

con.execute(f"""
CREATE OR REPLACE TABLE ix AS
SELECT f.code, f.name, f.market, p.d, p.px, f.vola20, f.beta250,
	f.listed_shares * p.px          AS mcap_d,
	d5.amt5, p.cum_amt, p.n_bars, p.flat_pct,
	p.cum_amt / nullif(d5.amt5, 0) AS pace,
	percent_rank() OVER (PARTITION BY p.d ORDER BY p.cum_amt / nullif(d5.amt5, 0)) AS pace_pr,
	{TICK}                          AS tick,
	p.bar_rng_won / ({TICK})        AS bar_ticks,
	({TICK}) / p.px * 100.0         AS tick_pct,
	(q.hi / p.px - 1) * 100.0       AS up,
	(q.lo / p.px - 1) * 100.0       AS dn,
	(q.eod / p.px - 1) * 100.0      AS eod,
	row_number() OVER (PARTITION BY p.d ORDER BY p.cum_amt DESC) AS amt_rank
FROM pre p
JOIN post q USING (iid, d)
JOIN feat f ON f.iid = p.iid AND f.d = p.d
JOIN d5    ON d5.iid = p.iid AND d5.d = p.d
WHERE f.sec_class = 'COMMON' AND p.px > 0 AND d5.amt5 IS NOT NULL""")

SEL = """count(*) AS n,
	round(avg(pace), 3)          AS 배율,
	round(avg(tick_pct), 3)      AS 호가단위pct,
	round(avg(flat_pct), 1)      AS 정지봉pct,
	round(avg(bar_ticks), 2)     AS 분봉진폭틱,
	round(avg(up - dn), 2)       AS 진폭,
	round(avg(up), 2)            AS 상방,
	round(100.0 * avg(CASE WHEN up >= 2 THEN 1.0 ELSE 0 END), 1) AS 상방2p,
	round(100.0 * avg(CASE WHEN up >= 3 THEN 1.0 ELSE 0 END), 1) AS 상방3p,
	round(avg(eod), 2)           AS 종가"""


def block(title, rows, extra="1=1"):
	print(f"\n=== {title} ===")
	print(pd.concat([con.execute(
		f"SELECT '{lab}' AS seg, {SEL} FROM ix WHERE {w} AND {extra}").fetchdf()
		for lab, w in rows]).to_string(index=False))


PRICE = [("1) ~2천원", "px < 2000"), ("2) 2천~5천", "px >= 2000 AND px < 5000"),
		 ("3) 5천~2만", "px >= 5000 AND px < 20000"),
		 ("4) 2만~5만", "px >= 20000 AND px < 50000"),
		 ("5) 5만~20만", "px >= 50000 AND px < 200000"),
		 ("6) 20만 초과", "px >= 200000")]
AMT5 = [("1) 5일 ~10억", "amt5 < 1e9"), ("2) 10억~50억", "amt5 >= 1e9 AND amt5 < 5e9"),
		("3) 50억~200억", "amt5 >= 5e9 AND amt5 < 2e10"),
		("4) 200억~1천억", "amt5 >= 2e10 AND amt5 < 1e11"),
		("5) 1천억 초과", "amt5 >= 1e11")]
CUM = [("1) 09:30 ~5억", "cum_amt < 5e8"), ("2) 5억~20억", "cum_amt >= 5e8 AND cum_amt < 2e9"),
	   ("3) 20억~100억", "cum_amt >= 2e9 AND cum_amt < 1e10"),
	   ("4) 100억 초과", "cum_amt >= 1e10"), ("5) 당일 수급 상위30", "amt_rank <= 30")]

block("주가대별 (수급 필터 없음)", PRICE)
block("주가대별 (5일 50억↑ + 09:30 20억↑ 적용)", PRICE,
	  "amt5 >= 5e9 AND cum_amt >= 2e9")
block("5일 평균거래대금별", AMT5)
block("09:30 누적거래대금별", CUM)
block("중소형(500억~3조) 내 수급별", CUM, "mcap_d BETWEEN 5e10 AND 3e12")
block("대형(3조↑) 내 수급별", CUM, "mcap_d > 3e12")

print("\n=== 배율 중립값 (09:30 시점) ===")
print(con.execute("""
	SELECT round(median(pace), 3) AS 중앙값, round(quantile(pace, 0.75), 3) AS p75,
		   round(quantile(pace, 0.90), 3) AS p90, round(quantile(pace, 0.99), 3) AS p99
	FROM ix""").fetchdf().to_string(index=False))

PACE = [("1) 배율 하위50%", "pace_pr < 0.5"),
		("2) 50~80%", "pace_pr >= 0.5 AND pace_pr < 0.8"),
		("3) 80~95%", "pace_pr >= 0.8 AND pace_pr < 0.95"),
		("4) 95~99%", "pace_pr >= 0.95 AND pace_pr < 0.99"),
		("5) 상위 1%", "pace_pr >= 0.99")]

block("배율만 (절대하한 없음)", PACE)
block("배율 + 09:30 누적 100억↑", PACE, "cum_amt >= 1e10")
block("배율 + 100억↑ + 중소형(500억~3조)", PACE,
	  "cum_amt >= 1e10 AND mcap_d BETWEEN 5e10 AND 3e12")
block("배율 + 100억↑ + 대형(3조↑)", PACE, "cum_amt >= 1e10 AND mcap_d > 3e12")

print("\n=== 최종 후보군 (중소형 + 100억↑ + 배율 상위5%) 주가대별 ===")
print(pd.concat([con.execute(
	f"SELECT '{lab}' AS seg, {SEL} FROM ix WHERE {w} "
	"AND cum_amt >= 1e10 AND mcap_d BETWEEN 5e10 AND 3e12 AND pace_pr >= 0.95"
	).fetchdf() for lab, w in PRICE]).to_string(index=False))

print("\n=== 최종 후보군 일별 종목수 분포 ===")
print(con.execute("""
	SELECT round(avg(n), 1) AS 일평균, min(n) AS 최소, max(n) AS 최대,
		   round(quantile(n, 0.5), 0) AS 중앙 FROM (
	  SELECT d, count(*) AS n FROM ix
	  WHERE cum_amt >= 1e10 AND mcap_d BETWEEN 5e10 AND 3e12 AND pace_pr >= 0.95
	  GROUP BY d)""").fetchdf().to_string(index=False))
