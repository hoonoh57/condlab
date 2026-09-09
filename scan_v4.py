"""scan_v4.py — 조건식 = 모집단 선별기. KPI: 순도달률 + 대장주성 + 테마주성
   손익/승률/EV 측정 전면 제거. 정밀 전략은 별도 단계.
"""
from condlab import api, config
import pandas as pd
pd.set_option("display.width", 250)

D0, D1 = "2026-03-02", "2026-09-07"
C0, C1 = "2025-12-01", "2026-02-27"     # 상관 학습 구간 (D0 이전 = 룩어헤드 0)
T_OPEN, T_TO, T_END = "09:01:00", "10:00:00", "15:20:00"
HZ    = 60      # 관측 지평(분)
NET   = 2.0     # 제비용+슬리피지 커버 기준
CUNIV = 700     # 상관 계산 종목수 (RAM 부족시 400으로)
PEERK = 30      # 동종으로 볼 상위 상관 종목수

con = api._con()
MP = (config.MIN1_DIR / '**' / '*.parquet').as_posix()
DP = config.DAILY_PQ.as_posix()
con.execute(f"CREATE OR REPLACE VIEW feat AS SELECT * FROM read_parquet('{config.FEAT_PQ.as_posix()}')")

# ── 1) 상관 클러스터 (학습구간 전용)
con.execute(f"""CREATE OR REPLACE TABLE cuniv AS
SELECT iid FROM read_parquet('{DP}') WHERE d BETWEEN DATE '{C0}' AND DATE '{C1}'
GROUP BY 1 HAVING count(*) >= 50 AND median(amt) >= 2e9
ORDER BY median(amt) DESC LIMIT {CUNIV}""")
con.execute(f"""CREATE OR REPLACE TABLE ret AS
SELECT iid, d, r - avg(r) OVER (PARTITION BY d) AS r
FROM (
  SELECT iid, d, c/lag(c) OVER (PARTITION BY iid ORDER BY d) - 1 AS r
  FROM read_parquet('{DP}')
  WHERE d BETWEEN DATE '{C0}' AND DATE '{C1}' AND iid IN (SELECT iid FROM cuniv)
) WHERE r IS NOT NULL""")
print("상관 유니버스", con.execute("SELECT count(*) FROM cuniv").fetchone()[0], "종목 — 1~3분 소요")
con.execute(f"""CREATE OR REPLACE TABLE peer AS
SELECT iid, pid, rho FROM (
  SELECT a.iid, b.iid AS pid, corr(a.r, b.r) AS rho, count(*) AS n
  FROM ret a JOIN ret b USING (d)
  WHERE a.iid <> b.iid AND a.r IS NOT NULL AND b.r IS NOT NULL
  GROUP BY 1,2 HAVING count(*) >= 40
) QUALIFY row_number() OVER (PARTITION BY iid ORDER BY rho DESC) <= {PEERK}""")
print(con.execute("""SELECT round(median(rho),3) AS 중앙, round(avg(rho),3) AS 평균,
       round(quantile_cont(rho,0.9),3) AS p90, round(max(rho),3) AS 최대 FROM peer""")
      .fetchdf().to_string(index=False))

# ── 2) 일봉 컨텍스트
con.execute(f"""CREATE OR REPLACE TABLE d5 AS
SELECT iid, d, pc, amt5, rng5,
  max(dchg) OVER (PARTITION BY iid ORDER BY d ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS pmax20
FROM (
  SELECT iid, d,
    lag(c) OVER w AS pc,
    c / nullif(lag(c) OVER w, 0) * 100 - 100 AS dchg,
    avg(amt) OVER (PARTITION BY iid ORDER BY d ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS amt5,
    avg((h-l)/nullif(c,0)*100) OVER (PARTITION BY iid ORDER BY d ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS rng5
  FROM read_parquet('{DP}')
  WINDOW w AS (PARTITION BY iid ORDER BY d)
)""")

# ── 3) 분봉 + 시장 전체 순위 (10:00까지만)
con.execute(f"""CREATE OR REPLACE TABLE mb AS
SELECT iid, d, CAST(ts AS TIME) AS t, o,h,l,c,v, c*v AS amt,
       row_number() OVER (PARTITION BY iid, d ORDER BY ts) AS k
FROM read_parquet('{MP}')
WHERE d BETWEEN DATE '{D0}' AND DATE '{D1}'
  AND CAST(ts AS TIME) BETWEEN TIME '{T_OPEN}' AND TIME '{T_END}'""")

con.execute(f"""CREATE OR REPLACE TABLE bars AS
SELECT m.*, max(m.h) OVER w AS cum_h, min(m.l) OVER w AS cum_l,
  first_value(m.o) OVER w AS op, sum(m.amt) OVER w AS cum_amt,
  sum(m.c*m.v) OVER w / nullif(sum(m.v) OVER w,0) AS vwap,
  lag(m.c,5) OVER p AS c5
FROM mb m WHERE m.t <= TIME '{T_TO}'
WINDOW p AS (PARTITION BY m.iid,m.d ORDER BY m.t),
       w AS (PARTITION BY m.iid,m.d ORDER BY m.t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)""")

# 동종 동반 상승 강도 (테마주성) — 5분 격자에서만
con.execute(f"""CREATE OR REPLACE TABLE heat AS
SELECT b.iid, b.d, b.t, avg(pb.c/pd.pc*100-100) AS peer_chg,
       sum(CASE WHEN pb.c/pd.pc*100-100 >= 3 THEN 1 ELSE 0 END) AS peer_up
FROM bars b JOIN peer p ON p.iid = b.iid
     JOIN bars pb ON pb.iid = p.pid AND pb.d = b.d AND pb.t = b.t
     JOIN d5 pd ON pd.iid = p.pid AND pd.d = b.d
WHERE minute(b.t) % 5 = 0 GROUP BY 1,2,3""")

# 동종 내 거래대금 순위 (대장주성)
con.execute(f"""CREATE OR REPLACE TABLE lead_r AS
SELECT b.iid, b.d, b.t,
  1 + sum(CASE WHEN pb.cum_amt > b.cum_amt THEN 1 ELSE 0 END) AS peer_rank,
  b.cum_amt / nullif(max(pb.cum_amt),0) AS vs_top
FROM bars b JOIN peer p ON p.iid = b.iid
     JOIN bars pb ON pb.iid = p.pid AND pb.d = b.d AND pb.t = b.t
WHERE minute(b.t) % 5 = 0 GROUP BY 1,2,3, b.cum_amt""")

# ── 4) 진입봉 (종목-일 1회 dedup) + 전방 MFE/MAE
con.execute(f"""CREATE OR REPLACE TABLE cand AS
SELECT b.iid,b.d,b.t,b.k, b.c AS px, f.code,f.name, f.mcap, d5.rng5, d5.pmax20,
  b.c/d5.pc*100-100 AS chg, b.op/d5.pc*100-100 AS gap,
  b.c/b.cum_h*100-100 AS ext, b.c/b.cum_l*100-100 AS off_low,
  b.c/b.vwap*100-100 AS vs_vwap, b.c/nullif(b.c5,0)*100-100 AS mom5,
  b.cum_amt/nullif(d5.amt5,0) AS pace, b.cum_amt,
  b.cum_amt/nullif(f.mcap,0)*100 AS turn,
  h.peer_chg, h.peer_up, l.peer_rank, l.vs_top,
  row_number() OVER (PARTITION BY b.iid,b.d ORDER BY b.t) AS seq
FROM bars b JOIN d5 USING(iid,d) JOIN feat f ON f.iid=b.iid AND f.d=b.d
     LEFT JOIN heat   h USING(iid,d,t)
     LEFT JOIN lead_r l USING(iid,d,t)
WHERE minute(b.t) % 5 = 0 AND f.sec_class='COMMON' AND d5.amt5 >= 5e9
  AND d5.rng5 IS NOT NULL""")

con.execute(f"""CREATE OR REPLACE TABLE obs AS
SELECT c.*, max(m.h)/c.px*100-100 AS mfe, min(m.l)/c.px*100-100 AS mae
FROM cand c JOIN mb m ON m.iid=c.iid AND m.d=c.d AND m.k>c.k AND m.k<=c.k+{HZ}
GROUP BY ALL""")
days = con.execute("SELECT count(DISTINCT d) FROM obs").fetchone()[0]
print("관찰", con.execute("SELECT count(*) FROM obs").fetchone()[0], f"/ {days}영업일")

SEL = f"""count(*) AS n, count(DISTINCT (iid::VARCHAR||d::VARCHAR)) AS 종목일,
  round(100.0*avg(CASE WHEN mfe>={NET} THEN 1 ELSE 0 END),2) AS 순도달,
  round(100.0*avg(CASE WHEN mfe>=3 THEN 1 ELSE 0 END),2) AS 도달3,
  round(100.0*avg(CASE WHEN mfe>=5 THEN 1 ELSE 0 END),2) AS 도달5,
  round(100.0*avg(CASE WHEN mae<=-{NET} THEN 1 ELSE 0 END),2) AS 하방2,
  round(avg(mfe),2) AS MFE, round(avg(mae),2) AS MAE,
  round(avg(mfe)/nullif(-avg(mae),0),2) AS 비대칭"""

def dim(title, expr, extra="1=1"):
    df = con.execute(f"""SELECT {expr} AS 구간, {SEL} FROM obs WHERE {extra}
        GROUP BY 1 HAVING count(DISTINCT (iid::VARCHAR||d::VARCHAR))>=200 ORDER BY 1""").fetchdf()
    print(f"\n=== {title} ===\n{df.to_string(index=False)}")
    return df.assign(축=title)

NB = con.execute(f"SELECT 100.0*avg(CASE WHEN mfe>={NET} THEN 1 ELSE 0 END) FROM obs").fetchone()[0]
print(f"순도달 기준선 {NB:.2f}%")

AX = [
 ("① 동종순위(대장주성)", "CASE WHEN peer_rank IS NULL THEN 'z 미분류' WHEN peer_rank=1 THEN 'a 1위' "
     "WHEN peer_rank<=3 THEN 'b 2~3위' WHEN peer_rank<=10 THEN 'c 4~10위' ELSE 'd 11위+' END"),
 ("② 동종열기(테마주성)", "CASE WHEN peer_chg IS NULL THEN 'z 미분류' WHEN peer_chg<0 THEN 'a <0' "
     "WHEN peer_chg<1 THEN 'b 0~1' WHEN peer_chg<2 THEN 'c 1~2' ELSE 'd 2+' END"),
 ("③ 동종급등수",  "CASE WHEN peer_up IS NULL THEN 'z' WHEN peer_up=0 THEN 'a 0개' "
     "WHEN peer_up<=2 THEN 'b 1~2' WHEN peer_up<=5 THEN 'c 3~5' ELSE 'd 6+' END"),
 ("④ 회전율",     "CASE WHEN turn<0.3 THEN 'a <0.3' WHEN turn<1 THEN 'b 0.3~1' "
     "WHEN turn<3 THEN 'c 1~3' ELSE 'd 3+' END"),
 ("⑤ 평소진폭",   "CASE WHEN rng5<4 THEN 'a <4' WHEN rng5<8 THEN 'b 4~8' "
     "WHEN rng5<15 THEN 'c 8~15' ELSE 'd 15+' END"),
 ("⑥ 신규성",     "CASE WHEN chg > pmax20 THEN 'a 20일 최대급등' ELSE 'b 반복' END"),
 ("⑦ 등락률",     "CASE WHEN chg<0 THEN 'a <0' WHEN chg<3 THEN 'b 0~3' WHEN chg<6 THEN 'c 3~6' "
     "WHEN chg<10 THEN 'd 6~10' WHEN chg<15 THEN 'e 10~15' ELSE 'f 15+' END"),
 ("⑧ 배율",       "CASE WHEN pace<0.1 THEN 'a <0.1' WHEN pace<0.3 THEN 'b 0.1~0.3' "
     "WHEN pace<0.7 THEN 'c 0.3~0.7' WHEN pace<1.5 THEN 'd 0.7~1.5' ELSE 'e 1.5+' END"),
 ("⑨ 시각",       "printf('%02d:%02d', hour(t), CAST(floor(minute(t)/10)*10 AS INTEGER))"),
]
alld = pd.concat([dim(t,e) for t,e in AX])
print("\n===== 순도달 기준선 초과 =====")
print(alld[(alld["순도달"]>NB)].sort_values("순도달",ascending=False)
      [["축","구간","종목일","순도달","도달5","하방2","MFE","MAE","비대칭"]].to_string(index=False))

# ── 5) 최종 모집단 후보 (누적)
R = [("과열회피",  "chg < 12"),
     ("신고가회피","ext <= -1.0"),
     ("급등",     "chg >= 3"),
     ("수급",     "pace >= 0.3"),
     ("진폭체질",  "rng5 >= 5"),
     ("대장주성",  "peer_rank <= 3"),
     ("테마주성",  "peer_up >= 1")]
w, rows = [], []
for nm,c in R:
    w.append(c)
    df = con.execute(f"""SELECT {SEL} FROM (SELECT * FROM obs WHERE {' AND '.join(w)}
                         QUALIFY row_number() OVER (PARTITION BY iid,d ORDER BY t)=1)""").fetchdf()
    df.insert(0,"조합","+".join(x[0] for x in R[:len(w)]))
    df.insert(1,"일평균", round(df["종목일"][0]/days,1))
    rows.append(df)
print("\n===== 누적 조합 (종목-일 1회 dedup) =====")
print(pd.concat(rows).to_string(index=False))

print("\n===== 최종 후보 샘플 =====")
print(con.execute(f"""SELECT d,t,code,name, round(chg,1) AS 등락, round(ext,1) AS 고점대,
  round(pace,2) AS 배율, round(rng5,1) AS 평소진폭, peer_rank AS 동종순위, peer_up AS 동종급등,
  round(peer_chg,1) AS 동종열기, round(mfe,1) AS MFE, round(mae,1) AS MAE, px
FROM obs WHERE {' AND '.join(c for _,c in R)}
QUALIFY row_number() OVER (PARTITION BY iid,d ORDER BY t)=1
ORDER BY d DESC, t LIMIT 30""").fetchdf().to_string(index=False))
