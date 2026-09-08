"""지표테이블 검증: 키움 0150 '베타' 조건식 재현 + JMA 육안 대조."""
from condlab import features, api

print(features.build())
con = api._con()

D = "2026-09-08"   # 0150 캡처 일자
for lo, hi, amt in ((1.0, 10.0, 10_000_000_000),):
    row = con.execute(f"""
        SELECT count(*) FROM feat
        WHERE d = DATE '{D}' AND sec_class = 'COMMON'
          AND beta360 BETWEEN {lo} AND {hi} AND amt20 >= {amt}""").fetchone()
    print(f"beta360 {lo}~{hi} & amt20>={amt/1e8:.0f}억 -> {row[0]}종목 (키움 98)")

print(con.execute("""
    SELECT code, name, beta360, beta120, beta20, amt20, vola20, c
    FROM feat WHERE d = DATE '2026-09-08' AND beta360 BETWEEN 1 AND 10
      AND amt20 >= 10000000000 ORDER BY beta360 DESC LIMIT 15""").fetchdf())

print(con.execute("""
    SELECT d, c, jma, jma_dir, jma_slope FROM feat
    WHERE code = '005930' ORDER BY d DESC LIMIT 5""").fetchdf())
