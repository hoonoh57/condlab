"""오늘까지 확인된 조건식·검증옵션을 버전으로 등록한다. 1회만 실행."""
from condlab import params, store
from condlab import backtest as bt

NAME = "ma60_breakout"
SETS = [
    ({"vol_basis": "cum", "t_from": "09:00:00"},
     "v1 일봉 조건 그대로. 누적거래량 기준 조건식."),
    ({"vol_basis": "pace", "t_from": "09:00:00"},
     "v2 거래량을 시각 비례(pace)로 재정의한 조건식."),
    ({"vol_basis": "pace", "t_from": "09:05:00", "require_prev_below": 0,
      "sl_pct": 0.04, "hold_min": 60},
     "v3 손절 4% 확대, 09:05 기준, 전일 미돌파 조건 해제."),
]

for override, note in SETS:
    rec = store.save_cond(NAME, params.merge_cond(None), bt.merge_bt(override), note)
    print(f"{rec['name']} v{rec['ver']} 등록 - {note[:40]}...")
