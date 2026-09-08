"""오늘까지 확인된 조건식·검증옵션을 버전으로 등록한다. 1회만 실행."""
from condlab import params, store
from condlab import backtest as bt

NAME = "ma60_breakout"
SETS = [
    ({"vol_basis": "cum", "t_from": "09:00:00"},
  "v1 일봉 조건 그대로 적용. 2026-09-07 신호 5건뿐. 원인은 vol_mult=1.5가 "
  "'당일 누적거래량 > 전일 하루 총거래량 x1.5'를 뜻해 오전에는 통과 불가. "
  "vol_basis=none이면 25건, liq_basis까지 none이면 75건으로 확인됨."),
    ({"vol_basis": "pace", "t_from": "09:00:00"},
  "v2 거래량을 시각비례(pace)로 재정의: 전일거래량을 09:00부터 흐른 분/380으로 "
  "안분. 21건으로 키움 1516의 32건과 자리수 일치. 승률 23.81%, 평균 -0.96%, "
  "장종료 -0.28%, MFE 2.79 / MAE -2.75 (대칭=방향성 우위 없음), "
  "익절5·손절16·종가청산0. 하루 0.31초. 시장대비(등가중) -0.09%."),
    ({"vol_basis": "pace", "t_from": "09:05:00", "require_prev_below": 0,
      "sl_pct": 0.04, "hold_min": 60},
  "v3 가설검증안. 1분봉에서 ±2%는 거의 반드시 먼저 터치되어 청산규칙이 결과를 "
  "지배하므로 손절 4%로 확대. require_prev_below는 A=B=21로 무의미 확인되어 해제 "
  "(장중 교차 자체가 오늘 아래에 있었음을 이미 함의). t_from 09:05는 20건으로 "
  "1건만 감소해 손실 미미."),
]

for override, note in SETS:
    rec = store.save_cond(NAME, params.merge_cond(None), bt.merge_bt(override), note)
    print(f"{rec['name']} v{rec['ver']} 등록 - {note[:40]}...")
