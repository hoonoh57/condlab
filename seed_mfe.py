"""KPI = 장종료 MFE 10%↑ 종목 선별. 모멘텀형 후보를 버전으로 등록. 1회 실행."""
from condlab import backtest as bt, params, store

NAME = "mfe10_hunt"
BASE_BT = {"vol_basis": "pace", "t_from": "09:05:00", "t_until": "11:00:00",
           "require_prev_below": 0, "base_pool": 1}
SETS = [
    ({}, {"chg_min": 0.02, "chg_max": 0.29},
     "v1 기준선. 60일선 돌파(base59) + 09:05~11:00 + pace. 등락률 상한 0.12를 0.29로 "
     "열어 강한 종목을 스스로 잘라내던 문제 제거. KPI 3종(건수/정밀도/재현율)의 출발점."),
    ({"strat": "hod"}, {"chg_min": 0.03, "chg_max": 0.29},
     "v2 코어를 당일고가 돌파(hod)로 교체. 가설: 60일선 돌파는 '내려와 있다가 회복'을 "
     "잡아 MFE가 구조적으로 낮고, 10%↑는 당일 신고가 갱신 흐름에서 나온다."),
    ({"strat": "hod", "max_amt": 50_000_000_000},
     {"chg_min": 0.05, "chg_max": 0.29, "vol_mult": 3.0},
     "v3 정밀도 가설. 대형주 제외(전일거래대금 500억 이하) + 초반강도 5%↑ + pace 3배. "
     "선별 수는 줄지만 lift가 오르는지 확인."),
]
for opt, cond, note in SETS:
    rec = store.save_cond(NAME, params.merge_cond(cond), bt.merge_bt({**BASE_BT, **opt}), note)
    print(f"{rec['name']} v{rec['ver']} 등록 - {note[:40]}...")
