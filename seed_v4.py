"""요인분석(68일)에서 실측된 조합을 조건식으로 확정. 1회 실행."""
from condlab import backtest as bt, params, store

NAME = "vola_lowpx"

# 모집단 정의를 조합탐색과 동일하게 맞춘다. 여기서 어긋나면 32.54%가 재현되지 않는다.
POOL = {"min_amt": 1_000_000_000, "min_price": 1000,
        "chg_min": -1.0, "chg_max": 1.0, "bull": 0, "over_max": 1000.0}
BASE = {"strat": "state", "t_from": "09:05:00", "t_until": "09:05:00",
        "vol_basis": "none", "liq_basis": "prev_amt",
        "min_vola": 5.39, "max_price": 4877, "base_pool": 1}

SETS = [
    ({}, {"tp_pct": 0.99, "sl_pct": 0.99, "hold_min": 390},
     "v4 재현검증용. 청산 없이(익절/손절 99%) 09:05 일괄진입 후 장종료. "
     "2026-06-01~09-07 실행 시 하루 13.9종목 · MFE10%↑ 정밀도 30.6% · 선별력 4.6배가 "
     "나와야 조합탐색과 성과검증이 같은 것을 재고 있다는 증거. 컷 출처: 장중변동%(고저폭) "
     "상위10% = 5.39%, 주가 하위10% = 4877원. 3단계 60일선이격은 +2%p뿐이라 제외."),
    ({}, {"tp_pct": 0.05, "sl_pct": 0.03, "hold_min": 390, "fee_pct": 0.005},
     "v5 청산규칙 A. 목표 5% / 손절 3%. 수수료 0.5%는 저가주 스프레드·슬리피지 대용. "
     "근거: 이 바스켓 평균MFE 9.26% vs 평균종료 -2.42% = 보유하면 반납, 목표가 필수."),
    ({}, {"tp_pct": 0.08, "sl_pct": 0.04, "hold_min": 390, "fee_pct": 0.005},
     "v6 청산규칙 B. 목표 8% / 손절 4%. MFE10%↑가 30.6%이므로 목표를 높게 잡아 "
     "적중 시 이익을 키우는 안. v5와 비교해 기대값이 큰 쪽을 채택."),
]
for opt, exit_rule, note in SETS:
    rec = store.save_cond(NAME, params.merge_cond(POOL),
                          bt.merge_bt({**BASE, **opt, **exit_rule}), note)
    print(f"{rec['name']} v{rec['ver']} 등록 - {note[:40]}...")
