"""조건식 파라미터 정의/검증. SQL에 바인딩되는 값만 여기서 관리."""

DEFAULT_COND = {
	"ma_period": 60,
	"chg_min": 0.02,
	"chg_max": 0.12,
	"vol_mult": 1.5,
	"min_price": 1000,
	"min_amt": 1000000000,
	"sec_class": "COMMON",
	"market": "ALL",
	"over_max": 100.0,
	"bull": 1,
}

SQL_KEYS = list(DEFAULT_COND.keys())
INT_KEYS = {"ma_period", "min_price", "min_amt", "bull"}
FLOAT_KEYS = {"chg_min", "chg_max", "vol_mult", "over_max"}


def merge_cond(user=None):
	p = dict(DEFAULT_COND)
	if user:
		unknown = set(user) - set(DEFAULT_COND)
		if unknown:
			raise ValueError(f"unknown cond params: {sorted(unknown)}")
		p.update(user)

	for k in INT_KEYS:
		p[k] = int(p[k])
	for k in FLOAT_KEYS:
		p[k] = float(p[k])

	if p["ma_period"] < 2:
		raise ValueError("ma_period must be >= 2")
	if p["chg_min"] > p["chg_max"]:
		raise ValueError("chg_min > chg_max")
	return p