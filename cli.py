"""condlab 외부 진입점. stdin/인자로 파라미터 받고 stdout에 JSON 출력."""
import argparse
import json
import sys

from condlab import api


def main():
	ap = argparse.ArgumentParser(prog="condlab")
	sub = ap.add_subparsers(dest="cmd", required=True)

	s = sub.add_parser("scan")
	s.add_argument("--date", required=True)
	s.add_argument("--params", default=None, help='JSON, 예: \'{"vol_mult":1.2}\'')

	r = sub.add_parser("scan_range")
	r.add_argument("--from", dest="d_from", required=True)
	r.add_argument("--to", dest="d_to", required=True)
	r.add_argument("--params", default=None)
	r.add_argument("--summary", action="store_true", help="종목 목록 생략")

	sub.add_parser("health")

	a = ap.parse_args()
	prm = json.loads(a.params) if getattr(a, "params", None) else None

	if a.cmd == "scan":
		out = api.scan(a.date, prm)
	elif a.cmd == "scan_range":
		out = api.scan_range(a.d_from, a.d_to, prm, include_hits=not a.summary)
	else:
		out = api.health()

	json.dump(out, sys.stdout, ensure_ascii=False, indent=2)
	sys.stdout.write("\n")


if __name__ == "__main__":
	main()