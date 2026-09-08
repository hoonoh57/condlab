"""jma.ts정확 포팅. 차트와 소수 4자리까지 일치해야 한다."""
from __future__ import annotations

import math
import sys


def round_to_even(value: float, digits: int) -> float:
	if value != value or value in (float("inf"), float("-inf")):
		return value
	factor = 10 ** digits
	scaled = value * factor
	floor = math.floor(scaled)
	fraction = scaled - floor
	epsilon = sys.float_info.epsilon * max(1.0, abs(scaled)) * 4
	if abs(fraction - 0.5) <= epsilon:
		rounded = floor if floor % 2 == 0 else floor + 1
	else:
		rounded = math.floor(scaled + 0.5)   # JS Math.round
	return rounded / factor


def step(state, source: float, period: int, phase: int, power: int):
	"""(state, value, direction, slope) 반환. state=None이면 초기화."""
	if state is None:
		e0, e1, e2 = source, 0.0, 0.0
		last, warm, direction, count = source, 0.0, 0, 0
	else:
		e0, e1, e2, last, warm, direction, count = state

	beta = 0.45 * (period - 1) / (0.45 * (period - 1) + 2)
	alpha = beta ** power
	e0 = (1 - alpha) * source + alpha * e0
	e1 = (source - e0) * (1 - beta) + beta * e1
	e2 = (e0 + (phase / 100 + 1.5) * e1 - last) * ((1 - alpha) ** 2) + (alpha ** 2) * e2

	count += 1
	warm += source
	current = (round_to_even(warm / count, 4) if count <= period
			   else round_to_even(e2 + last, 4))
	previous = last

	if current > previous:
		direction = 1
	elif current < previous:
		direction = -1
	elif direction == 0:
		direction = 1

	slope = round_to_even((current / previous - 1) * 100, 1) if previous else 0.0
	return (e0, e1, e2, current, warm, direction, count), current, direction, slope


def series(closes, period: int = 7, phase: int = 50, power: int = 2) -> list[float]:
	state, out = None, []
	for close in closes:
		state, value, _, _ = step(state, float(close), period, phase, power)
		out.append(value)
	return out
