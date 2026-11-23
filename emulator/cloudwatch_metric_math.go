package emulator

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// The metric-math subset GetMetricData evaluates (#1595).
//
// A MetricDataQuery's Expression may be metric math, a SEARCH or a Metrics Insights SELECT.
// Substrate evaluates the arithmetic core of metric math and nothing else: numbers, the Ids of the
// request's other queries, the four operators + - * / with the usual precedence, unary minus and
// parentheses. That covers the error-rate and sum-of-errors shapes the reference's own examples
// use ("m2 / m1", "(error_total/m4)*100", "e2 + m3").
//
// Two series combine at the timestamps both carry, and a series and a number combine at every
// timestamp of the series. A point divided by zero, or one whose result is not a finite number,
// is dropped rather than reported, so no result carries a value no protocol can render. Those are
// substrate's readings: the metric-math page describes the operators, not how they align two
// series whose timestamps differ, and the series GetMetricData answers are laid on the same period
// grid from the same rounded start, so a request's MetricStat queries of one Period align anyway.
//
// An Expression longer than MetricDataQuery's published "Maximum length of 2048" is refused with
// InvalidParameterValue before it is parsed, and parentheses and unary minus may nest at most
// [cwMathMaxDepth] deep, so no expression can exhaust the parser's stack.
//
// Anything else — a function such as SUM(METRICS()), METRICS(), FILL or ANOMALY_DETECTION_BAND, a
// SEARCH, a SELECT, an expression that is only a number (1/0 included), an unknown Id, nesting past
// the depth limit, or a cycle — is not
// evaluated. Its result carries no values and a Messages entry saying so, with Code Unsupported;
// that code and its text are substrate's own, since MessageData publishes neither a code list nor
// wording, and it is a message rather than an error because "if there is a message, as much of the
// operation as possible is still executed."

// cwUnsupportedExpressionCode is the MessageData Code substrate attaches to a result whose
// expression it did not evaluate.
const cwUnsupportedExpressionCode = "Unsupported"

// cwMaxExpressionLength is MetricDataQuery Expression's "Maximum length of 2048".
const cwMaxExpressionLength = 2048

// cwMathMaxDepth bounds how deeply parentheses and unary minus nest in one expression. It is a
// guard against a hostile expression rather than a published limit.
const cwMathMaxDepth = 64

// cwSeriesValues is one time series in ascending timestamp order.
type cwSeriesValues struct {
	ts   []time.Time
	vals []float64
}

// cwMathValue is an intermediate value: a series, or a scalar when series is nil.
type cwMathValue struct {
	series *cwSeriesValues
	scalar float64
}

// cwMathResolver supplies the series an Id names.
type cwMathResolver func(id string) (cwSeriesValues, error)

// cwMathParser is a recursive-descent parser and evaluator over one expression.
type cwMathParser struct {
	src     string
	pos     int
	depth   int
	resolve cwMathResolver
}

// enter records one level of nesting, refusing one past [cwMathMaxDepth].
func (p *cwMathParser) enter() error {
	p.depth++
	if p.depth > cwMathMaxDepth {
		return fmt.Errorf("the expression nests deeper than %d levels", cwMathMaxDepth)
	}
	return nil
}

// cwEvalExpression evaluates expr, or explains why it was not.
func cwEvalExpression(expr string, resolve cwMathResolver) (cwSeriesValues, error) {
	p := &cwMathParser{src: expr, resolve: resolve}
	v, err := p.expr()
	if err != nil {
		return cwSeriesValues{}, err
	}
	p.skipSpace()
	if p.pos != len(p.src) {
		return cwSeriesValues{}, fmt.Errorf("unexpected %q at offset %d", p.src[p.pos:], p.pos)
	}
	if v.series == nil {
		return cwSeriesValues{}, fmt.Errorf("the expression is a constant, which substrate does not lay on a time series")
	}
	return *v.series, nil
}

func (p *cwMathParser) skipSpace() {
	for p.pos < len(p.src) && (p.src[p.pos] == ' ' || p.src[p.pos] == '\t' || p.src[p.pos] == '\n') {
		p.pos++
	}
}

func (p *cwMathParser) peek() byte {
	p.skipSpace()
	if p.pos >= len(p.src) {
		return 0
	}
	return p.src[p.pos]
}

func (p *cwMathParser) expr() (cwMathValue, error) {
	left, err := p.term()
	if err != nil {
		return cwMathValue{}, err
	}
	for {
		op := p.peek()
		if op != '+' && op != '-' {
			return left, nil
		}
		p.pos++
		right, err := p.term()
		if err != nil {
			return cwMathValue{}, err
		}
		left = cwMathCombine(left, right, op)
	}
}

func (p *cwMathParser) term() (cwMathValue, error) {
	left, err := p.unary()
	if err != nil {
		return cwMathValue{}, err
	}
	for {
		op := p.peek()
		if op != '*' && op != '/' {
			return left, nil
		}
		p.pos++
		right, err := p.unary()
		if err != nil {
			return cwMathValue{}, err
		}
		left = cwMathCombine(left, right, op)
	}
}

func (p *cwMathParser) unary() (cwMathValue, error) {
	if p.peek() == '-' {
		p.pos++
		if err := p.enter(); err != nil {
			return cwMathValue{}, err
		}
		defer func() { p.depth-- }()
		v, err := p.unary()
		if err != nil {
			return cwMathValue{}, err
		}
		return cwMathCombine(cwMathValue{scalar: -1}, v, '*'), nil
	}
	return p.primary()
}

func (p *cwMathParser) primary() (cwMathValue, error) {
	c := p.peek()
	switch {
	case c == '(':
		p.pos++
		if err := p.enter(); err != nil {
			return cwMathValue{}, err
		}
		defer func() { p.depth-- }()
		v, err := p.expr()
		if err != nil {
			return cwMathValue{}, err
		}
		if p.peek() != ')' {
			return cwMathValue{}, fmt.Errorf("missing closing parenthesis")
		}
		p.pos++
		return v, nil
	case c >= '0' && c <= '9' || c == '.':
		start := p.pos
		for p.pos < len(p.src) && (p.src[p.pos] >= '0' && p.src[p.pos] <= '9' || p.src[p.pos] == '.') {
			p.pos++
		}
		f, err := strconv.ParseFloat(p.src[start:p.pos], 64)
		if err != nil {
			return cwMathValue{}, fmt.Errorf("%q is not a number", p.src[start:p.pos])
		}
		return cwMathValue{scalar: f}, nil
	case c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
		start := p.pos
		for p.pos < len(p.src) && cwMathIdentByte(p.src[p.pos]) {
			p.pos++
		}
		id := p.src[start:p.pos]
		if p.peek() == '(' {
			return cwMathValue{}, fmt.Errorf("the function %s is not evaluated", strings.ToUpper(id))
		}
		s, err := p.resolve(id)
		if err != nil {
			return cwMathValue{}, err
		}
		return cwMathValue{series: &s}, nil
	case c == 0:
		return cwMathValue{}, fmt.Errorf("the expression ends early")
	}
	return cwMathValue{}, fmt.Errorf("unexpected %q at offset %d", string(c), p.pos)
}

func cwMathIdentByte(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_'
}

// cwMathApply applies one operator, reporting false for a division by zero and for a result that
// is not a finite number.
func cwMathApply(a, b float64, op byte) (float64, bool) {
	var v float64
	switch op {
	case '+':
		v = a + b
	case '-':
		v = a - b
	case '*':
		v = a * b
	default:
		if b == 0 {
			return 0, false
		}
		v = a / b
	}
	return v, !math.IsNaN(v) && !math.IsInf(v, 0)
}

// cwMathCombine applies op to two values; see the file comment for how series align.
func cwMathCombine(a, b cwMathValue, op byte) cwMathValue {
	switch {
	case a.series == nil && b.series == nil:
		// A constant stays a constant even when it is undefined, so 1/0 is reported as the
		// constant expression it is; a series it later combines with drops every point.
		v, ok := cwMathApply(a.scalar, b.scalar, op)
		if !ok {
			return cwMathValue{scalar: math.NaN()}
		}
		return cwMathValue{scalar: v}
	case a.series != nil && b.series != nil:
		out := &cwSeriesValues{}
		j := 0
		for i, t := range a.series.ts {
			for j < len(b.series.ts) && b.series.ts[j].Before(t) {
				j++
			}
			if j < len(b.series.ts) && b.series.ts[j].Equal(t) {
				if v, ok := cwMathApply(a.series.vals[i], b.series.vals[j], op); ok {
					out.ts, out.vals = append(out.ts, t), append(out.vals, v)
				}
			}
		}
		return cwMathValue{series: out}
	default:
		out := &cwSeriesValues{}
		s, scalar, seriesLeft := a.series, b.scalar, true
		if s == nil {
			s, scalar, seriesLeft = b.series, a.scalar, false
		}
		for i, t := range s.ts {
			x, y := s.vals[i], scalar
			if !seriesLeft {
				x, y = y, x
			}
			if v, ok := cwMathApply(x, y, op); ok {
				out.ts, out.vals = append(out.ts, t), append(out.vals, v)
			}
		}
		return cwMathValue{series: out}
	}
}
