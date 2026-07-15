import json

from pgcli.pyev import Visualizer
from pgcli.explain_output_formatter import ExplainOutputFormatter


def _plan():
    return {
        "Plan": {
            "Node Type": "Hash Join",
            "Actual Total Time": 150.0,
            "Actual Loops": 1,
            "Total Cost": 2000,
            "Plan Rows": 100,
            "Actual Rows": 100,
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "orders",
                    "Schema": "public",
                    "Actual Total Time": 120.0,
                    "Actual Loops": 1,
                    "Total Cost": 1500,
                    "Plan Rows": 10,
                    "Actual Rows": 5000,
                    "Plans": [],
                },
                {
                    "Node Type": "Hash",
                    "Actual Total Time": 20.0,
                    "Actual Loops": 1,
                    "Total Cost": 200,
                    "Plan Rows": 50,
                    "Actual Rows": 50,
                    "Plans": [
                        {
                            "Node Type": "Seq Scan",
                            "Relation Name": "users",
                            "Schema": "public",
                            "Actual Total Time": 18.0,
                            "Actual Loops": 1,
                            "Total Cost": 180,
                            "Plan Rows": 50,
                            "Actual Rows": 50,
                            "Plans": [],
                        }
                    ],
                },
            ],
        },
        "Planning Time": 0.5,
        "Execution Time": 160.0,
    }


def test_explain_summary_sections():
    v = Visualizer(100, color=False, summary=True)
    v.load(_plan())
    out = v.get_list()
    assert "Summary" in out
    assert "Slowest nodes" in out
    assert "Time by relation" in out
    assert "Planner estimate misses" in out


def test_explain_summary_exclusive_time_and_slowest():
    """Exclusive time = node minus children; orders is the slowest at 120ms."""
    v = Visualizer(100, color=False, summary=True)
    v.load(_plan())
    out = v.get_list()
    summary = out[out.index("Summary") :]
    # orders (120ms exclusive, 75%) must be the first slowest node
    slowest_block = summary.split("Slowest nodes")[1]
    first_line = [ln for ln in slowest_block.splitlines() if ln.strip()][1]
    assert "public.orders" in first_line and "120.00 ms" in first_line and "75%" in first_line


def test_explain_summary_row_estimate_miss():
    """orders planned 10 rows, got 5000 -> under-estimated 500x flagged."""
    v = Visualizer(100, color=False, summary=True)
    v.load(_plan())
    out = v.get_list()
    assert "under-estimated 500x" in out


def test_explain_summary_can_be_disabled():
    v = Visualizer(100, color=False, summary=False)
    v.load(_plan())
    assert "Summary" not in v.get_list()


def test_explain_formatter_passes_summary_flag():
    data = json.dumps([_plan()])
    cur = [(data,)]
    out = "\n".join(ExplainOutputFormatter(100, summary=True).format_output(iter(cur), None))
    assert "Time by relation" in out
    cur2 = [(data,)]
    out2 = "\n".join(ExplainOutputFormatter(100, summary=False).format_output(iter(cur2), None))
    assert "Summary" not in out2
