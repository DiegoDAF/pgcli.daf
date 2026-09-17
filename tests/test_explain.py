import json

from pgcli.pyev import Visualizer
from pgcli.explain_output_formatter import ExplainOutputFormatter
from pgcli.pgexecute import PGExecute


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
    assert "By table" in out
    assert "By node type" in out
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
    assert "By table" in out
    cur2 = [(data,)]
    out2 = "\n".join(ExplainOutputFormatter(100, summary=False).format_output(iter(cur2), None))
    assert "Summary" not in out2


# ---------------------------------------------------------------------------
# Plan diagnostics
#
# Every threshold and field name below was checked against a live PostgreSQL 17
# server: the queries that produce each symptom are in the changelog entry.
# ---------------------------------------------------------------------------


def _wrap(node, execution_time=100.0):
    """Wrap a single node as a complete EXPLAIN ANALYZE document."""
    node.setdefault("Actual Loops", 1)
    node.setdefault("Actual Total Time", 50.0)
    node.setdefault("Total Cost", 100)
    node.setdefault("Plan Rows", 1)
    node.setdefault("Actual Rows", 1)
    node.setdefault("Plans", [])
    return {"Plan": node, "Planning Time": 0.5, "Execution Time": execution_time}


def _diagnose(node):
    v = Visualizer(100, color=False, summary=True)
    v.load(_wrap(node))
    return v


def test_hash_spill_is_reported():
    v = _diagnose({"Node Type": "Hash", "Hash Batches": 32, "Original Hash Batches": 1, "Peak Memory Usage": 2048})
    assert [d["topic"] for d in v.diagnostics] == ["work_mem"]
    detail = v.diagnostics[0]["detail"]
    assert "32 batches" in detail and "planned 1" in detail and "2.0 MB" in detail
    out = v.get_list()
    assert "spill disk" in out  # inline tag
    assert "Diagnostics:" in out and "work_mem" in out


def test_hash_without_spill_is_quiet():
    assert _diagnose({"Node Type": "Hash", "Hash Batches": 1}).diagnostics == []


def test_sort_on_disk_is_reported():
    v = _diagnose({"Node Type": "Sort", "Sort Space Type": "Disk", "Sort Method": "external merge", "Sort Space Used": 5240})
    assert v.diagnostics[0]["topic"] == "work_mem"
    assert "external merge" in v.diagnostics[0]["detail"] and "5.1 MB" in v.diagnostics[0]["detail"]


def test_sort_in_memory_is_quiet():
    assert _diagnose({"Node Type": "Sort", "Sort Space Type": "Memory", "Sort Space Used": 64}).diagnostics == []


def test_lossy_bitmap_is_reported():
    v = _diagnose({"Node Type": "Bitmap Heap Scan", "Lossy Heap Blocks": 1565, "Rows Removed by Index Recheck": 16139})
    assert v.diagnostics[0]["topic"] == "work_mem"
    assert "1,565 lossy heap blocks" in v.diagnostics[0]["detail"]
    assert "16,139 rows rechecked" in v.diagnostics[0]["detail"]


def test_unselective_filter_is_reported():
    v = _diagnose({"Node Type": "Seq Scan", "Rows Removed by Filter": 200000, "Actual Rows": 0})
    assert v.diagnostics[0]["topic"] == "index"
    assert "discarded 200,000 rows" in v.diagnostics[0]["detail"]


def test_filter_below_the_threshold_is_quiet():
    # A filter that throws away few rows, or few relative to what it keeps, is
    # ordinary and must not produce a warning.
    assert _diagnose({"Node Type": "Seq Scan", "Rows Removed by Filter": 999, "Actual Rows": 0}).diagnostics == []
    assert _diagnose({"Node Type": "Seq Scan", "Rows Removed by Filter": 5000, "Actual Rows": 5000}).diagnostics == []


def test_filter_counts_are_multiplied_by_loops():
    v = _diagnose({"Node Type": "Index Scan", "Rows Removed by Filter": 2000, "Actual Rows": 1, "Actual Loops": 5})
    assert "10,000 rows" in v.diagnostics[0]["detail"]


def test_heap_fetches_point_at_vacuum():
    v = _diagnose({"Node Type": "Index Only Scan", "Heap Fetches": 4000})
    assert v.diagnostics[0]["topic"] == "vacuum"
    assert "4,000 heap fetches" in v.diagnostics[0]["detail"]


def test_high_loop_count_is_reported_once():
    v = _diagnose({"Node Type": "Index Scan", "Actual Loops": 2999})
    assert [d["topic"] for d in v.diagnostics] == ["plan"]
    assert "2,999 times" in v.diagnostics[0]["detail"]


def test_materialize_is_exempt_from_the_loop_warning():
    # Materialize and friends exist to be re-read cheaply; a high loop count
    # there is the point, not a problem.
    assert _diagnose({"Node Type": "Materialize", "Actual Loops": 5000}).diagnostics == []


def test_missing_parallel_workers_are_reported():
    v = _diagnose({"Node Type": "Gather", "Workers Planned": 4, "Workers Launched": 1})
    assert v.diagnostics[0]["topic"] == "parallel"
    assert "1 of 4" in v.diagnostics[0]["detail"]


def test_all_workers_launched_is_quiet():
    assert _diagnose({"Node Type": "Gather", "Workers Planned": 2, "Workers Launched": 2}).diagnostics == []


def test_a_healthy_plan_says_nothing():
    v = Visualizer(100, color=False, summary=True)
    v.load(_plan())
    assert v.diagnostics == []
    assert "Diagnostics:" not in v.get_list()


def test_diagnostics_follow_the_summary_setting():
    node = {"Node Type": "Hash", "Hash Batches": 8}
    off = Visualizer(100, color=False, summary=False)
    off.load(_wrap(dict(node)))
    assert "spill disk" not in off.get_list()
    on = Visualizer(100, color=False, summary=True)
    on.load(_wrap(dict(node)))
    assert "spill disk" in on.get_list()


def test_the_node_is_named_in_the_summary_block():
    v = _diagnose({
        "Node Type": "Seq Scan",
        "Relation Name": "orders",
        "Schema": "public",
        "Rows Removed by Filter": 50000,
        "Actual Rows": 10,
    })
    assert v.diagnostics[0]["label"] == "Seq Scan on public.orders"
    assert "node: Seq Scan on public.orders" in v.get_list()


# ---------------------------------------------------------------------------
# Explain mode and a user-written EXPLAIN
# ---------------------------------------------------------------------------


def test_own_explain_is_not_prefixed_again():
    """With F5 on, prepending to a statement that already is an EXPLAIN
    produced "EXPLAIN (...) explain (...)", which the server rejects."""
    sql = PGExecute.explain_mode_sql("explain (analyze, verbose, wal) select 1")
    assert sql.lower().count("explain") == 1
    assert "verbose" in sql and "wal" in sql


def test_own_explain_keeps_its_options_and_gains_what_is_needed():
    sql = PGExecute.explain_mode_sql(
        "explain (analyze, verbose, costs, buffers, timing, summary, settings, wal, memory, serialize ) select 1"
    )
    for kept in ("verbose", "buffers", "timing", "summary", "settings", "wal", "memory", "serialize"):
        assert kept in sql, kept
    assert "FORMAT JSON" in sql


def test_plain_statement_still_gets_the_prefix():
    assert PGExecute.explain_mode_sql("select 1") == PGExecute.explain_prefix() + "select 1"


def test_analyze_and_costs_are_added_when_missing():
    sql = PGExecute.explain_mode_sql("explain (verbose) select 1")
    assert "ANALYZE" in sql and "COSTS" in sql


def test_costs_off_is_overridden_because_the_visualizer_needs_them():
    sql = PGExecute.explain_mode_sql("explain (costs off) select 1")
    assert "COSTS ON" in sql and "costs off" not in sql.lower()


def test_a_format_the_visualizer_cannot_read_is_left_alone():
    for fmt in ("text", "yaml", "xml"):
        original = "explain (format %s, analyze) select 1" % fmt
        assert PGExecute.explain_mode_sql(original) == original


def test_legacy_explain_without_parentheses_is_left_alone():
    for original in ("explain analyze select 1", "explain select 1"):
        assert PGExecute.explain_mode_sql(original) == original


def test_unbalanced_parentheses_are_left_for_the_server_to_reject():
    original = "explain (analyze select 1"
    assert PGExecute.explain_mode_sql(original) == original


def test_formatter_falls_back_to_the_raw_plan_when_it_is_not_json():
    rows = [("Seq Scan on t  (cost=0.00..1.00 rows=1 width=4)",), ("Planning Time: 0.1 ms",)]
    out = list(ExplainOutputFormatter(100, summary=True).format_output(iter(rows), None))
    assert "Seq Scan on t" in "\n".join(out)


def test_visualizer_survives_a_plan_without_costs_or_timings():
    v = Visualizer(100, color=False, summary=True)
    v.load({"Plan": {"Node Type": "Seq Scan", "Plans": []}, "Execution Time": 1.0})
    assert "Seq Scan" in v.get_list()


# ---------------------------------------------------------------------------
# Query statistics, in the style of explain.depesz.com
# ---------------------------------------------------------------------------


def _summary_of(visualizer):
    """Just the summary block: the tree above it also mentions node types."""
    lines = visualizer.get_list().splitlines()
    return lines[lines.index("Summary") :]


def _io_plan(**buffers):
    node = {
        "Node Type": "Seq Scan",
        "Relation Name": "t",
        "Schema": "public",
        "Actual Total Time": 5.0,
        "Actual Loops": 1,
        "Total Cost": 10,
        "Plan Rows": 1,
        "Actual Rows": 1,
        "Plans": [],
    }
    node.update(buffers)
    return {"Plan": node, "Planning Time": 0.1, "Execution Time": 10.0}


def test_io_totals_use_eight_kilobyte_pages():
    v = Visualizer(100, color=False, summary=True)
    # 1280 blocks * 8 kB = 10 MB
    v.load(_io_plan(**{"Shared Read Blocks": 1280, "Temp Written Blocks": 128}))
    assert v.io_totals["read"] == 1280 and v.io_totals["written"] == 128
    out = v.get_list()
    assert "I/O:" in out and "read 10.0 MB" in out and "wrote 1.0 MB" in out


def test_temp_io_is_called_out_separately():
    v = Visualizer(100, color=False, summary=True)
    v.load(_io_plan(**{"Temp Read Blocks": 256, "Temp Written Blocks": 256}))
    assert "temp 2.0 MB read / 2.0 MB written" in v.get_list()


def test_cache_hits_alone_do_not_print_an_io_line():
    # Reading everything from shared buffers is not disk traffic.
    v = Visualizer(100, color=False, summary=True)
    v.load(_io_plan(**{"Shared Hit Blocks": 5000}))
    assert "I/O:" not in v.get_list()


def test_node_types_are_counted_and_totalled():
    v = Visualizer(100, color=False, summary=True)
    v.load(_plan())
    summary = _summary_of(v)
    assert any("By node type" in line for line in summary)
    # two Seq Scans in the fixture plan, totalling 120 + 18 ms
    row = next(line for line in summary[summary.index("  By node type:") :] if "Seq Scan" in line)
    assert row.split() == ["Seq", "Scan", "2", "138.00", "ms", "(86%)"]


def test_table_rows_break_down_by_scan_type():
    """A table read twice by different scans shows each one underneath."""
    plan = {
        "Plan": {
            "Node Type": "Append",
            "Actual Total Time": 30.0,
            "Actual Loops": 1,
            "Total Cost": 300,
            "Plan Rows": 1,
            "Actual Rows": 1,
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "t",
                    "Schema": "public",
                    "Actual Total Time": 20.0,
                    "Actual Loops": 1,
                    "Total Cost": 200,
                    "Plan Rows": 1,
                    "Actual Rows": 1,
                    "Plans": [],
                },
                {
                    "Node Type": "Index Scan",
                    "Relation Name": "t",
                    "Schema": "public",
                    "Actual Total Time": 5.0,
                    "Actual Loops": 1,
                    "Total Cost": 50,
                    "Plan Rows": 1,
                    "Actual Rows": 1,
                    "Plans": [],
                },
            ],
        },
        "Planning Time": 0.1,
        "Execution Time": 30.0,
    }
    v = Visualizer(100, color=False, summary=True)
    v.load(plan)
    summary = _summary_of(v)
    rows = summary[summary.index("  By table:") :]
    assert rows[1].split() == ["public.t", "2", "25.00", "ms", "(83%)"]  # both scans, added up
    assert rows[2].split() == ["Seq", "Scan", "1", "20.00", "ms"]
    assert rows[3].split() == ["Index", "Scan", "1", "5.00", "ms"]
