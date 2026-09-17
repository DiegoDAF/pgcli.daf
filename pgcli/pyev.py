import textwrap
import re
from click import style as color

DESCRIPTIONS = {
    "Append": "Used in a UNION to merge multiple record sets by appending them together.",
    "Limit": "Returns a specified number of rows from a record set.",
    "Sort": "Sorts a record set based on the specified sort key.",
    "Nested Loop": "Merges two record sets by looping through every record in the first set and trying to find a match in the second set. All matching records are returned.",
    "Merge Join": "Merges two record sets by first sorting them on a join key.",
    "Hash": "Generates a hash table from the records in the input recordset. Hash is used by Hash Join.",
    "Hash Join": "Joins to record sets by hashing one of them (using a Hash Scan).",
    "Aggregate": "Groups records together based on a GROUP BY or aggregate function (e.g. sum()).",
    "Hashaggregate": "Groups records together based on a GROUP BY or aggregate function (e.g. sum()). Hash Aggregate uses a hash to first organize the records by a key.",
    "Sequence Scan": "Finds relevant records by sequentially scanning the input record set. When reading from a table, Seq Scans (unlike Index Scans) perform a single read operation (only the table is read).",
    "Seq Scan": "Finds relevant records by sequentially scanning the input record set. When reading from a table, Seq Scans (unlike Index Scans) perform a single read operation (only the table is read).",
    "Index Scan": "Finds relevant records based on an Index. Index Scans perform 2 read operations: one to read the index and another to read the actual value from the table.",
    "Index Only Scan": "Finds relevant records based on an Index. Index Only Scans perform a single read operation from the index and do not read from the corresponding table.",
    "Bitmap Heap Scan": "Searches through the pages returned by the Bitmap Index Scan for relevant rows.",
    "Bitmap Index Scan": "Uses a Bitmap Index (index which uses 1 bit per page) to find all relevant pages. Results of this node are fed to the Bitmap Heap Scan.",
    "CTEScan": "Performs a sequential scan of Common Table Expression (CTE) query results. Note that results of a CTE are materialized (calculated and temporarily stored).",
    "ProjectSet": "ProjectSet appears when the SELECT or ORDER BY clause of the query.  They basically just execute the set-returning function(s) for each tuple until none of the functions return any more records.",
    "Result": "Returns result",
}


# Thresholds for the plan diagnostics. They are deliberately conservative: a
# warning that fires on a healthy plan trains you to ignore all of them.
DIAG_MIN_DISCARDED_ROWS = 1000  # rows a filter must throw away before it is worth mentioning
DIAG_DISCARD_RATIO = 10  # ...and how many times more than it keeps
DIAG_MIN_LOOPS = 1000  # loops on the inner side of a join before it reads as a risk
DIAG_MIN_HEAP_FETCHES = 1000  # heap fetches in an index-only scan before blaming the visibility map


class Visualizer:
    def __init__(self, terminal_width=100, color=True, summary=False):
        self.color = color
        self.terminal_width = terminal_width
        self.string_lines = []
        self.summary = summary
        self.node_stats = []
        self.diagnostics = []

    def load(self, explain_dict):
        self.plan = explain_dict.pop("Plan")
        self.explain = explain_dict
        self.node_stats = []
        self.diagnostics = []
        self.process_all()
        self.generate_lines()

    def process_all(self):
        self.plan = self.process_plan(self.plan)
        self.plan = self.calculate_outlier_nodes(self.plan)
        self.collect_node_stats(self.plan)

    def collect_node_stats(self, plan):
        """Flatten the plan tree into per-node stats for the summary section.

        ``Actual Duration`` is already the node's EXCLUSIVE time (this node
        minus its children, times loops), computed in ``calculate_actuals``.
        """
        label = plan.get("Node Type", "?")
        relation = plan.get("Relation Name")
        if relation:
            # Schema is only present with EXPLAIN VERBOSE; without it, naming
            # the table alone beats printing "?.orders".
            schema = plan.get("Schema")
            label = "%s on %s" % (label, ("%s.%s" % (schema, relation)) if schema else relation)
        elif plan.get("CTE Name"):
            label = "%s %s" % (label, plan.get("CTE Name"))
        self.diagnose_node(plan, label)
        self.node_stats.append({
            "label": label,
            "relation": ((("%s.%s" % (plan["Schema"], relation)) if plan.get("Schema") else relation) if relation else None),
            "duration": plan.get("Actual Duration", 0) or 0,
            "rows": plan.get("Actual Rows", 0),
            "est_factor": plan.get("Planner Row Estimate Factor", 0) or 0,
            "est_dir": plan.get("Planner Row Estimate Direction", ""),
        })
        for child in plan.get("Plans", []):
            self.collect_node_stats(child)

    def diagnose_node(self, plan, label):
        """Flag the plan problems that the numbers state outright.

        Each finding is attached to its own node (``Diagnostics`` holds the
        short inline tags) and collected for the summary block. Counters that
        accumulate up the tree, such as the temp blocks, are read on the node
        that causes the spill rather than on its parents, so a single problem
        is reported once.
        """
        found = []

        def add(tag, topic, detail):
            found.append(tag)
            self.diagnostics.append({"tag": tag, "topic": topic, "label": label, "detail": detail})

        node_type = plan.get("Node Type", "")
        rows = plan.get("Actual Rows", 0) or 0
        loops = plan.get("Actual Loops", 1) or 1

        # Hash join that did not fit in work_mem: the build side was split into
        # batches and written out to temporary files.
        batches = plan.get("Hash Batches", 1) or 1
        if node_type == "Hash" and batches > 1:
            peak = plan.get("Peak Memory Usage")
            detail = "spilled to %s batches" % self.intcomma(batches)
            if plan.get("Original Hash Batches", batches) != batches:
                detail += " (planned %s)" % self.intcomma(plan["Original Hash Batches"])
            if peak:
                detail += ", peak memory %s" % self.kb_to_string(peak)
            add("spill disk", "work_mem", detail)

        # Sort that exceeded work_mem and fell back to an on-disk merge.
        if plan.get("Sort Space Type") == "Disk":
            add(
                "sort disk",
                "work_mem",
                "%s used %s of disk" % (plan.get("Sort Method", "sort"), self.kb_to_string(plan.get("Sort Space Used", 0))),
            )

        # Bitmap that ran out of memory and degraded to page granularity, which
        # forces a recheck of every row on those pages.
        lossy = plan.get("Lossy Heap Blocks", 0) or 0
        if lossy > 0:
            detail = "%s lossy heap blocks" % self.intcomma(lossy)
            recheck = plan.get("Rows Removed by Index Recheck", 0) or 0
            if recheck:
                detail += ", %s rows rechecked and dropped" % self.intcomma(recheck)
            add("lossy bitmap", "work_mem", detail)

        # A filter doing the work an index should be doing.
        removed = plan.get("Rows Removed by Filter", 0) or 0
        if removed >= DIAG_MIN_DISCARDED_ROWS and removed >= DIAG_DISCARD_RATIO * max(rows, 1):
            add(
                "bad filter",
                "index",
                "read and discarded %s rows to keep %s" % (self.intcomma(removed * loops), self.intcomma(rows * loops)),
            )

        # Index-only scan that still had to visit the heap: the visibility map
        # is behind, which is what VACUUM maintains.
        fetches = plan.get("Heap Fetches", 0) or 0
        if fetches >= DIAG_MIN_HEAP_FETCHES:
            add("heap fetches", "vacuum", "%s heap fetches in an index-only scan" % self.intcomma(fetches))

        # Inner side of a join re-executed many times.
        if loops >= DIAG_MIN_LOOPS and node_type not in ("Hash", "Sort", "Materialize"):
            add("high loops", "plan", "executed %s times" % self.intcomma(loops))

        # The planner asked for parallel workers and did not get them all.
        planned = plan.get("Workers Planned", 0) or 0
        launched = plan.get("Workers Launched", 0) or 0
        if planned and launched < planned:
            add("few workers", "parallel", "got %d of %d requested workers" % (launched, planned))

        if found:
            plan["Diagnostics"] = found

    def kb_to_string(self, value):
        """Render a size that PostgreSQL reports in kilobytes."""
        try:
            value = float(value)
        except (TypeError, ValueError):
            return "?"
        if value >= 1024 * 1024:
            return "%.1f GB" % (value / 1024 / 1024)
        if value >= 1024:
            return "%.1f MB" % (value / 1024)
        return "%d kB" % value

    #
    def process_plan(self, plan):
        plan = self.calculate_planner_estimate(plan)
        plan = self.calculate_actuals(plan)
        self.calculate_maximums(plan)
        #
        for index in range(len(plan.get("Plans", []))):
            _plan = plan["Plans"][index]
            plan["Plans"][index] = self.process_plan(_plan)
        return plan

    def prefix_format(self, v):
        if self.color:
            return color(v, fg="bright_black")
        return v

    def tag_format(self, v):
        if self.color:
            return color(v, fg="white", bg="red")
        return v

    def muted_format(self, v):
        if self.color:
            return color(v, fg="bright_black")
        return v

    def bold_format(self, v):
        if self.color:
            return color(v, fg="white")
        return v

    def good_format(self, v):
        if self.color:
            return color(v, fg="green")
        return v

    def warning_format(self, v):
        if self.color:
            return color(v, fg="yellow")
        return v

    def critical_format(self, v):
        if self.color:
            return color(v, fg="red")
        return v

    def output_format(self, v):
        if self.color:
            return color(v, fg="cyan")
        return v

    def calculate_planner_estimate(self, plan):
        plan["Planner Row Estimate Factor"] = 0
        plan["Planner Row Estimate Direction"] = "Under"

        # A plan can arrive without these (EXPLAIN without COSTS or without
        # ANALYZE); treat them as zero instead of raising KeyError.
        plan.setdefault("Plan Rows", 0)
        plan.setdefault("Actual Rows", 0)
        if plan["Plan Rows"] == plan["Actual Rows"]:
            return plan

        if plan["Plan Rows"] != 0:
            plan["Planner Row Estimate Factor"] = plan["Actual Rows"] / plan["Plan Rows"]

        if plan["Planner Row Estimate Factor"] < 10:
            plan["Planner Row Estimate Factor"] = 0
            plan["Planner Row Estimate Direction"] = "Over"
            if plan["Actual Rows"] != 0:
                plan["Planner Row Estimate Factor"] = plan["Plan Rows"] / plan["Actual Rows"]
        return plan

    #
    def calculate_actuals(self, plan):
        plan.setdefault("Actual Total Time", 0)
        plan.setdefault("Total Cost", 0)
        plan.setdefault("Actual Loops", 1)
        plan["Actual Duration"] = plan["Actual Total Time"]
        plan["Actual Cost"] = plan["Total Cost"]

        for child in plan.get("Plans", []):
            if child["Node Type"] != "CTEScan":
                plan["Actual Duration"] = plan["Actual Duration"] - child.get("Actual Total Time", 0)
                plan["Actual Cost"] = plan["Actual Cost"] - child.get("Total Cost", 0)

        if plan["Actual Cost"] < 0:
            plan["Actual Cost"] = 0

        plan["Actual Duration"] = plan["Actual Duration"] * plan["Actual Loops"]
        return plan

    def calculate_outlier_nodes(self, plan):
        plan["Costliest"] = plan["Actual Cost"] == self.explain["Max Cost"]
        plan["Largest"] = plan["Actual Rows"] == self.explain["Max Rows"]
        plan["Slowest"] = plan["Actual Duration"] == self.explain["Max Duration"]

        for index in range(len(plan.get("Plans", []))):
            _plan = plan["Plans"][index]
            plan["Plans"][index] = self.calculate_outlier_nodes(_plan)
        return plan

    def calculate_maximums(self, plan):
        if not self.explain.get("Max Rows"):
            self.explain["Max Rows"] = plan["Actual Rows"]
        elif self.explain.get("Max Rows") < plan["Actual Rows"]:
            self.explain["Max Rows"] = plan["Actual Rows"]

        if not self.explain.get("Max Cost"):
            self.explain["Max Cost"] = plan["Actual Cost"]
        elif self.explain.get("Max Cost") < plan["Actual Cost"]:
            self.explain["Max Cost"] = plan["Actual Cost"]

        if not self.explain.get("Max Duration"):
            self.explain["Max Duration"] = plan["Actual Duration"]
        elif self.explain.get("Max Duration") < plan["Actual Duration"]:
            self.explain["Max Duration"] = plan["Actual Duration"]

        if not self.explain.get("Total Cost"):
            self.explain["Total Cost"] = plan["Actual Cost"]
        elif self.explain.get("Total Cost") < plan["Actual Cost"]:
            self.explain["Total Cost"] = plan["Actual Cost"]

    #
    def duration_to_string(self, value):
        if value < 1:
            return self.good_format("<1 ms")
        elif value < 100:
            return self.good_format("%.2f ms" % value)
        elif value < 1000:
            return self.warning_format("%.2f ms" % value)
        elif value < 60000:
            return self.critical_format(
                "%.2f s" % (value / 1000.0),
            )
        else:
            return self.critical_format(
                "%.2f m" % (value / 60000.0),
            )

    # }
    #
    def format_details(self, plan):
        details = []

        if plan.get("Scan Direction"):
            details.append(plan["Scan Direction"])

        if plan.get("Strategy"):
            details.append(plan["Strategy"])

        if len(details) > 0:
            return self.muted_format(" [%s]" % ", ".join(details))

        return ""

    def format_tags(self, plan):
        tags = []

        if plan["Slowest"]:
            tags.append(self.tag_format("slowest"))
        if plan["Costliest"]:
            tags.append(self.tag_format("costliest"))
        if plan["Largest"]:
            tags.append(self.tag_format("largest"))
        if plan.get("Planner Row Estimate Factor", 0) >= 100:
            tags.append(self.tag_format("bad estimate"))
        if self.summary:
            # Diagnostics ride with the summary: no new knob, and a user who
            # turned the summary off keeps the plain tree.
            for tag in plan.get("Diagnostics", []):
                tags.append(self.critical_format(tag))

        return " ".join(tags)

    def get_terminator(self, index, plan):
        if index == 0:
            if len(plan.get("Plans", [])) == 0:
                return "⌡► "
            else:
                return "├►  "
        else:
            if len(plan.get("Plans", [])) == 0:
                return "   "
            else:
                return "│  "

    def wrap_string(self, line, width):
        if width == 0:
            return [line]
        return textwrap.wrap(line, width)

    def intcomma(self, value):
        sep = ","
        if not isinstance(value, str):
            value = int(value)

        orig = str(value)

        new = re.sub(r"^(-?\d+)(\d{3})", rf"\g<1>{sep}\g<2>", orig)
        if orig == new:
            return new
        else:
            return self.intcomma(new)

    def output_fn(self, current_prefix, string):
        return "%s%s" % (self.prefix_format(current_prefix), string)

    def create_lines(self, plan, prefix, depth, width, last_child):
        current_prefix = prefix
        self.string_lines.append(self.output_fn(current_prefix, self.prefix_format("│")))

        joint = "├"
        if last_child:
            joint = "└"
        #
        self.string_lines.append(
            self.output_fn(
                current_prefix,
                "%s %s%s %s"
                % (
                    self.prefix_format(joint + "─⌠"),
                    self.bold_format(plan["Node Type"]),
                    self.format_details(plan),
                    self.format_tags(plan),
                ),
            )
        )
        #
        if last_child:
            prefix += "  "
        else:
            prefix += "│ "

        current_prefix = prefix + "│ "

        cols = width - len(current_prefix)

        for line in self.wrap_string(
            DESCRIPTIONS.get(plan["Node Type"], "Not found : %s" % plan["Node Type"]),
            cols,
        ):
            self.string_lines.append(self.output_fn(current_prefix, "%s" % self.muted_format(line)))
        #
        if plan.get("Actual Duration"):
            self.string_lines.append(
                self.output_fn(
                    current_prefix,
                    "○ %s %s (%.0f%%)"
                    % (
                        "Duration:",
                        self.duration_to_string(plan["Actual Duration"]),
                        (plan["Actual Duration"] / self.explain["Execution Time"] * 100) if self.explain.get("Execution Time") else 0,
                    ),
                )
            )

        self.string_lines.append(
            self.output_fn(
                current_prefix,
                "○ %s %s (%.0f%%)"
                % (
                    "Cost:",
                    self.intcomma(plan["Actual Cost"]),
                    (plan["Actual Cost"] / self.explain["Total Cost"] * 100) if self.explain.get("Total Cost") else 0,
                ),
            )
        )

        self.string_lines.append(
            self.output_fn(
                current_prefix,
                "○ %s %s" % ("Rows:", self.intcomma(plan["Actual Rows"])),
            )
        )

        current_prefix = current_prefix + "  "

        if plan.get("Join Type"):
            self.string_lines.append(
                self.output_fn(
                    current_prefix,
                    "%s %s" % (plan["Join Type"], self.muted_format("join")),
                )
            )

        if plan.get("Relation Name"):
            self.string_lines.append(
                self.output_fn(
                    current_prefix,
                    "%s %s.%s"
                    % (
                        self.muted_format("on"),
                        plan.get("Schema", "unknown"),
                        plan["Relation Name"],
                    ),
                )
            )

        if plan.get("Index Name"):
            self.string_lines.append(
                self.output_fn(
                    current_prefix,
                    "%s %s" % (self.muted_format("using"), plan["Index Name"]),
                )
            )

        if plan.get("Index Condition"):
            self.string_lines.append(
                self.output_fn(
                    current_prefix,
                    "%s %s" % (self.muted_format("condition"), plan["Index Condition"]),
                )
            )

        if plan.get("Filter"):
            self.string_lines.append(
                self.output_fn(
                    current_prefix,
                    "%s %s %s"
                    % (
                        self.muted_format("filter"),
                        plan["Filter"],
                        self.muted_format("[-%s rows]" % self.intcomma(plan["Rows Removed by Filter"])),
                    ),
                )
            )

        if plan.get("Hash Condition"):
            self.string_lines.append(
                self.output_fn(
                    current_prefix,
                    "%s %s" % (self.muted_format("on"), plan["Hash Condition"]),
                )
            )

        if plan.get("CTE Name"):
            self.string_lines.append(self.output_fn(current_prefix, "CTE %s" % plan["CTE Name"]))

        if plan.get("Planner Row Estimate Factor") != 0:
            self.string_lines.append(
                self.output_fn(
                    current_prefix,
                    "%s %sestimated %s %.2fx"
                    % (
                        self.muted_format("rows"),
                        plan["Planner Row Estimate Direction"],
                        self.muted_format("by"),
                        plan["Planner Row Estimate Factor"],
                    ),
                )
            )

        current_prefix = prefix

        if len(plan.get("Output", [])) > 0:
            for index, line in enumerate(self.wrap_string(" + ".join(plan["Output"]), cols)):
                self.string_lines.append(
                    self.output_fn(
                        current_prefix,
                        self.prefix_format(self.get_terminator(index, plan)) + self.output_format(line),
                    )
                )

        for index, nested_plan in enumerate(plan.get("Plans", [])):
            self.create_lines(nested_plan, prefix, depth + 1, width, index == len(plan["Plans"]) - 1)

    def generate_lines(self):
        # EXPLAIN without ANALYZE has no timings, and without COSTS no cost:
        # report what is there instead of raising KeyError.
        self.string_lines = [
            "○ Total Cost: %s" % self.intcomma(self.explain.get("Total Cost", 0)),
            "○ Planning Time: %s" % self.duration_to_string(self.explain.get("Planning Time", 0)),
            "○ Execution Time: %s" % self.duration_to_string(self.explain.get("Execution Time", 0)),
            self.prefix_format("┬"),
        ]
        self.create_lines(
            self.plan,
            "",
            0,
            self.terminal_width,
            len(self.plan.get("Plans", [])) == 1,
        )
        if self.summary:
            self.generate_summary()

    def severity_format(self, pct, text):
        """Color a value by its share of total execution time (pgAdmin-style)."""
        if not self.color:
            return text
        if pct < 10:
            return color(text, fg="green")
        elif pct < 50:
            return color(text, fg="yellow")
        return color(text, fg="red")

    def generate_summary(self):
        """Append a compact analysis summary after the plan tree: the slowest
        nodes (by exclusive time), time grouped by relation, and the worst
        planner row-estimate misses. All values reuse metrics already computed
        for the tree."""
        exec_time = self.explain.get("Execution Time") or 0
        if not self.node_stats or exec_time <= 0:
            return

        def pct(d):
            return (d / exec_time) * 100 if exec_time else 0

        lines = ["", self.bold_format("Summary")]

        # Slowest nodes by exclusive time.
        slowest = sorted(self.node_stats, key=lambda n: n["duration"], reverse=True)[:5]
        lines.append(self.muted_format("  Slowest nodes (exclusive time):"))
        for n in slowest:
            p = pct(n["duration"])
            lines.append(
                "    %-45s %s %s"
                % (
                    n["label"][:45],
                    self.duration_to_string(n["duration"]),
                    self.severity_format(p, "(%.0f%%)" % p),
                )
            )

        # Time grouped by relation.
        by_rel: dict = {}
        for n in self.node_stats:
            if n["relation"]:
                agg = by_rel.setdefault(n["relation"], {"duration": 0.0, "nodes": 0})
                agg["duration"] += n["duration"]
                agg["nodes"] += 1
        if by_rel:
            lines.append(self.muted_format("  Time by relation:"))
            for rel, agg in sorted(by_rel.items(), key=lambda kv: kv[1]["duration"], reverse=True):
                p = pct(agg["duration"])
                lines.append(
                    "    %-35s %s %s  %d node(s)"
                    % (
                        rel[:35],
                        self.duration_to_string(agg["duration"]),
                        self.severity_format(p, "(%.0f%%)" % p),
                        agg["nodes"],
                    )
                )

        # Worst planner row-estimate misses (factor >= 10).
        misses = sorted(
            (n for n in self.node_stats if n["est_factor"] and n["est_factor"] >= 10),
            key=lambda n: n["est_factor"],
            reverse=True,
        )[:5]
        if misses:
            lines.append(self.muted_format("  Planner estimate misses:"))
            for n in misses:
                lines.append("    %-45s %s-estimated %.0fx" % (n["label"][:45], (n["est_dir"] or "mis").lower(), n["est_factor"]))

        # What the numbers say outright, grouped by what you would change.
        if self.diagnostics:
            lines.append(self.muted_format("  Diagnostics:"))
            for topic in ("work_mem", "index", "vacuum", "parallel", "plan"):
                for d in (x for x in self.diagnostics if x["topic"] == topic):
                    # Pad before coloring: the escape codes count as characters.
                    lines.append("    %s %s" % (self.critical_format("%-9s" % topic), d["detail"]))
                    lines.append("    %-9s %s" % ("", self.muted_format("node: %s" % d["label"][:60])))

        self.string_lines.extend(lines)

    def get_list(self):
        return "\n".join(self.string_lines)

    def print(self):
        for lin in self.string_lines:
            print(lin)
