from pgcli.pyev import Visualizer
import json


"""Explain response output adapter"""


class ExplainOutputFormatter:
    def __init__(self, max_width, summary=False):
        self.max_width = max_width
        self.summary = summary

    def format_output(self, cur, headers, **output_kwargs):
        rows = list(cur)
        try:
            # An explain result is a single row holding the whole plan.
            [(data,)] = rows
            explain_list = json.loads(data)
        except (ValueError, TypeError):
            # Not JSON: the user ran their own EXPLAIN asking for text, yaml or
            # xml. Show exactly what the server sent instead of failing.
            for row in rows:
                yield "\n".join("" if value is None else str(value) for value in row)
            return
        visualizer = Visualizer(self.max_width, summary=self.summary)
        for explain in explain_list:
            visualizer.load(explain)
            yield visualizer.get_list()
