from pgcli.packages.parseutils.meta import FunctionMetadata


def test_function_metadata_eq():
    f1 = FunctionMetadata("s", "f", ["x"], ["integer"], [], "int", False, False, False, False, None)
    f2 = FunctionMetadata("s", "f", ["x"], ["integer"], [], "int", False, False, False, False, None)
    f3 = FunctionMetadata("s", "g", ["x"], ["integer"], [], "int", False, False, False, False, None)
    assert f1 == f2
    assert f1 != f3
    assert not (f1 != f2)
    assert not (f1 == f3)
    assert hash(f1) == hash(f2)
    assert hash(f1) != hash(f3)


def test_fields_variadic_unnamed_args():
    # Functions with variadic/unnamed args normalize arg_names to None while
    # arg_modes stays truthy. fields() must not crash on zip(None, ...) and,
    # since there are no OUT/INOUT/TABLE params, must expose the function name
    # as the output column (e.g. 'labels(variadic text[]) RETURNS hstore').
    f = FunctionMetadata("public", "labels", None, ["text[]"], ["v"], "hstore", False, False, False, False, None)
    fields = f.fields()
    assert [(c.name, c.datatype) for c in fields] == [("labels", "hstore")]


def test_fields_named_input_only_args():
    # A function whose args are all input/named (no OUT/INOUT/TABLE) also has no
    # output parameters, so the function name is the output column.
    f = FunctionMetadata("public", "addone", ["n"], ["integer"], ["i"], "integer", False, False, False, False, None)
    fields = f.fields()
    assert [(c.name, c.datatype) for c in fields] == [("addone", "integer")]


def test_fields_out_params_listed():
    # When OUT/INOUT/TABLE params exist, those columns are returned (not the
    # function name fallback).
    f = FunctionMetadata(
        "public",
        "split",
        ["inp", "a", "b"],
        ["text", "text", "text"],
        ["i", "o", "o"],
        "record",
        False,
        False,
        False,
        False,
        None,
    )
    fields = f.fields()
    assert [(c.name, c.datatype) for c in fields] == [("a", "text"), ("b", "text")]


def test_function_metadata_fields_table_mode_without_arg_names():
    """A set-returning function declared with TABLE(...) but no argument names
    (upstream #1638 hits the same spot). Rather than offering nothing, fall
    back to the function name, the way the no-output-parameter case does."""
    f = FunctionMetadata("s", "f", None, ["int4", "text"], ["t", "t"], "record", False, False, True, False, None)
    assert [(c.name, c.datatype) for c in f.fields()] == [("f", "record")]
