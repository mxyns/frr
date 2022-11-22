import functools
import re

qmem_module_name_regex = re.compile("--- qmem (.+) ---")


def _split_show_memory_bgpd_log(output):
    per_module_memusage = dict()
    lines = output.split("\n")
    curr_module = None
    for i, line in enumerate(lines):
        regmatches = qmem_module_name_regex.search(line)
        if regmatches is not None:
            curr_module = regmatches.groups()[0].split(" ")[0].lower()
            per_module_memusage[curr_module] = list()
            continue

        if curr_module is not None:
            per_module_memusage[curr_module].append(line)

    return {k: _cleanup_lines(v) for k, v in per_module_memusage.items()}


def _cleanup_lines(lines):
    return [line.strip() for line in lines if not line.startswith("---") and len(line) > 0]


def _get_column(tab, col):
    return [re.split("\\s+", line)[col] for line in tab]


def _get_column_titles(tab):
    return [" ".join(re.split("\\s+", line)[:_get_col_index("title") + 1] or ['', '']).strip() for line in tab]


def _get_total_sum(col, avoid=None):
    if avoid is None:
        avoid = ["Total"]
    return sum(map(lambda x: 0 if x in avoid else int(x), col))


def get_modules_total_and_logs(output):
    per_module_memusage_logs = _split_show_memory_bgpd_log(output)
    return {module: {"total": _get_total_sum(_get_column(module_memusage_logs, _get_col_index("total"))),
                     "details": module_memusage_logs} for module, module_memusage_logs in
            per_module_memusage_logs.items()}


def get_modules_datatypes_and_sizes(per_module_memusage):
    return {module: [list(zip(_get_column_titles(values['details']),
                             _get_column(values['details'], _get_col_index("size")))) for values in series] for module, series in
            per_module_memusage.items()}


def _get_series_of_column(module_memuse, column):
    titles = functools.reduce(lambda s1, s2: s1.union(s2),
                              [set(_get_column_titles(memuse.get("details")[1:] or [])) for memuse in
                               module_memuse])
    default_vals = {k: 0 for k in titles}

    result = [default_vals | dict(zip(_get_column_titles(detail)[1:],
                                      list(map(int, _get_column(detail, _get_col_index(column))[1:])))) if len(
        detail) > 0 else default_vals for detail in
              [[] if x == [] else x for x in map(lambda x: x.get("details"), module_memuse)]]

    return titles, result, default_vals


def _get_column_types():
    return {
        "max_bytes": -1,
        "max_count": -2,
        "total": -3,
        "size": -4,
        "current_count": -5,
        "title": -7
    }


def _get_col_index(col):
    return _get_column_types().get(col)


def _get_column_display_name(col_raw_name):
    return {
        "max_bytes": "Maximum Size (bytes)",
        "max_count": "Maximum Count",
        "total": "Total (bytes)",
        "size": "Size (bytes)",
        "current_count": "Current Count",
        "title": "Datatype"
    }.get(col_raw_name)


def _get_details_col_data(details, line, col):
    col = _get_col_index(col)

    nxt = next(filter(lambda x: line in x, details), None)
    return 0 if nxt is None else int(_get_column([nxt], col)[0])
