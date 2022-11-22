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


def _split_bgp_log(output):
    bgpd_only = list()
    bmp_only = list()
    lines = output.split("\n")
    for i, line in enumerate(lines):
        if "qmem BMP" in line:
            bgpd_only = lines[13:i]
            bmp_only = lines[i + 1:]
            break

    return _cleanup_lines(bgpd_only), _cleanup_lines(bmp_only)


def _cleanup_lines(lines):
    return [line.strip() for line in lines if not line.startswith("---") and len(line) > 0]


def _get_column(tab, col):
    return [re.split("\\s+", line)[col] for line in tab]


def _get_column_titles(tab):
    return [" ".join(re.split("\\s+", line)[:-6] or ['', '']) for line in tab]


def _get_total_sum(col, avoid=None):
    if avoid is None:
        avoid = ["Total"]
    return sum(map(lambda x: 0 if x in avoid else int(x), col))


def get_modules_total_and_logs(output):
    per_module_memusage_logs = _split_show_memory_bgpd_log(output)
    return {module: (_get_total_sum(_get_column(module_memusage_logs, _get_col_index("total"))), module_memusage_logs) for module, module_memusage_logs in per_module_memusage_logs.items()}

def _get_column_types():
    return {
        "max_bytes": -1,
        "max_count": -2,
        "total": -3,
        "size": -4,
        "current_count": -5
    }


def _get_col_index(col):
    return _get_column_types().get(col)


def _get_column_display_name(col_raw_name):
    return {
        "max_bytes": "Maximum Size (bytes)",
        "max_count": "Maximum Count",
        "total": "Total (bytes)",
        "size": "SHOULDN'T BE PLOTTED",
        "current_count": "Current Count"
    }.get(col_raw_name)


def _get_details_col_data(details, line, col):
    col = _get_col_index(col)

    nxt = next(filter(lambda x: line in x, details), None)
    return 0 if nxt is None else int(_get_column([nxt], col)[0])
