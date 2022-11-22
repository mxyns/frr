import re


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


def get_bgp_bmp_total_sum(output):
    bgpd_only, bmp_only = _split_bgp_log(output)
    bmp_totals = _get_column(bmp_only, -3)
    bgp_totals = _get_column(bgpd_only, -3)

    return (_get_total_sum(bgp_totals), bgpd_only), (_get_total_sum(bmp_totals), bmp_only)


def get_lmlogs_total(output):
    return _get_details_col_data(output, "lmlogs logging stacks", "total"), []


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
