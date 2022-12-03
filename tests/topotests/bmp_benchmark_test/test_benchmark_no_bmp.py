"""
<template>.py: Test <template>.
"""
import functools
import sys

import pytest
from lib.topogen import TopoRouter

# Import benchmark common functions module
from tests.topotests.bmp_benchmark_test import benchmark_common as benchcom

# Import mandatory functions for pytest
from tests.topotests.bmp_benchmark_test.benchmark_common import get_router_ids, skip_on_failure

# Import tests to execute in order of execution
from tests.topotests.bmp_benchmark_test.benchmark_common import \
    test_get_version, \
    test_show_runnning_configuration, \
    test_connectivity, \
    test_query_ram_usage, \
    test_run_scenario


@pytest.fixture(scope="module")
def tgen(request):
    """Setup/Teardown the environment and provide tgen argument to tests"""

    prefix_file = "prefixes/routeviews_prefixes_more.json"
    test_name = "no_bmp"

    # Run test initialization function
    tgen, init_result = benchcom.init_test(request, ctx={
        "config_loader": functools.partial(benchcom.default_config_loader, overrides={
            "uut": {
                TopoRouter.RD_BGP: {
                    "file": f"bgpd_{test_name}.conf",
                    "args": ""
                }
            },
            "prvdr1": {
                TopoRouter.RD_BGP: {
                    "file": f"bgpd_{test_name}.conf",
                    "args": ""
                }
            }
        }),
        "prefixes_input_file": prefix_file,
        "memusage_output_dir": f"%CWD%/out/logs/{test_name}/%rname%/"
    })

    # Provide tgen as argument to each test function
    yield tgen

    # Run test finish function
    benchcom.fini_test(tgen, ctx={
        "init_result": init_result,
        "logs_output_dir": f"%CWD%/out/logs/{test_name}/%rname%/"
    })


if __name__ == "__main__":
    args = ["-s"] + sys.argv[1:]
    sys.exit(pytest.main(args))
