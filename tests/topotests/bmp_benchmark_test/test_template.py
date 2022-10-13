#!/usr/bin/env python

"""
<template>.py: Test <template>.
"""

import sys
import pytest

from lib.topogen import Topogen, TopoRouter
from lib.topolog import logger

# TODO: select markers based on daemons used during test
# pytest module level markers
pytestmark = [
    # pytest.mark.babeld,
    # pytest.mark.bfdd,
    pytest.mark.bgpd,
    # pytest.mark.eigrpd,
    # pytest.mark.isisd,
    # pytest.mark.ldpd,
    # pytest.mark.nhrpd,
    # pytest.mark.ospf6d,
    # pytest.mark.ospfd,
    # pytest.mark.pathd,
    # pytest.mark.pbrd,
    # pytest.mark.pimd,
    # pytest.mark.ripd,
    # pytest.mark.ripngd,
    # pytest.mark.sharpd,
    # pytest.mark.staticd,
    # pytest.mark.vrrpd,
]

# Function we pass to Topogen to create the topology
def build_topo(tgen):
    "Build function"

    uut = tgen.add_router("uut")
    p1 = tgen.add_router("p1")

    tgen.add_link(uut, p1)


# New form of setup/teardown using pytest fixture
@pytest.fixture(scope="module")
def tgen(request):
    "Setup/Teardown the environment and provide tgen argument to tests"

    # This function initiates the topology build with Topogen...
    tgen = Topogen(build_topo, request.module.__name__)

    tgen.start_topology()

    router_list = tgen.routers()

    for rname, router in router_list.items():
        router.load_config(TopoRouter.RD_ZEBRA, "zebra.conf")
        router.load_config(TopoRouter.RD_BGP, "bgpd.conf")

    # Start and configure the router daemons
    tgen.start_router()

    # Provide tgen as argument to each test function
    yield tgen

    # Teardown after last test runs
    tgen.stop_topology()


# Fixture that executes before each test
@pytest.fixture(autouse=True)
def skip_on_failure(tgen):
    if tgen.routers_have_failure():
        pytest.skip("skipped because of previous test failure")


# ===================
# The tests functions
# ===================


def test_get_version(tgen):
    "Test the logs the FRR version"

    r1 = tgen.gears["uut"]
    version = r1.vtysh_cmd("show version")
    logger.info("FRR version is: " + version)


def test_connectivity(tgen):
    "Test the logs the FRR version"

    r1 = tgen.gears["uut"]
    r2 = tgen.gears["p1"]
    output = r1.cmd_raises("ping -c1 192.168.1.2")
    output = r2.cmd_raises("ping -c1 192.168.1.1")


@pytest.mark.xfail
def test_expect_failure(tgen):
    "A test that is current expected to fail but should be fixed"

    assert False, "Example of temporary expected failure that will eventually be fixed"


@pytest.mark.skip
def test_will_be_skipped(tgen):
    "A test that will be skipped"
    assert False


# Memory leak test template
def test_memory_leak(tgen):
    "Run the memory leak test and report results."

    if not tgen.is_memleak_enabled():
        pytest.skip("Memory leak test/report is disabled")

    tgen.report_memory_leaks()


if __name__ == "__main__":
    args = ["-s"] + sys.argv[1:]
    sys.exit(pytest.main(args))
