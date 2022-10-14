#!/usr/bin/env python

"""
<template>.py: Test <template>.
"""
import os
import sys
import pytest
import threading
import json
import time

from lib.topogen import Topogen, TopoRouter
from lib.topolog import logger
from lib.bgp import verify_bgp_convergence_from_running_config

CWD = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(CWD, "../"))

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
    """Build function"""

    uut = tgen.add_router("uut")

    p1 = tgen.add_router("p1")
    tgen.add_link(uut, p1)

    vpn_ce1 = tgen.add_router("ce1")
    tgen.add_link(uut, vpn_ce1, "uut-eth1", "ce1-eth1")


# New form of setup/teardown using pytest fixture
def setup_router_vrf(router):
    vrf_config_path = "{}/{}/vrf.conf".format(CWD, router.name)
    if os.path.exists(vrf_config_path):
        with open(vrf_config_path) as vrf_config:
            for line in vrf_config.readlines():
                logger.info("adding vrf {}({}) and binding if {}".format(*line.split(" ")))
                router.cmd("/bin/bash {}/mkvrf {}".format(CWD, line.strip()))


@pytest.fixture(scope="module")
def tgen(request):
    """Setup/Teardown the environment and provide tgen argument to tests"""

    # This function initiates the topology build with Topogen...
    tgen = Topogen(build_topo, request.module.__name__)

    tgen.start_topology()

    router_list = tgen.routers()

    for rname, router in router_list.items():
        router.load_config(TopoRouter.RD_ZEBRA, "zebra.conf")
        router.load_config(TopoRouter.RD_BGP, "bgpd.conf")
        setup_router_vrf(router)

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


def test_show_runnning_configuration(tgen):
    for gear in tgen.gears:
        if isinstance(tgen.gears[gear], TopoRouter):
            tgen.gears[gear].vtysh_cmd("do show run")


def test_connectivity(tgen):
    """Test the logs the FRR version"""

    uut = tgen.gears["uut"]
    output = uut.cmd_raises("ip vrf show")
    logger.info("UUT Links: \n" + output)
    output = uut.cmd_raises("ip route show")
    logger.info("UUT Routes: \n" + output)
    output = uut.cmd_raises("ip link show")
    logger.info("UUT Links: \n" + output)

    p1 = tgen.gears["p1"]
    output = p1.cmd_raises("ip route show")
    logger.info("p1 Routes: \n" + output)
    output = uut.cmd_raises("ping -c1 10.0.0.2 -I uut-eth0")
    output = p1.cmd_raises("ping -c1 10.0.0.1")

    vpn_ce1 = tgen.gears["ce1"]
    output = vpn_ce1.cmd_raises("ip link show")
    logger.info("CE1 Links: \n" + output)
    output = vpn_ce1.cmd_raises("ip route show")
    logger.info("CE1 Routes: \n" + output)
    output = vpn_ce1.cmd_raises("ping -c1 20.0.0.1")
    output = uut.cmd_raises("ping -c1 20.0.0.2 -I uut-eth1")

    while not verify_bgp_convergence_from_running_config(tgen, uut):
        pass

    time.sleep(10)

    output = uut.vtysh_cmd("do show bgp sum")
    output = uut.vtysh_cmd("do show bgp all")
    output = vpn_ce1.vtysh_cmd("do show bgp sum")
    output = vpn_ce1.vtysh_cmd("do show bgp all")


def test_query_ram_usage(tgen):
    """Test if we can query the memory usage of each frr daemon on the UUT"""

    uut = tgen.gears["uut"]

    while not verify_bgp_convergence_from_running_config(tgen, uut):
        pass

    pidFiles = uut.run("ls -1 /var/run/frr/*.pid")
    pidFiles = pidFiles.split("\n")
    logger.info("pidFiles = " + str(pidFiles))
    ram_usages = dict()
    for pidFile in [pidFile for pidFile in pidFiles if pidFile != ""]:
        pid = int(uut.cmd_raises("cat %s" % pidFile, warn=False).strip())
        ram_usage = uut.run("pmap " + str(pid) + " | tail -n 1 | awk '/[0-9]K/{print $2}'")

        name = pidFile.split("/")[-1].split(".pid")[0]

        ram_usages[name] = int(ram_usage.strip()[:-1]) * 1000

    print("ram usages : " + str(ram_usages))


def test_prefix_spam(tgen):
    """Test the prefix announcement capability of p1"""

    uut = tgen.gears["uut"]

    def __send_prefixes_cmd(gear_asn, prefixes, yes):
        gear, asn = gear_asn
        gear.vtysh_cmd(
            """configure terminal
               router bgp ASN
               PREFIXES
            """ .replace("ASN", str(asn))
                .replace("PREFIXES", "\n".join([f"{'no ' if not yes else ''}network " + prefix for prefix in prefixes]))
        )

    def _send_prefixes(gear, prefixes, n, interval):
        __send_prefixes_cmd(gear, prefixes, True)
        time.sleep(interval / 1000)

        for _ in range(n):
            __send_prefixes_cmd(gear, prefixes, False)
            time.sleep(interval / 1000)
            __send_prefixes_cmd(gear, prefixes, True)
            time.sleep(interval / 1000)

        __send_prefixes_cmd(gear, prefixes, False)

    def _run_periodic_prefixes(gear, prefixes_path, n, interval):
        prefixes = json.load(open(prefixes_path)).get("prefixes")
        logger.info("Loaded prefixes: " + str(prefixes))
        logger.info(f"Repeat interval is {interval}ms")

        thread = threading.Thread(target=_send_prefixes, args=(gear, prefixes, n, interval))

        return thread

    ref_file = "{}/prefixes.json".format(CWD)

    threads = []
    for spammer in [("ce1", 200), ("p1", 100)]:
        spammer = (tgen.gears[spammer[0]], spammer[1])
        __send_prefixes_cmd(spammer, json.load(open(ref_file)).get("prefixes"), True)
        thread = _run_periodic_prefixes(spammer, ref_file, 2, 1000)
        threads.append(thread)
        thread.start()

    while True in [thread.is_alive() for thread in threads]:
        uut.vtysh_cmd("show bgp all")
        time.sleep(0.5)

    [thread.join() for thread in threads]


if __name__ == "__main__":
    args = ["-s"] + sys.argv[1:]
    sys.exit(pytest.main(args))
