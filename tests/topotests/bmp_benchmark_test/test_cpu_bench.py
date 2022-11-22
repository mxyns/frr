#!/usr/bin/env python

"""
<template>.py: Test <template>.
"""
import glob
import json
import os
import random
import re
import sys
import threading
import time

import pytest
from lib.bgp import verify_bgp_convergence_from_running_config
from lib.topogen import Topogen, TopoRouter
from lib.topolog import logger

from tests.topotests.bmp_benchmark_test.frr_memuse_log_parse import get_modules_total_and_logs
from tests.topotests.lib.common_config import run_frr_cmd

CWD = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(CWD, "../"))

# prefix_file = "prefixes.json"
prefix_file = "routeviews_prefixes.json"
# prefix_file = "prefixes_single.json"

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

router_ids = dict()


# Function we pass to Topogen to create the topology
def build_topo(tgen):
    """Build function"""

    uut = tgen.add_router("uut")

    p1 = tgen.add_router("p1")
    tgen.add_link(uut, p1, "uut-eth0", "p1-eth0")

    p2 = tgen.add_router("p2")
    tgen.add_link(uut, p2, "uut-eth1", "p2-eth0")

    ce1 = tgen.add_router("ce1")
    tgen.add_link(uut, ce1, "uut-eth2", "ce1-eth0")

    ce2 = tgen.add_router("ce2")
    tgen.add_link(uut, ce2, "uut-eth3", "ce2-eth0")

    provider_router_1 = tgen.add_router("prvdr1")
    tgen.add_link(uut, provider_router_1, "uut-eth4", "prvdr1-eth0")


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

    topo_running = False

    def _monitor_ram_usage(rnode):
        logger.info("RAM MONITOR")

        # wait for topology to start
        while not topo_running:
            time.sleep(0.1)

        all_daemons_found = {}

        ram_usages = list()
        # wait for topology to stop
        while topo_running:
            usage = get_router_ram_usages(rnode)
            logger.info("adding {} values".format(len(usage.keys())))
            ram_usages.append({"timestamp": time.time_ns(), "usage": usage})

            # keep one good usage with most information possible
            if usage != {}:
                # add keys but with 0 value
                all_daemons_found = all_daemons_found | {k: {"total": 0, "details": []} for k in usage.keys()}

        # dump mem usage results
        filepath = "{}/{}/benchmark/ram_usage".format(CWD, rnode.name)
        [os.remove(x) for x in glob.glob(os.path.join(os.path.dirname(filepath), "ram_usage*"))]
        with open(filepath, "w") as f:
            logger.info("writing to {}".format(filepath))
            # fill with 0 values where we're missing some data
            for ram_usage in ram_usages:
                ram_usage["usage"] = {
                    k: all_daemons_found.get(k) | (ram_usage.get("usage").get(k) or {"total": 0, "details": []})
                    for k in all_daemons_found.keys()}
            ram_usages_json = json.dumps(ram_usages)
            f.write(ram_usages_json)

    ram_usage_threads = [threading.Thread(target=_monitor_ram_usage, args=(tgen.gears[rname],)) for rname in ["uut"]]
    [thread.start() for thread in ram_usage_threads]

    topo_running = True
    tgen.start_topology()

    router_list = tgen.routers()

    for rname, router in router_list.items():
        router.load_config(TopoRouter.RD_ZEBRA, "zebra.conf")
        router.load_config(TopoRouter.RD_BGP, "bgpd.conf", "-M bmp --log-level debugging")
        logger.info(f"{type(router)}, {dir(type(router))}")
        setup_router_vrf(router)

    # Start and configure the router daemons
    tgen.start_router()

    # Provide tgen as argument to each test function
    yield tgen

    for rname, router in router_list.items():
        router.stop()

    time.sleep(5)

    logger.info("exporting logs")
    export_benchmark_logs(tgen, rnames=list(tgen.gears.keys()))

    # Teardown after last test runs
    tgen.stop_topology()

    topo_running = False
    [thread.join() for thread in ram_usage_threads]


# Fixture that executes before each test
@pytest.fixture(autouse=True)
def skip_on_failure(tgen):
    if tgen.routers_have_failure():
        pytest.skip("skipped because of previous test failure")


def get_router_id(rnode):
    return router_ids.get(rnode.name)


@pytest.fixture(autouse=True)
def get_router_ids(tgen):
    def _get_router_ids(rnode):
        show_bgp_json = json.loads(run_frr_cmd(rnode, "show bgp vrfs json") or "{\"vrfs\":{}}").get("vrfs") or dict(
            {"vrfs": {}})
        logger.info(show_bgp_json)

        router_id_out = dict()
        for vrf_name, vrf_obj in show_bgp_json.items():
            logger.info("{} = {}".format(vrf_name, vrf_obj.get("routerId")))
            router_id_out[vrf_name] = vrf_obj.get("routerId")

        return router_id_out if bool(router_id_out) else get_router_id(rnode)

    for rname, router in tgen.routers().items():
        router_ids[rname] = _get_router_ids(router)


# ===================
# The tests functions
# ===================


def test_get_version(tgen):
    """Test the logs the FRR version"""

    uut = tgen.gears["uut"]
    version = uut.vtysh_cmd("show version")
    logger.info("FRR version is: " + version)


def test_show_runnning_configuration(tgen):
    for gear in tgen.gears:
        if isinstance(tgen.gears[gear], TopoRouter):
            tgen.gears[gear].vtysh_cmd("do show run")


def test_connectivity(tgen):
    """Test the logs the FRR version"""

    uut = tgen.gears["uut"]
    p1 = tgen.gears["p1"]
    p2 = tgen.gears["p2"]
    ce1 = tgen.gears["ce1"]
    ce2 = tgen.gears["ce2"]
    prvdr1 = tgen.gears["prvdr1"]

    def _log_ce(ce):
        o = ce.cmd_raises("ip link show")
        logger.info("{} Links: \n{}".format(ce.name, o))
        o = ce.cmd_raises("ip route show")
        logger.info("{} Routes: \n{}".format(ce.name, o))

    output = uut.cmd_raises("ip vrf show")
    logger.info("UUT Links: \n" + output)
    output = uut.cmd_raises("ip route show")
    logger.info("UUT Routes: \n" + output)
    output = uut.cmd_raises("ip link show")
    logger.info("UUT Links: \n" + output)

    def _log_p(p):
        o = p.cmd_raises("ip route show")
        logger.info("{} Routes: \n{}".format(p.name, o))

    _log_p(p1)
    output = p1.cmd_raises("ping -c1 10.1.0.1")
    output = uut.cmd_raises("ping -c1 10.1.0.2 -I uut-eth0")

    _log_p(p2)
    output = p2.cmd_raises("ping -c1 10.2.0.1")
    output = uut.cmd_raises("ping -c1 10.2.0.2 -I uut-eth1")

    _log_ce(ce1)
    output = ce1.cmd_raises("ping -c1 20.0.1.1")
    output = uut.cmd_raises("ping -c1 20.0.1.2 -I uut-eth2")

    _log_ce(ce2)
    output = ce2.cmd_raises("ping -c1 20.0.2.1")
    output = uut.cmd_raises("ping -c1 20.0.2.2 -I uut-eth3")

    _log_ce(prvdr1)
    output = prvdr1.cmd_raises("ping -c1 100.0.001.1")
    output = uut.cmd_raises("ping -c1 100.0.001.2 -I uut-eth4")

    while not verify_bgp_convergence_from_running_config(tgen, uut):
        pass

    time.sleep(3)

    def _show_all(router):
        o = router.vtysh_cmd("do show bgp sum")
        o = router.vtysh_cmd("do show bgp all")

    _show_all(ce1)
    _show_all(ce2)
    _show_all(uut)

    o = tgen.gears["uut"].vtysh_cmd("do show bmp")


def get_pids(router: TopoRouter):
    pid_files = router.run("ls -1 /var/run/frr/*.pid")
    if re.search(r"No such file or directory", pid_files):
        return {}

    pid_files = pid_files.split("\n")
    logger.info("PID FILES " + str(pid_files))
    logger.info("PID FILES " + str(pid_files))

    return {pid_file.split("/")[-1].split(".pid")[0]: pid_file for pid_file in pid_files if
            pid_file.startswith("/") and pid_file.endswith(".pid")}


def get_router_ram_usages(rnode):
    try:
        o = rnode.vtysh_cmd("show memory bgpd")
        return get_modules_total_and_logs(o)
    except Exception as e:
        logger.info(f"ERROR", e)
        return {
            "bmp": {},
            "bgpd": {},
            "lmlogs": {},
            "logging": {},
            "libfrr": {}
        }


def test_query_ram_usage(tgen):
    """Test if we can query the memory usage of each frr daemon on the UUT"""

    uut = tgen.gears["uut"]

    while not verify_bgp_convergence_from_running_config(tgen, uut):
        pass

    ram_usages = get_router_ram_usages(uut)
    logger.info("ram usages : " + str(ram_usages))


def test_prefix_spam(tgen):
    """Test the prefix announcement capability of p1"""

    uut = tgen.gears["uut"]

    def __send_prefixes_cmd(gear_asn, prefixes, yes):
        gear, asn = gear_asn
        gear.vtysh_cmd(
            """configure terminal
               router bgp ASN
               PREFIXES
            """.replace("ASN", str(asn))
            .replace("PREFIXES", "\n".join([f"{'no ' if not yes else ''}network " + prefix for prefix in prefixes]))
        )

    def _send_prefixes(gear, prefixes, n, interval, pick_random: int = -1):
        __send_prefixes_cmd(gear, prefixes, True)
        time.sleep(interval / 1000)

        this_time_prefixes = prefixes
        pick_random = min(max(0, pick_random), len(prefixes))
        for _ in range(n):
            this_time_prefixes = random.sample(prefixes, k=pick_random) if pick_random > 0 else this_time_prefixes
            __send_prefixes_cmd(gear, prefixes, False)
            time.sleep(interval / 1000)
            __send_prefixes_cmd(gear, prefixes, True)
            time.sleep(interval / 1000)

        __send_prefixes_cmd(gear, prefixes, False)

    def _run_periodic_prefixes(gear, prefixes, n, interval, pick_random=-1):
        logger.info("Loaded prefixes: " + str(prefixes))
        logger.info(f"Repeat interval is {interval}ms")

        thread = threading.Thread(target=_send_prefixes, args=(gear, prefixes, n, interval, pick_random))

        return thread

    def _norm_slice(arr, start_perc, end_perc):
        logger.info(f"slicing from {start_perc * 100.0}% to {end_perc * 100.0}%")
        return arr[int(start_perc * len(arr)):int(end_perc * len(arr))]

    ref_file = "{}/{}".format(CWD, prefix_file)
    spammers = [("ce1", 200), ("p1", 101)]  # rname, asn
    spammer_count = len(spammers)
    threads = []

    with open(ref_file) as file:
        prefixes = json.load(file).get("prefixes")
        for idx, spammer in enumerate(spammers):
            spammer = (tgen.gears[spammer[0]], spammer[1])
            prefixes_slice = _norm_slice(prefixes, float(idx) / spammer_count, float(idx + 1.0) / spammer_count)
            thread = _run_periodic_prefixes(spammer, prefixes_slice, 10, 1000, pick_random=100)
            threads.append(thread)
            thread.start()

    while True in [thread.is_alive() for thread in threads]:
        time.sleep(.5)
        uut.vtysh_cmd("do show bmp")
        uut.vtysh_cmd("do show bgp all")

    [thread.join() for thread in threads]


def export_benchmark_logs(tgen, rnames=None, routers=None):
    def _export_file(router, file_from, file_to):
        content = router.cmd_raises(f"cat {file_from}")
        # logger.info(content)
        dir_to = os.path.dirname(file_to)
        os.makedirs(dir_to, exist_ok=True)

        with open(file_to, "w") as f:
            logger.info("opened " + file_to)
            f.write(content)

    def _list_files(router, ls_dir):
        content = router.cmd(f"ls {ls_dir} -1 --color=never ")
        logger.info(f"benchmark files :\n {content}")
        return content.split("\n")

    def _export_files(router, logdir, prefix):
        def _is_right_file(file_path):
            for vrf_name, rid in get_router_id(router).items():
                if prefix % rid in file_path:
                    return True

            return False

        logger.info("exporting benchmark logs")
        to_dir = f"{CWD}/{router.name}/benchmark"
        [os.remove(f) for f in glob.glob(os.path.join(to_dir, "benchmark_*_vrf_*")) if
         not os.path.samefile(to_dir, CWD) and not os.path.isdir(f)]
        for file in [os.path.join(logdir, x) for x in _list_files(router, logdir) if _is_right_file(x)]:
            path = os.path.join(to_dir, os.path.basename(file))
            logger.info(f"{file} -> {path}")
            _export_file(router, file, path)

    for router in (routers or [tgen.gears[x] for x in rnames] if rnames is not None else []):
        _export_files(router, "/tmp", "benchmark_%s_vrf_")


if __name__ == "__main__":
    args = ["-s"] + sys.argv[1:]
    sys.exit(pytest.main(args))
