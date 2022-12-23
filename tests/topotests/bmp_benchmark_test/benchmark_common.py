#!/usr/bin/env python

"""
<template>.py: Test <template>.
"""
import glob
import json
import multiprocessing
import os
import random
import re
import sys
import time

import pytest
from bmp_benchmark_test.frr_memuse_log_parse import get_modules_total_and_logs
from lib.bgp import verify_bgp_convergence_from_running_config
from lib.common_config import run_frr_cmd
from lib.topogen import Topogen, TopoRouter
from lib.topolog import logger

CWD = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(CWD, "../"))

reference_prefixes_registry = dict()


def load_prefix_files(prefix_files):
    global reference_prefixes_registry
    for name, rel_path in prefix_files.items():
        ref_file = "{}/{}".format(CWD, rel_path)
        with open(ref_file) as file:
            reference_prefixes_registry[name] = json.load(file).get("prefixes")


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

# capture router ids during the run
# to extract files correctly after the tests
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
    tgen.add_link(uut, provider_router_1, "uut-eth5", "prvdr1-eth1")


# New form of setup/teardown using pytest fixture
def setup_router_vrf(router):
    vrf_config_path = "{}/routers/{}/vrf.conf".format(CWD, router.name)
    if os.path.exists(vrf_config_path):
        with open(vrf_config_path) as vrf_config:
            for line in vrf_config.readlines():
                logger.info("adding vrf {}({}) and binding if {}".format(*line.split(" ")))
                router.cmd("/bin/bash {}/tools/mkvrf {}".format(CWD, line.strip()))


def init_test(request, ctx):
    # This function initiates the topology build with Topogen...
    tgen = Topogen(build_topo, request.module.__name__)

    ram_usage_tasks, topo_running = make_memusage_monitors(tgen, ["uut"], output_dir_format=ctx["memusage_output_dir"],
                                                           freq=100.0)

    # Start workers
    [task.start() for task in ram_usage_tasks]

    # Notify workers that topology will start soon (doesn't mean daemons are done loading)
    # so they should start recording
    with topo_running.get_lock():
        topo_running.value = 1

    # Start topology
    tgen.start_topology()

    # Load config into routers
    ctx['config_loader'](tgen)

    # Start and configure the router daemons
    tgen.start_router()

    load_prefix_files(ctx["prefixes_input_files"])

    return tgen, {
        "ram_usage_tasks": ram_usage_tasks,
        "topo_running": topo_running
    }


def fini_test(tgen, ctx):
    for rname, router in tgen.routers().items():
        router.stop()

    # TODO check if routers are stopped instead of waiting like a dummy
    time.sleep(5)

    logger.info("exporting logs")
    export_benchmark_logs(tgen, rnames=list(tgen.gears.keys()), to_dir=ctx["logs_output_dir"])

    # Teardown after last test runs
    tgen.stop_topology()

    topo_running = ctx["init_result"]["topo_running"]
    with topo_running.get_lock():
        topo_running.value = 0

    ram_usage_tasks = ctx["init_result"]["ram_usage_tasks"]
    [task.join() for task in ram_usage_tasks]


def default_config_loader(tgen, overrides):
    backup = os.environ["PYTEST_TOPOTEST_SCRIPTDIR"]
    current_scriptdir = f"{backup}/routers"
    os.environ["PYTEST_TOPOTEST_SCRIPTDIR"] = current_scriptdir

    def _daemon_config(rname, daemonRD):
        default = {"file": f"{TopoRouter.RD[daemonRD]}.conf", "args": "", "prefixes": None}
        return default | overrides[rname][daemonRD] \
            if rname in overrides and overrides[rname].get(daemonRD) is not None \
            else default

    def _abs_path(target, roots=(CWD,)):
        return os.path.join(*roots, target)

    for rname, router in tgen.routers().items():
        router.load_config(TopoRouter.RD_ZEBRA, "zebra.conf")

        bgp_config = _daemon_config(rname, TopoRouter.RD_BGP)
        print(rname, bgp_config)
        if bgp_config["prefixes"] is not None and os.path.exists(
                prefixes_file_absolute_path := _abs_path(bgp_config["prefixes"])):
            with open(prefixes_file_absolute_path) as prefixes_file:
                prefixes = json.load(prefixes_file)["prefixes"]
                print("initial prefixes count", len(prefixes))
            with open(_abs_path(bgp_config["file"], (current_scriptdir, rname))) as config_file:
                config = config_file.read().replace("### INIT_PREFIXES_PLACEHOLDER ###",
                                                    prefixes_announce_cmd_list(prefixes, update=True), 1)
                with open(_abs_path(bgp_config["file"] + ".test", (current_scriptdir, rname)), "w") as temp_config_file:
                    temp_config_file.write(config)
                    bgp_config["file"] = bgp_config["file"] + ".test"
                    bgp_config["delete_config"] = temp_config_file.name
        router.load_config(TopoRouter.RD_BGP, bgp_config["file"], bgp_config["args"])

        if bgp_config.get("delete_config") is not None:
            os.remove(bgp_config["delete_config"])

        setup_router_vrf(router)

    os.environ["PYTEST_TOPOTEST_SCRIPTDIR"] = backup


# Fixture that executes before each test
@pytest.fixture(autouse=True)
def skip_on_failure(tgen):
    if tgen.routers_have_failure():
        pytest.skip("skipped because of previous test failure")


def get_router_id(rnode):
    return router_ids.get(rnode.name)


# Function collecting routers ids before each test
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


def make_memusage_monitors(tgen, rnames, output_dir_format, freq=100.0):
    topo_running = multiprocessing.Value('i', 0)

    # Task run by memory usage monitoring workers, one for each router monitored
    def _monitor_ram_usage(rnode, topo_running_shared):

        # wait for topology to start
        while topo_running_shared.value == 0:
            time.sleep(0.1)

        all_daemons_found = {}

        ram_usages = list()

        # freq is Hz
        period = 1.0 / freq
        while topo_running_shared.value > 0:
            start = time.time()
            usage = get_router_ram_usages(rnode)
            # logger.info("adding {} values".format(len(usage.keys())))
            ram_usages.append({"timestamp": time.time_ns(), "usage": usage})

            # keep one good usage with most information possible
            if usage != {}:
                # add keys but with 0 value
                all_daemons_found = all_daemons_found | {k: {"total": 0, "details": []} for k in usage.keys()}

            end = time.time()
            duration = start - end
            time.sleep(min(max(0, period - duration), period))

        # dump mem usage results
        filepath = f"{output_dir_format}/ram_usage".replace("%CWD%", CWD).replace("%rname%", rnode.name)
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

    # Run memuse monitoring workers on selected routers
    ram_usage_tasks = [multiprocessing.Process(target=_monitor_ram_usage, args=(tgen.gears[rname], topo_running,)) for
                       rname in rnames]

    return ram_usage_tasks, topo_running


# Make a list of network announce commands from a list of prefixes
# Update command if update parameter is True else withdraw command is given
def prefixes_announce_cmd_list(prefixes, update):
    return "\n".join(
        [f"{'no ' if not update else ''}network " + prefix for prefix in prefixes])


# Send a bgp announce command to FRR router
def send_prefixes_announce_cmd(gear_asn, prefixes, update):
    gear, asn = gear_asn
    gear.vtysh_cmd(
        """configure terminal
        router bgp ASN 
        PREFIXES
        """.replace("ASN", str(asn)).replace("PREFIXES", prefixes_announce_cmd_list(prefixes, update))
    )


# Start periodic prefix announcers
def make_announcers(tgen, announcers, prefixes, normalize_slices, interval_ms, random_pick, N_iter=10):
    """Test the prefix announcement capability of p1"""

    announcers_count = len(announcers)

    def _send_prefixes_periodic(gear, prefixes, n, interval, pick_random: int = -1):
        # send_prefixes_announce_cmd(gear, prefixes, True)
        send_prefixes_announce_cmd(gear, ["69.69.69.0/24"], True)
        time.sleep(1)
        send_prefixes_announce_cmd(gear, ["69.69.69.0/24"], False)
        time.sleep(interval / 1000)

        this_time_prefixes = prefixes
        pick_random = min(max(0, pick_random), len(prefixes))
        for i in range(n):
            this_time_prefixes = random.sample(prefixes, k=pick_random) if pick_random > 0 else this_time_prefixes
            send_prefixes_announce_cmd(gear, this_time_prefixes, False)
            time.sleep(interval / 1000)
            send_prefixes_announce_cmd(gear, this_time_prefixes, True)
            time.sleep(interval / 1000)
            print("Announce wave {}/{}".format(i + 1, n))

        # send_prefixes_announce_cmd(gear, prefixes, False)

    def _run_periodic_prefixes(gear, prefixes, n, interval, pick_random=-1):
        logger.info(f"Loaded {len(prefixes)} prefixes")
        logger.info(f"Repeat interval is {interval}ms")

        task = multiprocessing.Process(target=_send_prefixes_periodic, args=(gear, prefixes, n, interval, pick_random))

        return task

    def _norm_slice(arr, start_perc, end_perc):
        return arr[int(start_perc * len(arr)):int(end_perc * len(arr))]

    tasks = []

    for idx, announcer in enumerate(announcers):
        announcer = (tgen.gears[announcer[0]], announcer[1])  # Router, ASN
        prefixes_slice = _norm_slice(prefixes, float(idx) / announcers_count, float(
            idx + 1.0) / announcers_count) if normalize_slices else prefixes

        task = _run_periodic_prefixes(announcer, prefixes_slice, N_iter, interval_ms, pick_random=random_pick)
        tasks.append(task)

    return tasks


# Export bencmark log files from a set of routers provided by name or object
def export_benchmark_logs(tgen, to_dir, rnames=None, routers=None):
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

    def _export_files(router, logdir, logfile_prefix, to_dir):
        def _is_right_file(file_path):
            for vrf_name, rid in get_router_id(router).items():
                if logfile_prefix % rid in file_path:
                    return True

            return False

        logger.info("exporting benchmark logs")
        to_dir = to_dir.replace("%CWD%", CWD).replace("%rname%", router.name)
        [os.remove(f) for f in glob.glob(os.path.join(to_dir, "benchmark_*_vrf_*")) if
         not os.path.samefile(to_dir, CWD) and not os.path.isdir(f)]
        for file in [os.path.join(logdir, x) for x in _list_files(router, logdir) if _is_right_file(x)]:
            path = os.path.join(to_dir, os.path.basename(file))
            logger.info(f"{file} -> {path}")
            _export_file(router, file, path)

    for router in (routers or [tgen.gears[x] for x in rnames] if rnames is not None else []):
        _export_files(router, "/tmp", "benchmark_%s_vrf_", to_dir=to_dir)


# Get pids of daemons
# Not used anymore, kept at a relic if needed one day
def get_pids(router: TopoRouter):
    pid_files = router.run("ls -1 /var/run/frr/*.pid")
    if re.search(r"No such file or directory", pid_files):
        return {}

    pid_files = pid_files.split("\n")
    logger.info("PID FILES " + str(pid_files))
    logger.info("PID FILES " + str(pid_files))

    return {pid_file.split("/")[-1].split(".pid")[0]: pid_file for pid_file in pid_files if
            pid_file.startswith("/") and pid_file.endswith(".pid")}


# Get daemons route usage for a router and get default if error
def get_router_ram_usages(rnode):
    try:
        command = "show memory bgpd"
        vtysh_command = 'vtysh -c "{}" 2>/dev/null'.format(command)
        o = rnode.run(vtysh_command)
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


# ===================
# The tests functions
# ===================


# Print FRR version
def test_get_version(tgen):
    """Test the logs the FRR version"""

    uut = tgen.gears["uut"]
    version = uut.vtysh_cmd("show version")
    logger.info("FRR version is: " + version)


# Show initial running configuration
def test_show_runnning_configuration(tgen):
    logger.info(tgen.gears.keys())
    for gear in tgen.gears:
        if isinstance(tgen.gears[gear], TopoRouter):
            tgen.gears[gear].vtysh_cmd("do show run")


# Check correct connectivity between nodes
def test_connectivity(tgen):
    """Test the logs the FRR version"""

    uut = tgen.gears["uut"]
    p1 = tgen.gears["p1"]
    p2 = tgen.gears["p2"]
    ce1 = tgen.gears["ce1"]
    ce2 = tgen.gears["ce2"]
    prvdr1 = tgen.gears["prvdr1"]

    # Log interfaces and routing tables
    def _log_ce(ce):
        o = ce.cmd_raises("ip link show")
        logger.info("{} Links: \n{}".format(ce.name, o))
        o = ce.cmd_raises("ip route show")
        logger.info("{} Routes: \n{}".format(ce.name, o))

    output = uut.cmd_raises("ip vrf show")
    logger.info("UUT VRF: \n" + output)
    output = uut.cmd_raises("ip route show")
    logger.info("UUT Routes: \n" + output)
    output = uut.cmd_raises("ip link show")
    logger.info("UUT Links: \n" + output)

    # Log routing table for peers
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
    output = prvdr1.cmd_raises("ping -c1 100.0.001.1 -I prvdr1-eth0")
    output = uut.cmd_raises("ping -c1 100.0.001.2 -I uut-eth4")
    output = prvdr1.cmd_raises("ping -c1 100.0.002.1 -I prvdr1-eth1")
    output = uut.cmd_raises("ping -c1 100.0.002.2 -I uut-eth5")

    while not verify_bgp_convergence_from_running_config(tgen, uut):
        pass

    # Show bgp sum for each vrf and bgp all
    def _show_all(router, vrfs=None):
        if vrfs is None:
            vrfs = []
        o = router.vtysh_cmd("do show bgp sum")
        for vrf in vrfs: o = router.vtysh_cmd(f"do show bgp vrf {vrf} sum")
        o = router.vtysh_cmd("do show bgp all")

    _show_all(ce1)
    _show_all(ce2)
    _show_all(uut, vrfs=["BLUE"])
    _show_all(prvdr1, vrfs=["BLUE"])

    o = tgen.gears["uut"].vtysh_cmd("do show bmp")

    o = tgen.gears["uut"].vtysh_cmd("do show bgp vrfs json", isjson=True)

    assert o["totalVrfs"] == 2

    # TODO add bgp peering assert


def test_query_ram_usage(tgen):
    """Test if we can query the memory usage of each frr daemon on the UUT"""

    uut = tgen.gears["uut"]

    while not verify_bgp_convergence_from_running_config(tgen, uut):
        pass

    ram_usages = get_router_ram_usages(uut)
    logger.info("ram usages : " + str(ram_usages))


def test_run_scenario(tgen):
    # ipv4 unicast ~1M prefixes
    logger.info(f"Using {len(reference_prefixes_registry)} prefixes for IPv4 Unicast")
    ipv4uni_1M = reference_prefixes_registry["ipv4"]

    # ipv4 vpn ~500k prefixes
    logger.info("Selecting 500k prefixes for IPv4 VPN")
    ipv4vpn_500k = reference_prefixes_registry["vpnv4"]

    # Iteration count
    N_iter = 50
    interval_ms = 500
    random_pick = 100

    # Initial RIB populate
    # logger.info("Populating RIBs")
    # logger.info("   RIB P1")
    # send_prefixes_announce_cmd(gear_asn=(tgen.gears["p1"], 101), prefixes=ipv4uni_1M, update=True)
    # logger.info("   RIB CE1")
    # send_prefixes_announce_cmd(gear_asn=(tgen.gears["ce1"], 200), prefixes=ipv4vpn_500k, update=True)

    # Prepare tasks
    logger.info("Making announcer tasks")
    p2_announcer = make_announcers(tgen=tgen, announcers=(("p2", 102),), prefixes=ipv4uni_1M,
                                   normalize_slices=False, interval_ms=interval_ms, random_pick=random_pick,
                                   N_iter=N_iter)
    ce2_announcer = make_announcers(tgen=tgen, announcers=(("ce2", 200),), prefixes=ipv4vpn_500k,
                                    normalize_slices=False, interval_ms=interval_ms, random_pick=random_pick,
                                    N_iter=N_iter)
    tasks = [*p2_announcer, *ce2_announcer]

    logger.info("Starting announcers")
    [task.start() for task in tasks]

    logger.info("Waiting for announcers")
    [task.join() for task in tasks]

    logger.info("Announcers done")
