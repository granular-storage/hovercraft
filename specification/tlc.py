#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the Apache 2.0 License.
#
# Python TLC wrapper script for the raft project
# Goals:
# - No dependencies, no venv, no pip install
# - Set sensible defaults with an eye on performance
# - Capture useful switches for CI, debugging
# - Expose specification configuration through CLI
# - Provide a useful --help, and basic sanity checks

import os
import sys
import shlex
import argparse
import pprint
import pathlib

DEFAULT_JVM_ARGS = [
    "-XX:+UseParallelGC",  # Use parallel garbage collection, for performance
    "-Xmx8G",              # Set maximum heap size to 8GB
    "-Xms4G",              # Set initial heap size to 4GB
    "-XX:MaxDirectMemorySize=4G"  # Set maximum direct memory size to 4GB
]

DEFAULT_CLASSPATH_ARGS = ["-cp", "tla2tools.jar:CommunityModules-deps.jar"]

USAGE = """
To forward arguments directly to TLC that the wrapper does not support,
run with the -n flag, and evaluate the output beforehand, e.g. `./tlc.py -n mc Spec.tla` -debugger
"""


def cli():
    parser = argparse.ArgumentParser(
        description="TLC model checker wrapper for the raft project", usage=USAGE
    )

    # Common options for all commands
    parser.add_argument(
        "-x",
        action="store_true",
        help="Print out command and environment before running",
    )
    parser.add_argument(
        "-n",
        action="store_true",
        help="Print out command and environment, but do not run",
    )
    # Changes to TLC defaults
    parser.add_argument(
        "--disable-cdot", action="store_true", help="Disable \\cdot support"
    )
    parser.add_argument(
        "--workers",
        type=str,
        default="auto",
        help="Number of workers to use, default is 'auto'",
    )
    parser.add_argument(
        "--checkpoint", type=int, default=0, help="Checkpoint interval, default is 0"
    )
    parser.add_argument(
        "--lncheck",
        type=str,
        choices=["final", "default"],
        default="final",
        help="Liveness check, set to 'default' to run periodically or 'final' to run once at the end, default is final",
    )
    # Convenient shortcuts applicable to all commands
    parser.add_argument(
        "--jmx",
        action="store_true",
        help="Enable JMX to allow monitoring, use echo 'get -b tlc2.tool:type=ModelChecker CurrentState' | jmxterm -l localhost:55449 -i to query",
    )
    parser.add_argument(
        "--dot", action="store_true", help="Generate a dot file for the state graph"
    )
    parser.add_argument(
        "--coverage",
        type=int,
        default=None,
        help="Enable coverage statistics collection with the specified level (typically 1)",
    )
    parser.add_argument(
        "--trace-name",
        type=str,
        default=None,
        help="Name to give to the trace files, defaults to the config name if provided, otherwise to the base name of the spec",
    )
    parser.add_argument(
        "--config",
        type=pathlib.Path,
        default=None,
        help="Path to the TLA+ configuration, defaults to spec name",
    )
    parser.add_argument(
        "--difftrace",
        action="store_true",
        help="When printing a trace, show only the differences between states",
    )

    subparsers = parser.add_subparsers(dest="cmd")

    # Model checking
    mc = subparsers.add_parser("mc", help="Model checking")
    # Control spec options of raft
    mc.add_argument(
        "--raft-configs",
        type=str,
        default="1C3N",
        help="Raft configuration sequence, defaults to 1C3N",
    )

    # Simulation
    sim = subparsers.add_parser("sim", help="Simulation")
    sim.add_argument(
        "--num",
        type=int,
        default=None,
        help="Number of behaviours to simulate per worker thread",
    )
    sim.add_argument(
        "--depth",
        type=int,
        default=500,
        help="Set the depth of the simulation, defaults to 500",
    )
    sim.add_argument(
        "--max-seconds",
        type=int,
        default=3600,
        help="Set the timeout of the simulation, defaults to 3600 seconds",
    )

    parser.add_argument(
        "spec", type=pathlib.Path, help="Path to the TLA+ specification"
    )

    return parser


CI = "CI" in os.environ


if __name__ == "__main__":
    env = {}
    args = cli().parse_args()
    jvm_args = DEFAULT_JVM_ARGS
    cp_args = DEFAULT_CLASSPATH_ARGS
    tlc_args = [
        "-workers",
        args.workers,
        "-checkpoint",
        str(args.checkpoint),
        "-lncheck",
        args.lncheck,
    ]
    trace_name = args.trace_name or os.path.basename(args.config or args.spec).replace(
        ".tla", ""
    )

    if CI:
        # When run in CI, format output for GitHub, and participate in statistics collection
        jvm_args.append("-Dtlc2.TLC.ide=Github")
        jvm_args.append(
            "-Dutil.ExecutionStatisticsCollector.id=be29f6283abeed2fb1fd0be898bc6601"
        )

    if args.trace_name or CI:
        # When run in CI, or told explicitly, dump a trace
        tlc_args.extend(["-dumpTrace", "json", f"{trace_name}.trace.json"])
    if args.jmx:
        jvm_args.append("-Dcom.sun.management.jmxremote")
        jvm_args.append("-Dcom.sun.management.jmxremote.port=55449")
        jvm_args.append("-Dcom.sun.management.jmxremote.ssl=false")
        jvm_args.append("-Dcom.sun.management.jmxremote.authenticate=false")
    if not args.disable_cdot:
        jvm_args.append("-Dtlc2.tool.impl.Tool.cdot=true")
    if args.config is not None:
        tlc_args.extend(["-config", args.config])
    if args.dot:
        tlc_args.extend(
            ["-dump", "dot,constrained,colorize,actionlabels", f"{trace_name}.dot"]
        )
    if args.difftrace:
        tlc_args.extend(["-difftrace"])
    if args.coverage is not None:
        tlc_args.extend(["-coverage", str(args.coverage)])

    if args.cmd == "mc":
        if args.raft_configs is not None:
            env["RAFT_CONFIGS"] = args.raft_configs
    elif args.cmd == "sim":
        tlc_args.extend(["-simulate", "num=10000"])
        if args.num is not None:
            tlc_args.extend([f"num={args.num}"])
        env["SIM_TIMEOUT"] = str(args.max_seconds)
        if args.depth is not None:
            tlc_args.extend(["-depth", str(args.depth)])
    else:
        raise ValueError(f"Unknown command: {args.cmd}")

    cmd = ["java"] + jvm_args + cp_args + ["tlc2.TLC"] + tlc_args + [args.spec]
    
    if args.x or args.n:
        env_prefix = " ".join(
            f"{key}={shlex.quote(value)}" for key, value in env.items()
        )
        print(f"env {env_prefix} {shlex.join(str(arg) for arg in cmd)}")
    if args.n:
        sys.exit(0)
    merged_env = os.environ.copy()
    merged_env.update(env)
    os.execvpe("java", cmd, merged_env)
