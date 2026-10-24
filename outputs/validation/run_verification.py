#!/usr/bin/env python3
"""Run the repository's verification suites with isolated artifacts and true JUnit counts."""

from __future__ import annotations

import argparse
import json
import math
import os
import shlex
import signal
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# This is intentionally based on the file location, not the caller's current directory.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
RESULTS_ROOT = PROJECT_ROOT / "outputs" / "validation" / "results"
LOGS_ROOT = PROJECT_ROOT / ".work" / "validation"

E2E_SELECTORS = [
    "tests/test_acceptance_e2e.py",
    "tests/test_security_e2e.py",
    (
        "tests/test_server_mcp.py::"
        "test_real_stdio_chain_works_from_other_cwd_and_shutdown_reaps_live_pty"
    ),
    (
        "tests/test_config.py::"
        "test_production_config_cli_enforces_roots_and_os_identity_and_writes_jsonl"
    ),
    "tests/test_edge_cases.py::test_delivered_repl_client_uses_venv_and_real_stdio",
    "tests/test_lifecycle_e2e.py",
]

SUITE_SELECTORS = {
    "e2e": E2E_SELECTORS,
    "full": ["tests"],
}


class VerificationArgumentParser(argparse.ArgumentParser):
    """Render command-line input errors in Chinese."""

    def error(self, message: str) -> None:
        self.exit(2, f"参数错误：{message}\n")


def positive_repeat(value: str) -> int:
    """Parse a strictly positive repeat count."""
    try:
        repeat = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--repeat 必须是正整数") from exc
    if repeat <= 0:
        raise argparse.ArgumentTypeError("--repeat 必须是正整数")
    return repeat


def positive_seconds(value: str) -> float:
    """Parse one positive, finite per-stage timeout in seconds."""
    try:
        seconds = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--timeout 必须是正数秒数") from exc
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError("--timeout 必须是有限的正数秒数")
    return seconds


def build_parser() -> argparse.ArgumentParser:
    """Build the reusable command-line interface."""
    parser = VerificationArgumentParser(
        description="使用项目虚拟环境运行真实 pytest 验收套件，并保存分阶段结果。"
    )
    parser.add_argument(
        "--suite",
        choices=("both", "e2e", "full"),
        default="both",
        help="要运行的套件：both、e2e 或 full（默认：%(default)s）",
    )
    parser.add_argument(
        "--repeat",
        type=positive_repeat,
        default=1,
        metavar="N",
        help="每个所选套件的运行次数，必须为正整数（默认：%(default)s）",
    )
    parser.add_argument(
        "--timeout",
        type=positive_seconds,
        default=180.0,
        metavar="SECONDS",
        help="单个 pytest 阶段的超时秒数，必须为正数（默认：%(default)s）",
    )
    return parser


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp suitable for JSON."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def make_run_id() -> str:
    """Make a UTC-bearing run identifier that is unique across concurrent runs."""
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
    return f"{timestamp}-{os.getpid()}-{uuid.uuid4().hex[:8]}"


def format_command(command: list[str]) -> str:
    """Format an argv list for a human-readable JSON record without invoking a shell."""
    return shlex.join(command)


def local_name(tag: str) -> str:
    """Drop an XML namespace prefix from an ElementTree tag."""
    return tag.rsplit("}", maxsplit=1)[-1]


def parse_junit_count(raw_value: str | None) -> int:
    """Read a non-negative JUnit count, treating an omitted attribute as zero."""
    if raw_value is None:
        return 0
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"JUnit 计数不是整数：{raw_value!r}") from exc
    if value < 0:
        raise ValueError(f"JUnit 计数不能为负数：{raw_value!r}")
    return value


def suite_counts(element: ET.Element) -> dict[str, int]:
    """Extract the four contract-required counters from one suite-like XML element."""
    return {
        "tests": parse_junit_count(element.get("tests")),
        "failures": parse_junit_count(element.get("failures")),
        "errors": parse_junit_count(element.get("errors")),
        "skipped": parse_junit_count(element.get("skipped")),
    }


def add_counts(total: dict[str, int], extra: dict[str, int]) -> None:
    """Add XML counters without deriving any expected count from source files."""
    for key in total:
        total[key] += extra[key]


def parse_junit(junit_path: Path) -> tuple[dict[str, int] | None, str | None]:
    """Parse actual pytest JUnit output, avoiding double-counting nested suites."""
    if not junit_path.is_file():
        return None, "JUnit XML 未生成"

    try:
        root = ET.parse(junit_path).getroot()
    except (ET.ParseError, OSError) as exc:
        return None, f"JUnit XML 无法解析：{type(exc).__name__}: {exc}"

    try:
        root_name = local_name(root.tag)
        if root_name == "testsuite":
            return suite_counts(root), None
        if root_name != "testsuites":
            return None, f"JUnit XML 根元素不是 testsuite/testsuites：{root_name}"

        # pytest's xunit1 output normally aggregates its counters on <testsuites>.
        if any(key in root.attrib for key in ("tests", "failures", "errors", "skipped")):
            return suite_counts(root), None

        # Fall back to leaf suites for producers that omit root-level aggregates.
        total = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
        leaf_suites = []
        for element in root.iter():
            if local_name(element.tag) != "testsuite":
                continue
            has_nested_suite = any(local_name(child.tag) == "testsuite" for child in list(element))
            if not has_nested_suite:
                leaf_suites.append(element)
        if not leaf_suites:
            return None, "JUnit XML 中没有可统计的 testsuite"
        for suite in leaf_suites:
            add_counts(total, suite_counts(suite))
        return total, None
    except ValueError as exc:
        return None, f"JUnit XML 计数无效：{exc}"


def wait_for_exit(process: subprocess.Popen[str], seconds: float) -> bool:
    """Wait for a process, continuing cleanup if the user sends another Ctrl-C."""
    deadline = time.monotonic() + seconds
    while process.poll() is None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        try:
            process.wait(timeout=min(remaining, 1.0))
        except subprocess.TimeoutExpired:
            continue
        except KeyboardInterrupt:
            # Cleanup must finish its bounded attempt before this script returns cancellation.
            continue
    return True


def signal_pytest_group(
    process: subprocess.Popen[str], signal_number: int, cleanup: list[str]
) -> None:
    """Signal only the process group created for this pytest invocation."""
    signal_name = signal.Signals(signal_number).name
    if process.poll() is not None:
        cleanup.append(f"pytest 已退出，未发送 {signal_name}")
        return
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal_number)
        else:
            process.send_signal(signal_number)
        cleanup.append(f"已向本轮 pytest 进程组发送 {signal_name}")
    except ProcessLookupError:
        cleanup.append(f"pytest 进程组已不存在，未发送 {signal_name}")
    except OSError as exc:
        cleanup.append(f"发送 {signal_name} 失败：{type(exc).__name__}: {exc}")


def clean_up_pytest_group(process: subprocess.Popen[str]) -> list[str]:
    """Escalate only within the current pytest process group after an interrupt."""
    cleanup: list[str] = []
    signal_pytest_group(process, signal.SIGINT, cleanup)
    if wait_for_exit(process, 8.0):
        return cleanup

    signal_pytest_group(process, signal.SIGTERM, cleanup)
    if wait_for_exit(process, 5.0):
        return cleanup

    signal_pytest_group(process, signal.SIGKILL, cleanup)
    if not wait_for_exit(process, 3.0):
        cleanup.append("pytest 进程组在 SIGKILL 后仍未确认退出")
    return cleanup


def junit_validation_issues(counters: dict[str, int] | None, junit_error: str | None) -> list[str]:
    """Return failures that must never be treated as a successful verification run."""
    if junit_error is not None:
        return [junit_error]
    if counters is None:
        return ["JUnit XML 统计不可用"]

    issues: list[str] = []
    if counters["failures"]:
        issues.append(f"JUnit 报告 {counters['failures']} 个 failures")
    if counters["errors"]:
        issues.append(f"JUnit 报告 {counters['errors']} 个 errors")
    if counters["skipped"]:
        issues.append(f"JUnit 报告 {counters['skipped']} 个 skipped，验收不通过")
    if counters["tests"] == 0:
        issues.append("JUnit 未记录任何测试")
    return issues


def run_stage(
    suite_name: str,
    repeat_index: int,
    timeout_seconds: float,
    results_dir: Path,
    logs_dir: Path,
) -> dict[str, Any]:
    """Run exactly one suite invocation and preserve all machine-readable evidence."""
    stage_id = f"{suite_name}-{repeat_index:02d}"
    junit_path = results_dir / f"{stage_id}.junit.xml"
    log_path = logs_dir / f"{stage_id}.console.log"
    command = [
        str(VENV_PYTHON),
        "-m",
        "pytest",
        "-q",
        "-W",
        "error",
        "--tb=short",
        "-o",
        "junit_family=xunit1",
        f"--junitxml={junit_path}",
        *SUITE_SELECTORS[suite_name],
    ]
    stage: dict[str, Any] = {
        "name": suite_name,
        "repeat_index": repeat_index,
        "stage_id": stage_id,
        "selectors": list(SUITE_SELECTORS[suite_name]),
        "command": command,
        "command_display": format_command(command),
        "cwd": str(PROJECT_ROOT),
        "venv_python": str(VENV_PYTHON),
        "log_path": str(log_path),
        "junit_path": str(junit_path),
        "timeout_seconds": timeout_seconds,
        "started_at_utc": utc_now(),
        "duration_seconds": None,
        "exit_code": None,
        "tests": None,
        "failures": None,
        "errors": None,
        "skipped": None,
        "cleanup": [],
        "cleanup_boundary": None,
        "validation_issues": [],
        "status": "running",
        "passed": False,
    }

    started = time.monotonic()
    timed_out = False
    cancelled = False
    launch_error: str | None = None
    process: subprocess.Popen[str] | None = None

    try:
        with log_path.open("w", encoding="utf-8") as log_file:
            log_file.write(f"Verification stage: {stage_id}\n")
            log_file.write(f"Started (UTC): {stage['started_at_utc']}\n")
            log_file.write(f"Command: {stage['command_display']}\n\n")
            log_file.flush()
            try:
                process = subprocess.Popen(
                    command,
                    cwd=str(PROJECT_ROOT),
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    text=True,
                    start_new_session=True,
                )
                try:
                    process.wait(timeout=timeout_seconds)
                except subprocess.TimeoutExpired:
                    timed_out = True
                    stage["cleanup"] = clean_up_pytest_group(process)
                except KeyboardInterrupt:
                    cancelled = True
                    stage["cleanup"] = clean_up_pytest_group(process)
            except OSError as exc:
                launch_error = f"{type(exc).__name__}: {exc}"
                log_file.write(f"\nUnable to start pytest: {launch_error}\n")
    finally:
        stage["duration_seconds"] = round(time.monotonic() - started, 3)

    if process is not None:
        stage["exit_code"] = process.poll()
    if launch_error is not None:
        stage["launch_error"] = launch_error

    counters, junit_error = parse_junit(junit_path)
    if counters is not None:
        stage.update(counters)

    issues: list[str] = []
    if launch_error is not None:
        issues.append(f"pytest 无法启动：{launch_error}")
    if timed_out:
        issues.append(f"pytest 超过单阶段超时 {timeout_seconds:g} 秒")
        stage["cleanup_boundary"] = (
            "超时后仅对本轮 pytest 进程组发送中断/停止信号；"
            "无法保证已脱离该进程组的 PTY 子进程被清理。"
        )
    if cancelled:
        issues.append("验收被取消")
        stage["cleanup_boundary"] = (
            "取消后仅对本轮 pytest 进程组发送中断/停止信号；"
            "无法保证已脱离该进程组的 PTY 子进程被清理。"
        )
    if process is not None and stage["exit_code"] is None:
        issues.append("pytest 退出码不可用")
    elif process is not None and stage["exit_code"] != 0:
        issues.append(f"pytest 返回非零退出码：{stage['exit_code']}")
    issues.extend(junit_validation_issues(counters, junit_error))

    stage["validation_issues"] = issues
    stage["passed"] = not issues
    if stage["passed"]:
        stage["status"] = "passed"
    elif timed_out:
        stage["status"] = "timed_out"
    elif cancelled:
        stage["status"] = "cancelled"
    elif launch_error is not None:
        stage["status"] = "launch_error"
    else:
        stage["status"] = "failed"
    stage["completed_at_utc"] = utc_now()
    return stage


def stage_console_summary(stage: dict[str, Any]) -> str:
    """Make the required bounded, per-stage console summary."""
    return (
        f"[完成] {stage['stage_id']}: status={stage['status']}; "
        f"tests={stage['tests']}; failures={stage['failures']}; "
        f"errors={stage['errors']}; skipped={stage['skipped']}; "
        f"exit_code={stage['exit_code']}; duration={stage['duration_seconds']}s"
    )


def write_summary(summary_path: Path, summary: dict[str, Any]) -> None:
    """Atomically persist the structured run result after each complete invocation set."""
    temporary_path = summary_path.with_suffix(".json.tmp")
    payload = json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    temporary_path.write_text(payload, encoding="utf-8")
    temporary_path.replace(summary_path)


def selected_suites(suite: str) -> list[str]:
    """Expand the CLI suite choice while keeping e2e and full as distinct phases."""
    if suite == "both":
        return ["e2e", "full"]
    return [suite]


def verify_environment() -> str | None:
    """Return a Chinese environment error instead of installing or resolving anything."""
    if not VENV_PYTHON.is_file():
        return f"项目虚拟环境解释器不存在：{VENV_PYTHON}；不会自动安装依赖。"
    if not os.access(VENV_PYTHON, os.X_OK):
        return f"项目虚拟环境解释器不可执行：{VENV_PYTHON}。"
    if not (PROJECT_ROOT / "tests").is_dir():
        return f"项目 tests 目录不存在：{PROJECT_ROOT / 'tests'}。"
    return None


def run_verification(args: argparse.Namespace) -> int:
    """Run requested suites and return a verification-oriented process exit code."""
    environment_error = verify_environment()
    if environment_error is not None:
        print(f"错误：{environment_error}", file=sys.stderr)
        return 2

    run_id = make_run_id()
    results_dir = RESULTS_ROOT / run_id
    logs_dir = LOGS_ROOT / run_id
    results_dir.mkdir(parents=True, exist_ok=False)
    logs_dir.mkdir(parents=True, exist_ok=False)
    summary_path = results_dir / "summary.json"
    suites = selected_suites(args.suite)
    started_at = utc_now()
    stages: list[dict[str, Any]] = []
    interrupted = False

    print(f"项目根目录：{PROJECT_ROOT}")
    print(f"虚拟环境解释器：{VENV_PYTHON}")
    print(f"运行 ID：{run_id}")

    for repeat_index in range(1, args.repeat + 1):
        for suite_name in suites:
            stage_id = f"{suite_name}-{repeat_index:02d}"
            print(f"[开始] {stage_id}（日志：{logs_dir / f'{stage_id}.console.log'}）")
            stage = run_stage(
                suite_name=suite_name,
                repeat_index=repeat_index,
                timeout_seconds=args.timeout,
                results_dir=results_dir,
                logs_dir=logs_dir,
            )
            stages.append(stage)
            print(stage_console_summary(stage))
            if not stage["passed"]:
                print(f"[失败详情] {stage['stage_id']}：日志 {stage['log_path']}", file=sys.stderr)
            if stage["status"] == "cancelled":
                interrupted = True
                break
        if interrupted:
            break

    overall_passed = bool(stages) and not interrupted and all(stage["passed"] for stage in stages)
    summary: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "project_root": str(PROJECT_ROOT),
        "venv_python": str(VENV_PYTHON),
        "requested_suite": args.suite,
        "repeat": args.repeat,
        "timeout_seconds_per_stage": args.timeout,
        "started_at_utc": started_at,
        "completed_at_utc": utc_now(),
        "interrupted": interrupted,
        "overall_passed": overall_passed,
        "stages": stages,
    }
    try:
        write_summary(summary_path, summary)
    except OSError as exc:
        message = f"错误：无法写入汇总文件 {summary_path}：{type(exc).__name__}: {exc}"
        print(message, file=sys.stderr)
        return 2

    print(f"完整 summary：{summary_path}")
    if interrupted:
        print("验收已取消；已保存当前阶段汇总。", file=sys.stderr)
        return 130
    if not overall_passed:
        print("验收未通过；请依据分阶段日志与 JUnit XML 排查。", file=sys.stderr)
        return 1
    print("验收通过。")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Parse CLI arguments and run verification."""
    parser = build_parser()
    args = parser.parse_args(argv)
    return run_verification(args)


if __name__ == "__main__":
    raise SystemExit(main())
