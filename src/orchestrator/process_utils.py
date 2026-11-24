"""Cross-platform non-interactive subprocess execution, process-tree kill, and port cleanup."""

from __future__ import annotations

import codecs
import contextlib
import ctypes
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

_ANSI_ESCAPE_RE = re.compile(
    r"(?:\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b[@-Z\\-_])"
)

_DEFAULT_WORKSPACE_WATCH_IGNORES: frozenset[str] = frozenset(
    {
        ".git",
        ".pipeline",
        ".venv",
        "venv",
        "node_modules",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
    }
)


def strip_ansi(text: str) -> str:
    """Strip ANSI escape sequences and carriage returns from CLI output."""
    return _ANSI_ESCAPE_RE.sub("", text).replace("\r", "")


def is_meaningful_output(chunk: str) -> bool:
    """Return True if chunk contains non-whitespace content after stripping ANSI codes."""
    return bool(strip_ansi(chunk).strip())


def _workspace_activity_signature(
    watch_dir: Path,
    ignore_dirs: set[str] | frozenset[str],
) -> tuple[int, float]:
    """Return (entry_count, latest_mtime) for files/dirs outside ignored directories."""
    count = 0
    latest_mtime = 0.0
    try:
        stack: list[Path] = [watch_dir]
        while stack:
            current = stack.pop()
            try:
                with os.scandir(current) as it:
                    for entry in it:
                        name = entry.name
                        if name in ignore_dirs:
                            continue
                        try:
                            stat = entry.stat(follow_symlinks=False)
                        except OSError:
                            continue
                        count += 1
                        if stat.st_mtime > latest_mtime:
                            latest_mtime = stat.st_mtime
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(Path(entry.path))
            except OSError:
                continue
    except OSError:
        pass
    return count, latest_mtime


@dataclass(frozen=True)
class ProcessRunResult:
    exit_code: int
    stdout: str
    stderr: str
    duration_seconds: float
    timed_out: bool
    idle_timed_out: bool = False
    received_meaningful_output: bool = False


def resolve_executable(
    command: list[str],
    env: Mapping[str, str] | None = None,
    *,
    cwd: Path | None = None,
) -> list[str]:
    """Resolve the executable via shutil.which or relative repo path so Windows wrappers work without shell=True."""
    if not command:
        raise ValueError("Command must not be empty")
    exe = command[0]
    path_env = env.get("PATH") if env is not None else os.environ.get("PATH")
    resolved = shutil.which(exe, path=path_env)
    if resolved is not None:
        return [resolved, *command[1:]]
    if cwd is not None:
        local_candidate = Path(exe) if Path(exe).is_absolute() else (cwd / exe)
        local_resolved = shutil.which(str(local_candidate))
        if local_resolved is not None:
            return [local_resolved, *command[1:]]
        if local_candidate.is_file():
            return [str(local_candidate), *command[1:]]
    return list(command)


if sys.platform == "win32":
    from ctypes import wintypes

    _JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION_CLASS = 1
    _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS = 9
    _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000

    class _FILETIME(ctypes.Structure):
        _fields_ = [
            ("dwLowDateTime", wintypes.DWORD),
            ("dwHighDateTime", wintypes.DWORD),
        ]

    class _JOBOBJECT_BASIC_ACCOUNTING_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("TotalUserTime", wintypes.LARGE_INTEGER),
            ("TotalKernelTime", wintypes.LARGE_INTEGER),
            ("ThisPeriodTotalUserTime", wintypes.LARGE_INTEGER),
            ("ThisPeriodTotalKernelTime", wintypes.LARGE_INTEGER),
            ("TotalPageFaultCount", wintypes.DWORD),
            ("TotalProcesses", wintypes.DWORD),
            ("ActiveProcesses", wintypes.DWORD),
            ("TotalTerminatedProcesses", wintypes.DWORD),
        ]

    class _IO_COUNTERS(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_uint64),
            ("WriteOperationCount", ctypes.c_uint64),
            ("OtherOperationCount", ctypes.c_uint64),
            ("ReadTransferCount", ctypes.c_uint64),
            ("WriteTransferCount", ctypes.c_uint64),
            ("OtherTransferCount", ctypes.c_uint64),
        ]

    class _JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", wintypes.LARGE_INTEGER),
            ("PerJobUserTimeLimit", wintypes.LARGE_INTEGER),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class _JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", _JOBOBJECT_BASIC_LIMIT_INFORMATION),
            ("IoInfo", _IO_COUNTERS),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    class _WindowsJobObject:
        """RAII wrapper for a Win32 Job Object configured to kill all descendants."""

        def __init__(self) -> None:
            self._kernel32 = ctypes.windll.kernel32
            self._kernel32.CreateJobObjectW.argtypes = [wintypes.LPVOID, wintypes.LPCWSTR]
            self._kernel32.CreateJobObjectW.restype = wintypes.HANDLE
            self._kernel32.SetInformationJobObject.argtypes = [
                wintypes.HANDLE,
                wintypes.INT,
                wintypes.LPVOID,
                wintypes.DWORD,
            ]
            self._kernel32.SetInformationJobObject.restype = wintypes.BOOL
            self._kernel32.QueryInformationJobObject.argtypes = [
                wintypes.HANDLE,
                wintypes.INT,
                wintypes.LPVOID,
                wintypes.DWORD,
                ctypes.POINTER(wintypes.DWORD),
            ]
            self._kernel32.QueryInformationJobObject.restype = wintypes.BOOL
            self._kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
            self._kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
            self._kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
            self._kernel32.TerminateJobObject.restype = wintypes.BOOL
            self._kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
            self._kernel32.CloseHandle.restype = wintypes.BOOL

            handle = self._kernel32.CreateJobObjectW(None, None)
            self._handle: int | None = int(handle) if handle else None
            self._assigned: bool = False
            if self._handle:
                info = _JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
                info.BasicLimitInformation.LimitFlags = _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
                ok = self._kernel32.SetInformationJobObject(
                    self._handle,
                    _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS,
                    ctypes.byref(info),
                    ctypes.sizeof(info),
                )
                if not ok:
                    self.close()

        def assign(self, process_handle: int) -> bool:
            if not self._handle:
                return False
            ok = bool(self._kernel32.AssignProcessToJobObject(self._handle, process_handle))
            if ok:
                self._assigned = True
            return ok

        def query_cpu_seconds(self) -> float | None:
            """Return cumulative user + kernel CPU seconds for all processes in this Job Object."""
            if not self._handle or not self._assigned:
                return None
            info = _JOBOBJECT_BASIC_ACCOUNTING_INFORMATION()
            returned = wintypes.DWORD(0)
            ok = self._kernel32.QueryInformationJobObject(
                self._handle,
                _JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION_CLASS,
                ctypes.byref(info),
                ctypes.sizeof(info),
                ctypes.byref(returned),
            )
            if not ok:
                return None
            total_100ns = int(info.TotalUserTime) + int(info.TotalKernelTime)
            return total_100ns / 10_000_000.0

        def terminate(self, exit_code: int = 1) -> bool:
            if self._handle:
                return bool(self._kernel32.TerminateJobObject(self._handle, exit_code))
            return False

        def close(self) -> None:
            if self._handle:
                self._kernel32.CloseHandle(self._handle)
                self._handle = None
                self._assigned = False


def _query_process_cpu_seconds_windows_fallback(proc: subprocess.Popen[bytes]) -> float | None:
    """Fallback CPU time query via Win32 GetProcessTimes for the main process."""
    if sys.platform != "win32":
        return None
    from ctypes import wintypes

    kernel32 = ctypes.windll.kernel32
    kernel32.GetProcessTimes.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(_FILETIME),
        ctypes.POINTER(_FILETIME),
        ctypes.POINTER(_FILETIME),
        ctypes.POINTER(_FILETIME),
    ]
    kernel32.GetProcessTimes.restype = wintypes.BOOL

    handle = getattr(proc, "_handle", None)
    opened_handle: int | None = None
    try:
        if handle is None:
            process_query_limited_info = 0x1000
            kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            kernel32.OpenProcess.restype = wintypes.HANDLE
            kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
            kernel32.CloseHandle.restype = wintypes.BOOL
            opened = kernel32.OpenProcess(process_query_limited_info, False, proc.pid)
            if not opened:
                return None
            opened_handle = int(opened)
            target_handle = opened_handle
        else:
            target_handle = int(handle)

        creation = _FILETIME()
        exit_ft = _FILETIME()
        kernel = _FILETIME()
        user = _FILETIME()
        ok = kernel32.GetProcessTimes(
            target_handle,
            ctypes.byref(creation),
            ctypes.byref(exit_ft),
            ctypes.byref(kernel),
            ctypes.byref(user),
        )
        if not ok:
            return None
        k_100ns = (int(kernel.dwHighDateTime) << 32) | int(kernel.dwLowDateTime)
        u_100ns = (int(user.dwHighDateTime) << 32) | int(user.dwLowDateTime)
        return (k_100ns + u_100ns) / 10_000_000.0
    except Exception:
        return None
    finally:
        if opened_handle is not None:
            with contextlib.suppress(Exception):
                kernel32.CloseHandle(opened_handle)


def _query_process_cpu_seconds_posix(pid: int) -> float | None:
    """Read cumulative process + waited-children CPU seconds from /proc/<pid>/stat on POSIX."""
    try:
        stat_text = Path(f"/proc/{pid}/stat").read_text(encoding="ascii", errors="replace")
        rparen = stat_text.rfind(")")
        if rparen < 0:
            return None
        fields = stat_text[rparen + 2 :].split()
        if len(fields) < 15:
            return None
        utime = int(fields[11])
        stime = int(fields[12])
        cutime = int(fields[13])
        cstime = int(fields[14])
        clk_tck = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
        if clk_tck <= 0:
            clk_tck = 100
        return (utime + stime + cutime + cstime) / float(clk_tck)
    except Exception:
        return None


def _query_process_tree_cpu_seconds(
    proc: subprocess.Popen[bytes],
    job: object | None = None,
) -> float | None:
    """Return cumulative CPU seconds for the process tree (Job Object on Windows, /proc on POSIX)."""
    if sys.platform == "win32":
        if job is not None and hasattr(job, "query_cpu_seconds"):
            with contextlib.suppress(Exception):
                val = job.query_cpu_seconds()
                if val is not None:
                    return float(val)
        return _query_process_cpu_seconds_windows_fallback(proc)
    return _query_process_cpu_seconds_posix(proc.pid)


def _kill_process_tree(proc: subprocess.Popen[bytes], job: object | None = None) -> None:
    """Terminate a process and all its children reliably across Windows and POSIX."""
    if sys.platform == "win32":
        job_terminated = False
        if job is not None and hasattr(job, "terminate"):
            with contextlib.suppress(Exception):
                job_terminated = bool(job.terminate(1))
        if job_terminated:
            with contextlib.suppress(subprocess.TimeoutExpired, OSError):
                proc.wait(timeout=0.5)
        if not job_terminated or proc.poll() is None:
            try:
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=5,
                    check=False,
                )
            except Exception:
                with contextlib.suppress(OSError):
                    proc.kill()
        return

    try:
        pgid = os.getpgid(proc.pid)
        os.killpg(pgid, signal.SIGTERM)
        try:
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except Exception:
        with contextlib.suppress(OSError):
            proc.kill()


def run_noninteractive_process(
    command: list[str],
    *,
    cwd: Path,
    env: Mapping[str, str] | None = None,
    stdin_data: str | None = None,
    timeout_seconds: int = 900,
    idle_timeout_seconds: float | None = None,
    on_first_output: Callable[[str], None] | None = None,
    on_stdout_chunk: Callable[[str], None] | None = None,
    on_stderr_chunk: Callable[[str], None] | None = None,
    workspace_watch_dir: Path | None = None,
    workspace_ignore_dirs: set[str] | frozenset[str] | None = None,
    idle_check_process_cpu: bool = True,
    idle_cpu_min_delta_seconds: float = 0.05,
) -> ProcessRunResult:
    """Run a subprocess in strict non-interactive mode with streaming stdout/stderr and idle/hard timeout."""
    merged_env = dict(os.environ)
    if env is not None:
        merged_env.update(env)
    merged_env.setdefault("CI", "true")
    merged_env.setdefault("GIT_TERMINAL_PROMPT", "0")
    merged_env.setdefault("PYTHONUTF8", "1")
    merged_env.setdefault("PYTHONIOENCODING", "utf-8")
    merged_env.setdefault("PYTHONDONTWRITEBYTECODE", "1")

    resolved_cmd = resolve_executable(command, env=merged_env, cwd=cwd)
    stdin_arg = subprocess.PIPE if stdin_data is not None else subprocess.DEVNULL

    start_time = time.monotonic()
    job: object | None = None

    popen_kwargs: dict[str, object] = {
        "cwd": str(cwd),
        "env": merged_env,
        "stdin": stdin_arg,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "bufsize": 0,
        "shell": False,
    }
    if sys.platform == "win32":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
        create_no_window = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        popen_kwargs["creationflags"] = creationflags | create_no_window
    else:
        popen_kwargs["start_new_session"] = True

    proc: subprocess.Popen[bytes] = subprocess.Popen(resolved_cmd, **popen_kwargs)  # type: ignore[call-overload]
    if sys.platform == "win32":
        win_job = _WindowsJobObject()
        handle = getattr(proc, "_handle", None)
        if handle is not None:
            win_job.assign(int(handle))
        job = win_job

    lock = threading.Lock()
    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []
    last_activity = [start_time]
    received_meaningful = [False]

    def _write_stdin() -> None:
        if proc.stdin is None or stdin_data is None:
            return
        try:
            proc.stdin.write(stdin_data.encode("utf-8"))
            proc.stdin.flush()
        except OSError:
            pass
        finally:
            with contextlib.suppress(OSError):
                proc.stdin.close()

    def _read_stream(
        stream: object,
        *,
        is_stdout: bool,
    ) -> None:
        if stream is None:
            return
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        try:
            fd = stream.fileno()  # type: ignore[attr-defined]
        except Exception:
            fd = None

        try:
            while True:
                try:
                    raw = os.read(fd, 4096) if fd is not None else stream.read(4096)  # type: ignore[attr-defined]
                except OSError:
                    break
                if not raw:
                    break
                text = decoder.decode(raw, final=False)
                if not text:
                    with lock:
                        last_activity[0] = time.monotonic()
                    continue
                fire_first = False
                with lock:
                    last_activity[0] = time.monotonic()
                    if is_stdout:
                        stdout_chunks.append(text)
                        if not received_meaningful[0] and is_meaningful_output(text):
                            received_meaningful[0] = True
                            fire_first = True
                    else:
                        stderr_chunks.append(text)
                if fire_first and on_first_output is not None:
                    with contextlib.suppress(Exception):
                        on_first_output(text)
                if is_stdout and on_stdout_chunk is not None:
                    with contextlib.suppress(Exception):
                        on_stdout_chunk(text)
                elif not is_stdout and on_stderr_chunk is not None:
                    with contextlib.suppress(Exception):
                        on_stderr_chunk(text)
        finally:
            tail = decoder.decode(b"", final=True)
            if tail:
                fire_first_tail = False
                with lock:
                    if is_stdout:
                        stdout_chunks.append(tail)
                        if not received_meaningful[0] and is_meaningful_output(tail):
                            received_meaningful[0] = True
                            fire_first_tail = True
                    else:
                        stderr_chunks.append(tail)
                if fire_first_tail and on_first_output is not None:
                    with contextlib.suppress(Exception):
                        on_first_output(tail)
            with contextlib.suppress(OSError):
                stream.close()  # type: ignore[attr-defined]

    stdin_thread: threading.Thread | None = None
    if stdin_data is not None:
        stdin_thread = threading.Thread(target=_write_stdin, daemon=True)
        stdin_thread.start()

    stdout_thread = threading.Thread(
        target=_read_stream,
        args=(proc.stdout,),
        kwargs={"is_stdout": True},
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=_read_stream,
        args=(proc.stderr,),
        kwargs={"is_stdout": False},
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    watch_dir = workspace_watch_dir if workspace_watch_dir is not None else cwd
    ignore_set = (
        workspace_ignore_dirs
        if workspace_ignore_dirs is not None
        else _DEFAULT_WORKSPACE_WATCH_IGNORES
    )
    last_ws_sig = (
        _workspace_activity_signature(watch_dir, ignore_set)
        if idle_timeout_seconds is not None
        else None
    )
    last_cpu_seconds = (
        _query_process_tree_cpu_seconds(proc, job)
        if (idle_timeout_seconds is not None and idle_check_process_cpu)
        else None
    )
    last_ws_check = start_time
    ws_check_interval = (
        min(1.0, max(0.05, idle_timeout_seconds / 4.0))
        if idle_timeout_seconds is not None
        else 1.0
    )
    poll_interval = (
        min(0.1, max(0.02, idle_timeout_seconds / 10.0))
        if idle_timeout_seconds is not None
        else 0.1
    )

    timed_out = False
    idle_timed_out = False
    interrupted = False
    exit_code = -1

    try:
        while True:
            rc = proc.poll()
            if rc is not None:
                exit_code = rc
                break
            now = time.monotonic()
            if now - start_time >= timeout_seconds:
                timed_out = True
                _kill_process_tree(proc, job=job)
                with contextlib.suppress(subprocess.TimeoutExpired):
                    proc.wait(timeout=5)
                exit_code = proc.returncode if proc.returncode is not None else -1
                break
            if idle_timeout_seconds is not None:
                if now - last_ws_check >= ws_check_interval:
                    cur_sig = _workspace_activity_signature(watch_dir, ignore_set)
                    last_ws_check = now
                    activity_detected = False
                    if last_ws_sig is not None and cur_sig != last_ws_sig:
                        last_ws_sig = cur_sig
                        activity_detected = True
                    if idle_check_process_cpu:
                        cur_cpu = _query_process_tree_cpu_seconds(proc, job)
                        if cur_cpu is not None:
                            if last_cpu_seconds is None:
                                last_cpu_seconds = cur_cpu
                            elif cur_cpu - last_cpu_seconds >= idle_cpu_min_delta_seconds:
                                last_cpu_seconds = cur_cpu
                                activity_detected = True
                    if activity_detected:
                        with lock:
                            last_activity[0] = now
                with lock:
                    idle_for = now - last_activity[0]
                if idle_for >= idle_timeout_seconds:
                    # Final workspace mtime & process-tree CPU check before killing
                    cur_sig = _workspace_activity_signature(watch_dir, ignore_set)
                    still_active = False
                    if last_ws_sig is not None and cur_sig != last_ws_sig:
                        last_ws_sig = cur_sig
                        still_active = True
                    if idle_check_process_cpu:
                        cur_cpu = _query_process_tree_cpu_seconds(proc, job)
                        if cur_cpu is not None:
                            if last_cpu_seconds is None:
                                last_cpu_seconds = cur_cpu
                            elif cur_cpu - last_cpu_seconds >= idle_cpu_min_delta_seconds:
                                last_cpu_seconds = cur_cpu
                                still_active = True
                    if still_active:
                        with lock:
                            last_activity[0] = now
                    else:
                        timed_out = True
                        idle_timed_out = True
                        _kill_process_tree(proc, job=job)
                        with contextlib.suppress(subprocess.TimeoutExpired):
                            proc.wait(timeout=5)
                        exit_code = proc.returncode if proc.returncode is not None else -1
                        break
            time.sleep(poll_interval)
    except BaseException:
        interrupted = True
        _kill_process_tree(proc, job=job)
        with contextlib.suppress(subprocess.TimeoutExpired, OSError):
            proc.wait(timeout=1.5)
        if sys.platform == "win32" and job is not None and hasattr(job, "close"):
            with contextlib.suppress(Exception):
                job.close()
            job = None
        for stream in (proc.stdin, proc.stdout, proc.stderr):
            if stream is not None:
                with contextlib.suppress(OSError, ValueError):
                    stream.close()
        raise
    finally:
        if interrupted:
            join_timeout = 1.5
        elif timed_out:
            join_timeout = 5.0
        else:
            join_timeout = 15.0
        if interrupted or timed_out:
            for stream in (proc.stdout, proc.stderr):
                if stream is not None:
                    with contextlib.suppress(OSError, ValueError):
                        stream.close()
        stdout_thread.join(timeout=join_timeout)
        stderr_thread.join(timeout=join_timeout)
        if stdin_thread is not None:
            stdin_thread.join(timeout=1.0 if interrupted else 2.0)
        if sys.platform == "win32" and job is not None and hasattr(job, "close"):
            with contextlib.suppress(Exception):
                job.close()

    with lock:
        stdout = "".join(stdout_chunks).replace("\r\n", "\n")
        stderr = "".join(stderr_chunks).replace("\r\n", "\n")
        meaningful = received_meaningful[0]

    duration = time.monotonic() - start_time
    return ProcessRunResult(
        exit_code=exit_code,
        stdout=stdout,
        stderr=stderr,
        duration_seconds=duration,
        timed_out=timed_out,
        idle_timed_out=idle_timed_out,
        received_meaningful_output=meaningful,
    )


def cleanup_ports(ports: list[int]) -> list[int]:
    """Terminate lingering processes listening on the specified TCP ports. Returns killed PIDs."""
    if not ports:
        return []
    target_ports = set(ports)
    pids: set[int] = set()
    own_pid = os.getpid()

    if sys.platform == "win32":
        res = subprocess.run(
            ["netstat", "-ano", "-p", "TCP"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
            check=False,
        )
        for line in res.stdout.splitlines():
            if "LISTENING" not in line.upper():
                continue
            parts = line.split()
            if len(parts) < 5:
                continue
            local_addr = parts[1]
            pid_str = parts[-1]
            match = re.search(r":(\d+)$", local_addr)
            if not match:
                continue
            port = int(match.group(1))
            if port in target_ports and pid_str.isdigit():
                pid = int(pid_str)
                if pid > 0 and pid != own_pid:
                    pids.add(pid)
        killed: list[int] = []
        for pid in sorted(pids):
            kill_res = subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
                check=False,
            )
            if kill_res.returncode == 0:
                killed.append(pid)
        return killed

    killed_posix: list[int] = []
    for port in sorted(target_ports):
        if shutil.which("lsof"):
            res = subprocess.run(
                ["lsof", "-ti", f"tcp:{port}"],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=10,
                check=False,
            )
            for token in res.stdout.split():
                if token.isdigit():
                    pid = int(token)
                    if pid > 0 and pid != own_pid:
                        pids.add(pid)
    for pid in sorted(pids):
        try:
            os.kill(pid, signal.SIGKILL)
            killed_posix.append(pid)
        except OSError:
            pass
    return killed_posix


def is_process_alive(pid: int) -> bool:
    """Return True if a process with the given PID is currently running (Windows & POSIX)."""
    if pid <= 0:
        return False
    if pid == os.getpid():
        return True

    if sys.platform == "win32":
        from ctypes import wintypes

        process_query_limited_info = 0x1000
        still_active = 259
        error_access_denied = 5

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        kernel32.GetExitCodeProcess.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL

        handle = kernel32.OpenProcess(process_query_limited_info, False, pid)
        if not handle:
            return ctypes.get_last_error() == error_access_denied
        try:
            exit_code = wintypes.DWORD()
            ok = bool(kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)))
            return ok and int(exit_code.value) == still_active
        finally:
            kernel32.CloseHandle(handle)

    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
