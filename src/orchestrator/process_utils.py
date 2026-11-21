"""Cross-platform non-interactive subprocess execution, process-tree kill, and port cleanup."""

from __future__ import annotations

import contextlib
import ctypes
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProcessRunResult:
    exit_code: int
    stdout: str
    stderr: str
    duration_seconds: float
    timed_out: bool


def resolve_executable(
    command: list[str],
    env: Mapping[str, str] | None = None,
) -> list[str]:
    """Resolve the executable via shutil.which so Windows .cmd/.bat wrappers work without shell=True."""
    if not command:
        raise ValueError("Command must not be empty")
    exe = command[0]
    path_env = env.get("PATH") if env is not None else os.environ.get("PATH")
    resolved = shutil.which(exe, path=path_env)
    if resolved is not None:
        return [resolved, *command[1:]]
    return list(command)


if sys.platform == "win32":
    from ctypes import wintypes

    _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS = 9
    _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000

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
            self._kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
            self._kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
            self._kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
            self._kernel32.TerminateJobObject.restype = wintypes.BOOL
            self._kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
            self._kernel32.CloseHandle.restype = wintypes.BOOL

            handle = self._kernel32.CreateJobObjectW(None, None)
            self._handle: int | None = int(handle) if handle else None
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
            return bool(self._kernel32.AssignProcessToJobObject(self._handle, process_handle))

        def terminate(self, exit_code: int = 1) -> None:
            if self._handle:
                self._kernel32.TerminateJobObject(self._handle, exit_code)

        def close(self) -> None:
            if self._handle:
                self._kernel32.CloseHandle(self._handle)
                self._handle = None


def _kill_process_tree(proc: subprocess.Popen[str], job: object | None = None) -> None:
    """Terminate a process and all its children reliably across Windows and POSIX."""
    if sys.platform == "win32":
        if job is not None and hasattr(job, "terminate"):
            with contextlib.suppress(Exception):
                job.terminate(1)
        try:
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
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


def _decode_stream_chunk(chunk: str | bytes | None) -> str:
    if chunk is None:
        return ""
    if isinstance(chunk, bytes):
        return chunk.decode("utf-8", errors="replace")
    return chunk


def run_noninteractive_process(
    command: list[str],
    *,
    cwd: Path,
    env: Mapping[str, str] | None = None,
    stdin_data: str | None = None,
    timeout_seconds: int = 900,
) -> ProcessRunResult:
    """Run a subprocess in strict non-interactive mode with guaranteed process-tree cleanup."""
    merged_env = dict(os.environ)
    if env is not None:
        merged_env.update(env)
    merged_env.setdefault("CI", "true")
    merged_env.setdefault("GIT_TERMINAL_PROMPT", "0")
    merged_env.setdefault("PYTHONUTF8", "1")
    merged_env.setdefault("PYTHONIOENCODING", "utf-8")
    merged_env.setdefault("PYTHONDONTWRITEBYTECODE", "1")

    resolved_cmd = resolve_executable(command, env=merged_env)
    stdin_arg = subprocess.PIPE if stdin_data is not None else subprocess.DEVNULL

    start_time = time.monotonic()
    job: object | None = None

    popen_kwargs: dict[str, object] = {
        "cwd": str(cwd),
        "env": merged_env,
        "stdin": stdin_arg,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True,
        "encoding": "utf-8",
        "errors": "replace",
        "shell": False,
    }
    if sys.platform == "win32":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
        create_no_window = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        popen_kwargs["creationflags"] = creationflags | create_no_window
    else:
        popen_kwargs["start_new_session"] = True

    proc: subprocess.Popen[str] = subprocess.Popen(resolved_cmd, **popen_kwargs)  # type: ignore[call-overload]
    if sys.platform == "win32":
        win_job = _WindowsJobObject()
        handle = getattr(proc, "_handle", None)
        if handle is not None:
            win_job.assign(int(handle))
        job = win_job

    timed_out = False
    stdout = ""
    stderr = ""
    exit_code = -1

    try:
        stdout, stderr = proc.communicate(input=stdin_data, timeout=timeout_seconds)
        exit_code = proc.returncode if proc.returncode is not None else -1
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        _kill_process_tree(proc, job=job)
        partial_out = _decode_stream_chunk(exc.stdout)
        partial_err = _decode_stream_chunk(exc.stderr)
        try:
            rem_out, rem_err = proc.communicate(timeout=5)
            stdout = partial_out + _decode_stream_chunk(rem_out)
            stderr = partial_err + _decode_stream_chunk(rem_err)
        except subprocess.TimeoutExpired:
            stdout = partial_out
            stderr = partial_err
        exit_code = proc.returncode if proc.returncode is not None else -1
    finally:
        if sys.platform == "win32" and job is not None and hasattr(job, "close"):
            job.close()

    duration = time.monotonic() - start_time
    return ProcessRunResult(
        exit_code=exit_code,
        stdout=stdout,
        stderr=stderr,
        duration_seconds=duration,
        timed_out=timed_out,
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
