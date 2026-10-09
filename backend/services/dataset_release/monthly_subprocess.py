"""Launch finite monthly commands without Windows console pipe inheritance."""

import subprocess


def headless_process_options() -> dict[str, int]:
    # A hidden supervisor child has no console. Creating one for its Git/SSH/
    # WSL children can leave conhost holding stdout/stderr after the command
    # exits, so communicate() never observes EOF (even after its timeout).
    # Zero is the unchanged POSIX subprocess contract.
    return {"creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0)}


def run_headless(command, **options):
    """Code-owned executor for injected node capabilities, not a shell."""
    options.update(headless_process_options())
    return subprocess.run(command, **options)
