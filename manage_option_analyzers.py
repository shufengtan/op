import concurrent.futures
from pathlib import Path
import signal
import subprocess
import sys
from typing import Dict, List, Tuple

# Configuration

MAX_WORKERS: int = 10
LOG_DIR: Path = Path.home() / "logs"
ACTIVE_PROCESSES: Dict[str, subprocess.Popen] = {}
ACTION = ""


def run_ssh_command(host: str, script_body: str, log_dir: Path) -> Tuple[str, int, Path]:
    safe_name = host.replace("@", "_").replace(":", "_")
    log_file = log_dir / f"{ACTION}-{safe_name}.log"

    # We send an inline bootstrap script via stdin.
    # We explicitly strip out the early return guard in memory when sourcing .bashrc,
    # guaranteeing that everything (PATH, Conda, NVM, modules) is loaded.
    remote_bootstrap = f"""
# Source system and user profiles
__conda_setup="$('/home/ana/conda/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/home/ana/conda/etc/profile.d/conda.sh" ]; then
        . "/home/ana/conda/etc/profile.d/conda.sh"
    else
        export PATH="/home/ana/conda/bin:$PATH"
    fi
fi
unset __conda_setup

# Run the actual user command payload
{script_body}
"""

    ssh_cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=3",
        host,
        "bash -s",  # Reads shell script directly from stdin
    ]

    try:
        with open(log_file, "w", encoding="utf-8", buffering=1) as f:
            proc = subprocess.Popen(
                ssh_cmd,
                stdin=subprocess.PIPE,
                stdout=f,
                stderr=subprocess.STDOUT,
                text=True,
            )
            ACTIVE_PROCESSES[host] = proc

            # Write the bootstrap payload into SSH stdin and close stdin so execution begins
            proc.stdin.write(remote_bootstrap)
            proc.stdin.close()

            returncode = proc.wait()
            return host, returncode, log_file

    except Exception as e:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"\n[ERROR] Process error on {host}: {e}\n")
        return host, -1, log_file
    finally:
        ACTIVE_PROCESSES.pop(host, None)


def handle_interrupt(signum, frame):
    print("\n[!] Terminating remote SSH connections...")
    for host, proc in list(ACTIVE_PROCESSES.items()):
        if proc.poll() is None:
            proc.terminate()
    sys.exit(0)


def run_parallel(hosts: List[str], script: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_interrupt)

    print(f"Logging output to: {LOG_DIR}")
    print(f"Running across {len(hosts)} hosts via SSH stdin stream (Press Ctrl+C to stop)...\n")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_host = {
            executor.submit(run_ssh_command, host, script, LOG_DIR): host
            for host in hosts
        }

        for future in concurrent.futures.as_completed(future_to_host):
            try:
                host, return_code, log_path = future.result()
                status = f"EXITED with return code {return_code}"
                print(f"[{host}] -> {status}, Log: {log_path}:")
                with open(log_path) as fo:
                    log_lines = [_ for _ in fo][-10:]
                    print(''.join(log_lines))
            except Exception as e:
                print(f"Execution failed: {e}")


if __name__ == "__main__":
    import sys
    if 'run' in sys.argv[0]:
        command = "cd lab; while true; do sync; python option_analyzer.py symbols-$(hostname).txt;sleep 1; done"
        ACTION = 'run'
    elif 'check' in sys.argv[0]:
        command = "ps -fu s |grep option_analyzer|grep -v grep"
        ACTION = 'check'
    elif 'kill' in sys.argv[0]:
        command = "ps -fu s |grep 'python option_analyzer'|grep -v grep|awk '{print $2}' | xargs kill"
        ACTION = 'kill'
    else:
        sys.stderr.write(f"Invalid command: {sys.argv[0]}\n")
        sys.exit(1)
    hosts = sys.argv[1:]
    run_parallel(hosts, command)
