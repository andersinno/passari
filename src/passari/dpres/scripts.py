import asyncio
import datetime
import os
from pathlib import Path
from subprocess import CalledProcessError

import aiofiles
from passari.logger import logger


async def run_command(
        cmd: list, cwd: str = None, log_path=None, environ=None) -> str:
    """
    Run a command.
    If 'log_path' is provided, stdout and stderr will be written to this
    location regardless of the end result.

    :raises subprocess.CalledProcessError: If the command returned an error

    :returns: Command stdout
    """
    if not environ:
        environ = os.environ.copy()

    process = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
        env=environ
    )
    stdout, stderr = await process.communicate()

    if log_path:
        async with aiofiles.open(log_path, "ab") as file_:
            now = datetime.datetime.now(datetime.timezone.utc)
            await file_.write(
                f"\n===COMMAND===\n{now.isoformat()}\n{cmd}".encode("utf-8")
            )
            await file_.write(b"\n===CWD===\n")
            await file_.write((cwd or os.getcwd()).encode("utf-8"))
            await file_.write(b"\n===STDOUT===\n")
            await file_.write(stdout)
            await file_.write(b"\n===STDERR===\n")
            await file_.write(stderr)

    if process.returncode != 0:
        raise CalledProcessError(
            returncode=process.returncode,
            cmd=cmd,
            output=stdout,
            stderr=stderr
        )

    logger.debug(
        "Command %s completed.\nOUTPUT: %s\n",
        " ".join(cmd), stdout
    )

    return stdout.decode("utf-8")

async def extract_archive(path, destination_path, log_path=None):
    """
    Extract an archive to the given directory and delete the original
    archive afterwards
    """
    path = Path(path)

    if path.suffix.lower() == ".zip":
        return await run_command(
            cmd=["unzip", str(path), "-d", str(destination_path)],
            log_path=log_path
        )

    raise RuntimeError(f"Can't extract archive {path}")
