"""Actual PTY entrypoint checks; headless widgets cannot verify terminal modes."""
import fcntl
import os
from pathlib import Path
import pty
import select
import struct
import subprocess
import sys
import termios
import time

ROOT=Path(__file__).resolve().parents[1]


def read_until(fd,needle,*,timeout=8):
    output=bytearray();deadline=time.monotonic()+timeout
    while needle not in output and time.monotonic()<deadline:
        if select.select([fd],[],[],.1)[0]:
            try:part=os.read(fd,65536)
            except OSError:break
            if not part:break
            output.extend(part)
            assert len(output)<1024*1024,'Terminal output grew without bound'
    assert needle in output,repr(output[-2000:])
    return bytes(output)


def test_tui_rejects_non_terminal_input(tmp_path):
    result=subprocess.run([sys.executable,'main.py','tui','--socket',str(tmp_path/'none')],
        cwd=ROOT,capture_output=True,timeout=5)
    assert result.returncode==2 and b'terminal' in result.stderr.lower()


def test_real_terminal_quit_restores_modes_with_missing_service(tmp_path):
    master,slave=pty.openpty()
    fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',28,100,0,0))
    before=termios.tcgetattr(slave)
    process=subprocess.Popen([sys.executable,'main.py','tui','--socket',str(tmp_path/'none')],
        cwd=ROOT,stdin=slave,stdout=slave,stderr=slave,
        env={**os.environ,'TERM':'xterm-256color'})
    try:
        read_until(master,b'SYMPHONY')
        os.write(master,b'q')
        process.wait(timeout=5)
        assert process.returncode==0
        assert termios.tcgetattr(slave)==before
        assert not (tmp_path/'none').exists()
    finally:
        if process.poll() is None:process.kill();process.wait(timeout=5)
        os.close(master);os.close(slave)
