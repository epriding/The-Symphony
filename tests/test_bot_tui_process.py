"""Actual PTY entrypoint checks; headless widgets cannot verify terminal modes."""
import fcntl
import asyncio
import json
import os
from pathlib import Path
import pty
import select
import struct
import subprocess
import sys
import termios
import time
import pytest

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


# The fixture's stdin drives real feed events; it is not a new product RPC.
ACTIVE_SERVICE = r'''
import asyncio,json,signal,sys
from pathlib import Path
sys.path.insert(0,'tests')
from test_bot_service import controlled_service
from bot_v1_support import eventually
async def run():
    service,feeds,clock=controlled_service(Path(sys.argv[2]))
    service.config['runtime'].update(startup_timeout_seconds=2,shutdown_grace_seconds=2)
    risks=[0];original=service._observe
    def observer(runner,kind,payload):
        if kind=='control' and payload.get('action')=='risk_tick':risks[0]+=1
        original(runner,kind,payload)
    service._observe=observer
    await service.start(sys.argv[1])
    commands=asyncio.Queue(maxsize=8);stopped=asyncio.Event()
    loop=asyncio.get_running_loop()
    def incoming():
        line=sys.stdin.readline()
        if line:commands.put_nowait(line.strip())
        else:loop.remove_reader(sys.stdin.fileno());stopped.set()
    loop.add_reader(sys.stdin.fileno(),incoming)
    loop.add_signal_handler(signal.SIGTERM,stopped.set)
    async def steps():
        while True:
            assert await commands.get()=='step'
            await eventually(lambda:all(feed.started.is_set() for feed in feeds))
            revision,risk=service.runner._revision,risks[0]
            clock.advance(clock()+.1)
            for feed in feeds:await feed.push_book()
            await eventually(lambda:service.runner._revision>revision and risks[0]>risk)
            print(json.dumps(dict(session=service.session_id,revision=service.runner._revision,risk=risks[0])),flush=True)
    worker=asyncio.create_task(steps())
    print('ready',flush=True)
    try:await stopped.wait()
    finally:
        loop.remove_reader(sys.stdin.fileno());worker.cancel()
        await asyncio.gather(worker,return_exceptions=True)
        await service.close()
asyncio.run(run())
'''


def open_terminal(path, *, env_extra=None, code=None):
    master,slave=pty.openpty()
    fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',28,100,0,0))
    before=termios.tcgetattr(slave)
    command=([sys.executable,'main.py','tui','--socket',str(path)] if code is None else
        [sys.executable,'-c',code,str(path)])
    process=subprocess.Popen(command,
        cwd=ROOT,stdin=slave,stdout=slave,stderr=slave,
        env={**os.environ,'TERM':'xterm-256color',**(env_extra or {})})
    return process,master,slave,before


@pytest.mark.asyncio
@pytest.mark.parametrize('exit_kind',['quit','interrupt','kill'])
async def test_actual_terminal_detach_keeps_same_bot_processing_inputs_and_risk(tmp_path,exit_kind,request):
    from src.operator.client import RuntimeClient
    from test_bot_service import child_line
    path=tmp_path/'private/control.sock'
    service=subprocess.Popen([sys.executable,'-u','-c',ACTIVE_SERVICE,str(path),str(tmp_path)],
        cwd=ROOT,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    terminals=[];operator=RuntimeClient(path)
    async def step():
        service.stdin.write('step\n');service.stdin.flush()
        return json.loads(await child_line(service))
    try:
        assert (await child_line(service)).strip()=='ready'
        hello=await operator.request('overview')
        started=await operator.request('start',target=(hello['service_id'],None))
        target=(hello['service_id'],started['session_id'])
        previous=await step()
        initial=dict(previous)
        for attempt in range(2):
            process,master,slave,before=open_terminal(path)
            terminals.append((process,master,slave))
            await asyncio.to_thread(read_until,master,target[1].encode())
            kind=exit_kind if attempt==0 else 'quit'
            if kind=='kill':process.kill()
            else:os.write(master,b'q' if kind=='quit' else b'\x03')
            await asyncio.to_thread(process.wait,5)
            assert process.returncode==({'quit':0,'interrupt':130}.get(kind,-9))
            if kind!='kill':assert termios.tcgetattr(slave)==before
            current=await step()
            assert current['session']==previous['session']==target[1]
            assert current['revision']>previous['revision'] and current['risk']>previous['risk']
            view=await operator.request('overview',target=target)
            assert view['data']['lifecycle']=='ready' and service.poll() is None
            previous=current
        assert len(list((tmp_path/'runs').iterdir()))==1
        request.node.user_properties.extend([('session_id',target[1]),
            ('initial_revision',initial['revision']),('final_revision',previous['revision']),
            ('initial_risk',initial['risk']),('final_risk',previous['risk']),('exit_kind',exit_kind)])
        service.terminate();await asyncio.to_thread(service.wait,6)
        assert service.returncode==0,service.stderr.read()
    finally:
        await operator.close()
        for process,master,slave in terminals:
            if process.poll() is None:process.kill();await asyncio.to_thread(process.wait,5)
            os.close(master);os.close(slave)
        if service.poll() is None:service.kill();await asyncio.to_thread(service.wait,5)
        service.stdin.close();service.stdout.close();service.stderr.close()


def test_actual_terminal_resize_restores_body_and_modes(tmp_path):
    import signal
    process,master,slave,before=open_terminal(tmp_path/'none')
    try:
        read_until(master,b'SYMPHONY')
        fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',20,60,0,0))
        process.send_signal(signal.SIGWINCH)
        read_until(master,b'Resize terminal')
        fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',28,100,0,0))
        process.send_signal(signal.SIGWINCH)
        read_until(master,b'EXECUTION')
        os.write(master,b'q');process.wait(timeout=5)
        assert process.returncode==0 and termios.tcgetattr(slave)==before
    finally:
        if process.poll() is None:process.kill();process.wait(timeout=5)
        os.close(master);os.close(slave)


def test_actual_terminal_ascii_monochrome_output_and_quit(tmp_path):
    process,master,slave,before=open_terminal(tmp_path/'none',
        env_extra={'NO_COLOR':'1','PYTHONIOENCODING':'ascii'})
    try:
        output=read_until(master,b'Disconnected')
        assert 'SYMPHONY' in output.decode('ascii')
        os.write(master,b'q');process.wait(timeout=5)
        assert process.returncode==0 and termios.tcgetattr(slave)==before
    finally:
        if process.poll() is None:process.kill();process.wait(timeout=5)
        os.close(master);os.close(slave)


def test_actual_terminal_unrecoverable_client_failure_restores_modes(tmp_path):
    code='''
import sys
from src.operator.client import RuntimeClient
async def failed(*args,**kwargs):
    raise RuntimeError('synthetic unrecoverable client failure')
RuntimeClient.request=failed
import main
raise SystemExit(main.main(['tui','--socket',sys.argv[1]]))
'''
    process,master,slave,before=open_terminal(tmp_path/'none',code=code)
    try:
        read_until(master,b'synthetic unrecoverable client failure')
        process.wait(timeout=5)
        assert process.returncode==1 and termios.tcgetattr(slave)==before
    finally:
        if process.poll() is None:process.kill();process.wait(timeout=5)
        os.close(master);os.close(slave)
