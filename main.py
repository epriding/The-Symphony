"""Paper bot commands; importing the entrypoint never starts a session."""
import argparse
import asyncio
import json
import os
import signal
import sys

from src.bot import BotRunner, load_bot_config, replay_bot
from src.utilities.logger import redact


async def _serve(args: argparse.Namespace, *, force_exit_on_unclean: bool = False) -> None:
    from src.operator.service import ControlService
    if args.smoke:
        service=ControlService(smoke=True,output_root=args.output_root)
    else:
        config=load_bot_config(args.config)
        if config.get('runtime',{}).get('source')!='public':
            raise ValueError('serve --config requires a public source; use --smoke for offline fixtures')
        service=ControlService(config,output_root=args.output_root)
    loop=asyncio.get_running_loop()
    stopped=asyncio.Event()
    installed=[]
    try:
        try:
            await service.start(args.socket)
        except OSError:
            raise ValueError('Cannot bind the private control socket') from None
        for sig in (signal.SIGINT,signal.SIGTERM):
            loop.add_signal_handler(sig,stopped.set)
            installed.append(sig)
        print('Bot control service ready (idle; connect a client to start a session).',
            file=sys.stderr,flush=True)
        await stopped.wait()
    finally:
        try:
            try:
                await service.close()
            except BaseException:
                if force_exit_on_unclean and service.requires_forced_exit():
                    # asyncio.run otherwise joins cancellation-resistant tasks
                    # or blocked executor threads indefinitely. This policy is
                    # restricted to the standalone service process, after its
                    # normal cleanup deadlines; the library never exits callers.
                    print('Runtime failed: control-service cleanup incomplete; exiting with failure.',
                        file=sys.stderr,flush=True)
                    os._exit(1)
                raise
        finally:
            for sig in installed:
                loop.remove_signal_handler(sig)


async def _execute(args):
    if args.command == 'replay':
        return await replay_bot(args.capture, args.output)
    if args.command == 'smoke':
        from src.strategies.smoke import smoke_fixture
        config, feeds, clock = smoke_fixture()
        runner = BotRunner(config, output_directory=args.output, feeds=feeds, clock=clock)
        duration = None
    else:
        config = load_bot_config(args.config)
        if config.get('runtime', {}).get('source') != 'public':
            raise ValueError('run requires runtime.source=public; use smoke for synthetic execution')
        if args.observe_only:
            config['runtime']['observe_only'] = True
        # Match the established CLI credential flow only when starting public
        # transport. Imports, pure validation, smoke and replay stay inert.
        from dotenv import load_dotenv
        load_dotenv()
        runner = BotRunner(config, output_directory=args.output)
        duration = args.duration
    loop = asyncio.get_running_loop()
    interrupts = 0
    task = asyncio.current_task()
    def stop():
        nonlocal interrupts
        interrupts += 1
        if interrupts == 1:
            runner.request_stop()
        else:
            task.cancel()
    installed = False
    try:
        loop.add_signal_handler(signal.SIGINT, stop)
        installed = True
    except (NotImplementedError, RuntimeError, ValueError):
        pass
    try:
        try:
            return await runner.run(duration=duration)
        except ValueError as error:
            if runner.status['phase'] == 'failed':
                raise RuntimeError(str(error)) from error
            raise
    finally:
        if installed:
            loop.remove_signal_handler(signal.SIGINT)


def main(argv=None):
    parser = argparse.ArgumentParser(description='Supervised paper-only bot runtime')
    commands = parser.add_subparsers(dest='command', required=True)
    run = commands.add_parser('run')
    run.add_argument('--config', required=True)
    run.add_argument('--output', required=True)
    run.add_argument('--duration', type=float)
    run.add_argument('--observe-only', action='store_true')
    smoke = commands.add_parser('smoke')
    smoke.add_argument('--output', required=True)
    replay = commands.add_parser('replay')
    replay.add_argument('--capture', required=True)
    replay.add_argument('--output', required=True)
    serve=commands.add_parser('serve',help='Run an initially idle local paper-bot control service')
    mode=serve.add_mutually_exclusive_group(required=True)
    mode.add_argument('--smoke',action='store_true')
    mode.add_argument('--config')
    serve.add_argument('--socket',required=True)
    serve.add_argument('--output-root',required=True)
    try:
        args=parser.parse_args(argv)
        if args.command=='serve':
            asyncio.run(_serve(args,force_exit_on_unclean=True))
            return 0
        summary = asyncio.run(_execute(args))
        print(json.dumps({key: summary[key] for key in ('session_id', 'submitted', 'accounts', 'assumptions')}, indent=2))
        return 0
    except ValueError as error:
        print(redact(f'Invalid configuration: {error}'), file=sys.stderr)
        return 2
    except BaseException as error:
        if isinstance(error, SystemExit):
            raise
        print(redact(f'Runtime failed: {type(error).__name__}: {error}'), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
