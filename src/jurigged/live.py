import argparse
import ast
import code
import functools
import importlib
import logging
import os
import sys
import threading
import traceback
from dataclasses import dataclass
from types import ModuleType
import types
import inspect
from typing import Any, Optional

import blessed
from ovld import ovld
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserverVFS

from . import codetools, runpy
from .register import registry
from .utils import EventSource, glob_filter, or_filter
from .version import version

log = logging.getLogger(__name__)
T = blessed.Terminal()
DEFAULT_DEBOUNCE = 0.05
RELOAD_ON_CONTINUE = True

class ReloadException(ValueError):
    """Exception when hot-restart fails to reload a function."""

    pass

@dataclass
class WatchOperation:
    filename: str

    def __str__(self):
        return f"Watch {self.filename}"


@ovld
def default_logger(event: codetools.UpdateOperation):
    if isinstance(event.defn, codetools.FunctionDefinition):
        print(T.bold_yellow(str(event)))


@ovld
def default_logger(event: codetools.AddOperation):
    print(T.bold_green(str(event)))


@ovld
def default_logger(event: codetools.DeleteOperation):
    print(T.bold_red(str(event)))


@ovld
def default_logger(event: WatchOperation):
    print(T.bold(str(event)))


@ovld
def default_logger(exc: Exception):
    lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
    print(T.bold_red("".join(lines)))


@ovld
def default_logger(exc: SyntaxError):
    lines = traceback.format_exception(
        type(exc), exc, exc.__traceback__, limit=0
    )
    print(T.bold_red("".join(lines)))


@ovld
def default_logger(event: object):
    print(event)


def conservative_logger(event):
    if isinstance(event, Exception):
        default_logger(event)


class Watcher:
    def __init__(self, registry, debounce=DEFAULT_DEBOUNCE, poll=False):
        
        if poll:
            self.observer = PollingObserverVFS(
                stat=os.stat, listdir=os.scandir, polling_interval=poll
            )
        else:
            self.observer = Observer()
            
        self.registry = registry
        self.registry.precache_activity.register(self.on_prepare)
        self.debounce = debounce
        self.poll = poll
        self.prerun = EventSource()
        self.postrun = EventSource()
    

    def on_prepare(self, module_name, filename):
        """Register a file to be watched."""
        JuriggedHandler(self, filename).schedule(self.observer)
        self.registry.log(WatchOperation(filename))
        
    def refresh(self, path):
        """Refresh a file or module."""
        cf = self.registry.get(path)
        if cf:
            try:
                self.prerun.emit(path, cf)
                cf.refresh()
                self.postrun.emit(path, cf)
            except Exception as exc:
                self.registry.log(exc)
        else:
            self.registry.log(f"Could not find {path} in registry")

    def start(self):
        """Start the file watcher."""
        self.observer.start()

    def stop(self):
        """Stop the file watcher."""
        self.observer.stop()

    def join(self):
        """Wait for the file watcher to finish."""
        self.observer.join()



class JuriggedHandler(FileSystemEventHandler):
    def __init__(self, watcher, filename):
        self.watcher = watcher
        self.filename = filename
        self.normalized_filename = os.path.normpath(filename)
        self.mtime = 0
        self.timer = None

    def _refresh(self):
        self.watcher.refresh(self.filename)
        self.timer = None

    def on_modified(self, event):
        if event.src_path == self.normalized_filename:
            mtime = os.path.getmtime(event.src_path)
            # The modified event sometimes fires twice for no reason
            # even though the mtime is the same
            if mtime != self.mtime:
                self.mtime = mtime
                if self.watcher.debounce:
                    if self.timer is not None:
                        self.timer.cancel()
                    self.timer = threading.Timer(
                        self.watcher.debounce, self._refresh
                    )
                    self.timer.start()
                else:
                    self._refresh()

    on_created = on_modified

    def schedule(self, observer):
        # Watch the directory, because when watching a file, the watcher stops when
        # it is deleted and will not pick back up if the file is recreated. This happens
        # when some editors save.
        observer.schedule(self, os.path.dirname(self.filename))


@ovld
def to_filter(pattern: str):
    return glob_filter(pattern)


@ovld
def to_filter(patterns: list):
    return or_filter([to_filter(p) for p in patterns])


@ovld
def to_filter(obj: object):
    return obj


def watch(
    pattern="./*.py",
    logger=default_logger,
    registry=registry,
    autostart=True,
    debounce=DEFAULT_DEBOUNCE,
    poll=False,
):
    registry.auto_register(filter=to_filter(pattern))
    registry.set_logger(logger)
    watcher = Watcher(
        registry,
        debounce=debounce,
        poll=poll,
    )
    if autostart:
        watcher.start()
    return watcher


def _loop_module():  # pragma: no cover
    try:
        from . import loop

        return loop

    except ModuleNotFoundError as exc:
        print("ModuleNotFoundError:", exc, file=sys.stderr)
        sys.exit("To use --loop or --xloop, install jurigged[develoop]")


def find_runner(opts, pattern, prepare=None):  # pragma: no cover
    if opts.module:
        module_spec, *rest = opts.module
        assert opts.script is None

        sys.argv[1:] = rest

        if ":" in module_spec:
            module_name, func = module_spec.split(":", 1)
            mod = importlib.import_module(module_name)
            return mod, getattr(mod, func)

        else:
            _, spec, code = runpy._get_module_details(module_spec)
            if pattern(spec.origin):
                registry.prepare("__main__", spec.origin)
            mod = ModuleType("__main__")

            def run():
                runpy.run_module(
                    module_spec, module_object=mod, prepare=prepare
                )

            return mod, run

    elif opts.script:
        path = os.path.abspath(opts.script)
        if pattern(path):
            # It won't auto-trigger through runpy, probably some idiosyncracy of
            # module resolution
            registry.prepare("__main__", path)
        sys.argv[1:] = opts.rest
        mod = ModuleType("__main__")

        def run():
            runpy.run_path(path, module_object=mod, prepare=prepare)

        return mod, run

    else:
        mod = ModuleType("__main__")
        return mod, None

watch_instance = None
#Get/set the global watch instance
def get_watch_instance() -> Watcher:
    global watch_instance
    return watch_instance # type: ignore
def set_watch_instance(instance):
    global watch_instance
    watch_instance = instance
    
def cli():  # pragma: no cover
    sys.path.insert(0, os.path.abspath(os.curdir))

    parser = argparse.ArgumentParser(
        description="Run a Python script so that it is live-editable."
    )
    parser.add_argument(
        "script", metavar="SCRIPT", help="Path to the script to run", nargs="?"
    )
    parser.add_argument(
        "--interactive",
        "-i",
        action="store_true",
        help="Run an interactive session after the program ends",
    )
    parser.add_argument(
        "--watch",
        "-w",
        metavar="PATH",
        action="append",
        help="Wildcard path/directory for which files to watch",
    )

    parser.add_argument(
        "--debounce",
        "-d",
        type=float,
        help="Interval to wait for to refresh a modified file, in seconds",
    )
    parser.add_argument(
        "--poll",
        type=float,
        help="Poll for changes using the given interval",
    )

    parser.add_argument(
        "-m",
        dest="module",
        metavar="MODULE",
        nargs=argparse.REMAINDER,
        help="Module or module:function to run",
    )
    parser.add_argument(
        "--loop",
        "-l",
        action="append",
        type=str,
        help="Name of the function(s) to loop on",
    )
    parser.add_argument(
        "--loop-interface",
        type=str,
        choices=("rich", "basic"),
        default="rich",
        help="Interface to use for --loop",
    )
    parser.add_argument(
        "--xloop",
        "-x",
        action="append",
        type=str,
        help="Name of the function(s) to loop on if they raise an error",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show watched files and changes as they happen",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="Print version",
    )
    parser.add_argument(
        "rest", metavar="...", nargs=argparse.REMAINDER, help="Script arguments"
    )
    opts = parser.parse_args()

    pattern = to_filter(opts.watch or ".")
    watch_args = {
        "pattern": pattern,
        "logger": default_logger if opts.verbose else conservative_logger,
        "debounce": opts.debounce or DEFAULT_DEBOUNCE,
        "poll": opts.poll,
    }

    banner = ""

    if opts.version:
        print(version)
        sys.exit()

    prepare = None

    if opts.loop or opts.xloop:
        import codefind

        loopmod = _loop_module()

        def prepare(glb):
            from .rescript import redirect_code

            filename = glb["__file__"]

            def _getcode(ref):
                if ref.startswith("/"):
                    _, module, *hierarchy = ref.split("/")
                    return codefind.find_code(*hierarchy, module=module)
                elif ":" in ref:
                    module, hierarchy_s = ref.split(":")
                    hierarchy = hierarchy_s.split(".")
                    return codefind.find_code(*hierarchy, module=module)
                else:
                    hierarchy = ref.split(".")
                    return codefind.find_code(*hierarchy, filename=filename)

            for ref in opts.loop or []:
                redirect_code(
                    _getcode(ref), loopmod.loop(interface=opts.loop_interface)
                )

            for ref in opts.xloop or []:
                redirect_code(
                    _getcode(ref), loopmod.xloop(interface=opts.loop_interface)
                )

    mod, run = find_runner(opts, pattern, prepare=prepare)
    watch_instance = watch(**watch_args)
    
    set_watch_instance(watch_instance)
    
    if run is None:
        banner = None
        opts.interactive = True
    else:
        banner = ""
        run()

    if opts.interactive:
        code.interact(banner=banner, local=vars(mod), exitmsg="")
