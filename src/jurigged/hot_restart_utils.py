import functools
import inspect
import logging
import os
import sys
import threading
import types

from .live import get_watch_instance

## Debugger to use
if "pydevd" in sys.modules:
    # Handles VSCode, probably others
    DEBUGGER = "pydevd"
    # Fake the path of generated sources to match the original source
    DEBUG_ORIGINAL_PATH_FOR_RELOADED_CODE = True
elif "pudb" in sys.modules:
    DEBUGGER = "pudb"
    DEBUG_ORIGINAL_PATH_FOR_RELOADED_CODE = True
else:
    # Default stdlib wrapper
    DEBUGGER = "pdb"
    # Show the generated "surrogate" source in the debugger
    DEBUG_ORIGINAL_PATH_FOR_RELOADED_CODE = False

HOT_RESTART_ALREADY_WRAPPED = "_hot_restart_already_wrapped"
HOT_RESTART_NO_WRAP = "_hot_restart_no_wrap"

log = logging.getLogger(__name__)

def get_module_file_path(func):
    # Dynamically get the file path of the module
    module_file_path = None
    try:
        module = inspect.getmodule(func)
        module_file_path = inspect.getfile(module)
        # Convert to absolute path if needed
        if not os.path.isabs(module_file_path):
            module_file_path = os.path.abspath(module_file_path)
        log.debug(f"Module {module.__name__!r} file path: {module_file_path}")
    except (TypeError, ValueError) as e:
        # This can happen for built-in modules
        log.debug(f"Could not determine file path for module {module.__name__!r}: {e}")
    return module_file_path
    
def wrap_module(module_or_name=None):
    """Wrap all eligible functions in a module to enable hot reloading."""
    if module_or_name is None:
        # Need to go get module of calling frame
        module_or_name = sys._getframe(1).f_globals["__name__"]
        module_name = module_or_name
    if isinstance(module_or_name, str):
        module_name = module_or_name
        module = sys.modules[module_or_name]
        module_d = module.__dict__
    else:
        module = module_or_name
        module_name = module.__name__
        module_d = module.__dict__
        
    log.debug(f"Wrapping module {module_name!r}")
    
    out_d = {}
    for k, v in list(module_d.items()):
        if getattr(v, HOT_RESTART_NO_WRAP, False):
            log.warn(f"Skipping wrapping of no_wrap {v!r}")
        elif getattr(v, HOT_RESTART_ALREADY_WRAPPED, False):
            log.warn(f"Skipping already wrapped {v!r}")
        elif inspect.isclass(v):
            v_module = inspect.getmodule(v)
            
            if v_module and v_module.__name__ == module_name:
                
                log.debug(f"Wrapping class {v!r}")
                hot_restart_wrap_class(v)
            else:
                log.debug(
                    f"Not wrapping in-scope class {v!r} since it originates from {v_module} != {module_name}"
                )
        elif callable(v):
            v_module = inspect.getmodule(v)
            if v_module and v_module.__name__ == module_name:
                
                log.debug(f"Wrapping callable {v!r}")
                out_d[k] = hot_restart_wrap(v)
            else:
                log.debug(
                    f"Not wrapping in-scope callable {v!r} since it originates from {v_module} != {module_name}"
                )
        else:
            log.debug(f"Not wrapping {v!r}")
    for k, v in out_d.items():
        module_d[k] = v
        
def hot_restart_wrap_class(cls):
    """Wrap all methods in a class to enable hot reloading."""
    log.debug(f"Wrapping class: {cls!r}")
    for k, v in list(vars(cls).items()):
        if callable(v):
            log.debug(f"Wrapping {cls!r}.{k}")
            setattr(cls, k, hot_restart_wrap(v))
            
def hot_restart_wrap(
    func=None,
    *,
    propagated_exceptions: tuple[type[Exception], ...] = (StopIteration,),
    propagate_keyboard_interrupt: bool = True,
):
    """Wrap a function to enable hot reloading on exception."""
    if inspect.isclass(func):
        raise ValueError("Use wrap_class to wrap a class")
    assert isinstance(
        propagated_exceptions, tuple
    ), "propagated_exceptions should be a tuple of exception types"
    if func is None:
        return functools.partial(
            hot_restart_wrap,
            propagated_exceptions=propagated_exceptions,
            propagate_keyboard_interrupt=propagate_keyboard_interrupt,
        )
    if getattr(func, HOT_RESTART_ALREADY_WRAPPED, False):
        log.debug(f"Already wrapped {func!r}, not wrapping again")
        return func
    log.debug(f"Wrapping {func!r}")
    
    if inspect.iscoroutinefunction(func):
        module_file_path = get_module_file_path(func)
        
        @functools.wraps(func)
        async def wrapped_async(*args, **kwargs):
            watch_instance = get_watch_instance()
            global EXIT_THIS_FRAME
            EXIT_THIS_FRAME = False
            restart_count = 0
            while not EXIT_THIS_FRAME:
                if restart_count > 0:
                    log.debug("Restarting wraped async module")
                try:
                    result = await func(*args, **kwargs)
                    return result
                except Exception as e:
                    if isinstance(e, propagated_exceptions):
                        raise e
                    if propagate_keyboard_interrupt and isinstance(e, KeyboardInterrupt):
                        raise e
                    
                    # Handle exception with debugger
                    excinfo = sys.exc_info()
                    new_tb, num_dead_frames = _create_undead_traceback(
                        excinfo[2], sys._getframe(1), wrapped_async
                    )
                    excinfo = (excinfo[0], excinfo[1], new_tb)
                
                    # Start post-mortem debugging
                    _start_post_mortem(module_file_path, excinfo, num_dead_frames)
                
                    # After debugging, refresh the code
                    watch_instance.refresh(module_file_path)
                    
                restart_count += 1
                
        setattr(wrapped_async, HOT_RESTART_ALREADY_WRAPPED, True)
        return wrapped_async
                
    else:
        module_file_path = get_module_file_path(func)
        
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            watch_instance = get_watch_instance()
            global EXIT_THIS_FRAME
            EXIT_THIS_FRAME = False
            restart_count = 0
            while not EXIT_THIS_FRAME:
                if restart_count > 0:
                    log.debug("Restarting wraped module")
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    if isinstance(e, propagated_exceptions):
                        raise e
                    if propagate_keyboard_interrupt and isinstance(e, KeyboardInterrupt):
                        raise e
                    
                    # Handle exception with debugger
                    excinfo = sys.exc_info()
                    new_tb, num_dead_frames = _create_undead_traceback(
                        excinfo[2], sys._getframe(1), wrapped_async
                    )
                    excinfo = (excinfo[0], excinfo[1], new_tb)
                
                    # Start post-mortem debugging
                    _start_post_mortem(module_file_path, excinfo, num_dead_frames)
                
                    # After debugging, refresh the code
                    watch_instance.refresh(module_file_path)
                    
                restart_count += 1
        setattr(wrapped, HOT_RESTART_ALREADY_WRAPPED, True)
        return wrapped

def _create_undead_traceback(exc_tb, current_frame, wrapper_function):
    """Create a new traceback object that includes the current frame's parents."""
    # We want to default to one frame below the last one (the frame of the wrapper)
    num_dead_frames = -1
    dead_tb = exc_tb
    while dead_tb is not None and dead_tb.tb_next is not None:
        num_dead_frames += 1
        dead_tb = dead_tb.tb_next
    num_dead_frames = max(0, num_dead_frames)
    # If we would end up in the frame of the wrapper, jump up one more frame to
    # provide a more useful context
    if dead_tb is not None and dead_tb.tb_frame.f_code == wrapper_function.__code__:
        num_dead_frames += 1
        log.debug("Debug frame is offset from restart frame")
    frame = current_frame
    # Create new traceback objects
    prev_tb = exc_tb
    while frame:
        if frame.f_code != wrapper_function.__code__:
            # Skip live wrapper frames to make the backtrace cleaner
            # Those calls are presumably not responsible for the crash, so
            # hiding them is fine.
            prev_tb = types.TracebackType(
                tb_next=prev_tb,
                tb_frame=frame,
                tb_lasti=frame.f_lasti,
                tb_lineno=frame.f_lineno,
            )
        frame = frame.f_back
    return prev_tb, num_dead_frames
def _start_post_mortem(func_path, excinfo, num_dead_frames):
    """Start post-mortem debugging based on the configured debugger."""
    if DEBUGGER == "pdb":
        _start_pdb_post_mortem(func_path, excinfo, num_dead_frames)
    elif DEBUGGER == "pydevd":
        _start_pydevd_post_mortem(func_path, excinfo)
    elif DEBUGGER == "pudb":
        _start_pudb_post_mortem(func_path, excinfo)
    else:
        log.debug(f"Unknown debugger {DEBUGGER}, falling back to breakpoint()")
        breakpoint()
        
def _start_pdb_post_mortem(func_path, excinfo, num_dead_frames):
    """Start post-mortem debugging with pdb."""
    import pdb
    
    e_type, e, tb = excinfo
    log.debug(f"Entering pdb debugging of {func_path}")
    
    # Create a custom pdb instance
    p = pdb.Pdb()
    p.reset()
    p.interaction(None, tb)
        
def _start_pudb_post_mortem(func_path, excinfo):
    """Start post-mortem debugging with pudb."""
    e_type, e, tb = excinfo
    log.debug(f"Entering pudb debugging of {func_path}")
    import pudb # type: ignore
    pudb.post_mortem(tb=tb, e_type=e_type, e_value=e)
def _start_pydevd_post_mortem(func_path, excinfo):
    """Start post-mortem debugging with pydevd (VSCode)."""
    print(f"jurigged: Continue to revive {func_path}", file=sys.stderr)
    
    try:
        import pydevd # type: ignore
    except ImportError:
        breakpoint()
        return
    py_db = pydevd.get_global_debugger()
    if py_db is None:
        breakpoint()
    else:
        thread = threading.current_thread()
        additional_info = py_db.set_additional_thread_info(thread)
        additional_info.is_tracing += 1
        try:
            py_db.stop_on_unhandled_exception(py_db, thread, additional_info, excinfo)
        finally:
            additional_info.is_tracing -= 1