import ctypes
import io
import os
import sys
import tempfile
from contextlib import contextmanager
from sys import platform

libc = ctypes.CDLL(None)
if platform == "linux" or platform == "linux2":
    c_stderr = ctypes.c_void_p.in_dll(libc, 'stderr')  # for linuxos
elif platform == "darwin":
    c_stderr = ctypes.c_void_p.in_dll(libc, '__stderrp')  # for macos


@contextmanager
def stderr_redirector(stream):
    original_stderr_fd = sys.stderr.fileno()

    def _redirect_stderr(to_fd):
        libc.fflush(c_stderr)
        sys.stderr.close()
        os.dup2(to_fd, original_stderr_fd)
        sys.stderr = io.TextIOWrapper(os.fdopen(original_stderr_fd, 'wb'))

    saved_stderr_fd = os.dup(original_stderr_fd)
    try:
        tfile = tempfile.TemporaryFile(mode='w+b')
        _redirect_stderr(tfile.fileno())
        yield
        _redirect_stderr(saved_stderr_fd)
        tfile.flush()
        tfile.seek(0, io.SEEK_SET)
        stream.write(tfile.read().decode())
    finally:
        tfile.close()
        os.close(saved_stderr_fd)