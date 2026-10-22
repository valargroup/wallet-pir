"""Use the publisher's checksum implementation from Python tooling.

Set PIR_ARTIFACT_HASH_COMMAND to the pinned pir-publisher executable when it is
not installed on PATH. Algorithm selection and digest encoding stay in Rust.
"""
import json
from contextlib import suppress
import os
import select
import subprocess
import time


class StreamHasher:
    def __init__(self):
        executable = os.environ.get('PIR_ARTIFACT_HASH_COMMAND', 'pir-publisher')
        self.process = subprocess.Popen([executable, 'artifact-checksum', 'hash'],
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, bufsize=0)
        os.set_blocking(self.process.stdin.fileno(), False)
        self.result = None
        self.failed = False

    def update(self, data, deadline=None):
        deadline = deadline if deadline is not None else time.monotonic() + 30
        view = memoryview(data)
        while view:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('artifact checksum input deadline')
            if not select.select([], [self.process.stdin], [], remaining)[1]:
                raise TimeoutError('artifact checksum input deadline')
            try:
                written = os.write(self.process.stdin.fileno(), view)
            except BlockingIOError:
                continue
            if not written:
                raise ValueError('artifact checksum helper closed its input')
            view = view[written:]

    def finish(self, timeout=30):
        if self.result is not None:
            return self.result
        if self.failed:
            raise ValueError('artifact checksum helper failed')
        try:
            self.process.stdin.close()
            self.process.stdin = None
            output, _errors = self.process.communicate(timeout=timeout)
            if self.process.returncode:
                raise ValueError('artifact checksum helper failed')
            result = json.loads(output)
            if not valid(result):
                raise ValueError('artifact checksum helper returned an invalid checksum')
            self.result = result
            return result
        except BaseException:
            self.failed = True
            raise
        finally:
            if self.process.poll() is None:
                self.process.kill()
                self.process.communicate()

    def hexdigest(self):
        return self.finish()['digest']

    def cancel(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait()
        for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
            if stream is not None:
                with suppress(OSError, ValueError):
                    stream.close()


def valid(value):
    return (isinstance(value, dict) and set(value) == {'algorithm', 'digest'} and
            isinstance(value['algorithm'], str) and value['algorithm'].isascii() and
            value['algorithm'].isalnum() and isinstance(value['digest'], str) and
            len(value['digest']) == 64 and all(c in '0123456789abcdef' for c in value['digest']))


def hash_bytes(data):
    hasher = StreamHasher()
    try:
        hasher.update(data)
        return hasher.finish()
    finally:
        hasher.cancel()


def hash_file(path):
    hasher = StreamHasher()
    try:
        with path.open('rb') as stream:
            while chunk := stream.read(1 << 20):
                hasher.update(chunk)
        return hasher.finish()
    finally:
        hasher.cancel()
