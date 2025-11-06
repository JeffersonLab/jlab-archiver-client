# mypkg/config.py
from __future__ import annotations
from dataclasses import dataclass, field
from threading import RLock

_lock = RLock()

@dataclass
class _Config:
    """A basic config file class that handles thread-safety and issues with name binding ('from' imports)"""
    protocol: str = "http"
    """The protocol used by the myquery server"""

    myquery_server: str = "epicsweb.jlab.org"
    """The fully qualified domain name of the myquery server.  Can include port number."""

    mysampler_path: str = "/myquery/mysampler"
    """The path to the mysampler endpoint"""

    interval_path: str = "/myquery/interval"
    """The path to the interval endpoint"""

    channel_path: str = "/myquery/channel"
    """The path to the channel endpoint"""

    def set(self, **kwargs) -> None:
        """mutate-in-place API so imports never go stale"""
        with _lock:
            for k, v in kwargs.items():
                setattr(self, k, v)

    def snapshot(self) -> dict:
        """Get a consistent (thread-safe) snapshot of the config."""
        with _lock:
            return {
                "protocol": self.protocol,
                "myquery_server": self.myquery_server,
                "mysampler_path": self.mysampler_path,
                "interval_path": self.interval_path,
                "channel_path": self.channel_path,
            }

config = _Config()  # singleton
