from datetime import datetime
from typing import Optional, List, Dict


__all__ = ["Query", "IntervalQuery", "MySamplerQuery"]


class Query:
    """An abstract base class to be used by various mya utility wrappers."""

    def __init__(self):
        raise NotImplementedError("Query class is abstract")


class IntervalQuery(Query):
    # noinspection PyMissingConstructor
    def __init__(self, channel: str, begin: datetime, end: datetime,
                 bin_limit: Optional[int] = None,
                 sample_type: Optional[str] = None,
                 deployment: str = "ops",
                 frac_time_digits: int = 0,
                 sig_figs: int = 6,
                 data_updates_only: bool = False,
                 prior_point: bool = False,
                 enums_as_str: bool = False,
                 unix_timestamps_ms: bool = False,
                 adjust_time_to_server_offset: bool = False,
                 integrate: bool = False,
                 **kwargs
                 ):
        """Construct a query to the myquery interval service.

        Args:
            channel: The name of the PV to query
            begin: The start time of the query
            end: The end time of the query
            bin_limit: How many points returned from MYA before sampling kicks in
            sample_type: What sampling algorithm should be used.  [graphical, eventsimple, myget, mysampler]
            frac_time_digits: How many digits should be displayed for fractional seconds
            sig_figs: How many significant figures should be reported in the PV values
            data_updates_only: Should the response include updates that only include value changes (not disconnects?)
            prior_point: Should the query use the most recent update prior to the start to give a value at the start of
                         the query.
            enums_as_str:  Should enum PV values be returned as their named strings instead of ints
            unix_timestamps_ms:  Should timestamps be returned as millis since unix epoch
            adjust_time_to_server_offset: Should the timestamp be localized to the myquery server
            integrate: Should the values be integrated (ony supported for float PVs)
            kwargs: Any extra parameters to be supplied to the interval web end point.
        """
        self.channel = channel
        self.begin = begin
        self.end = end
        self.bin_limit = bin_limit
        self.sample_type = sample_type
        self.deployment = deployment
        self.frac_time_digits = frac_time_digits
        self.sig_figs = sig_figs
        self.data_updates_only = data_updates_only
        self.prior_point = prior_point
        self.enums_as_str = enums_as_str
        self.unix_timestamps_ms = unix_timestamps_ms
        self.adjust_time_to_server_offset = adjust_time_to_server_offset
        self.integrate = integrate
        self.extra_opts = kwargs

    def to_web_params(self):
        """
        Convert the query to web parameters.

        Based on myquery v6.2, but use of kwargs makes this more flexible.

        Example URL that we're targeting
        https://epicsweb.jlab.org/myquery/interval?c=R1M1GMES&b=2023-05-09&e=2023-05-09+15%3A59%3A00&l=&t=graphical&m=history&f=0&v=6&d=on&p=on&s=on&u=on&a=on&i=on
        """
        ts_fmt = "%Y-%m-%dT%H:%M:%S"
        out = {'c': self.channel,
               'b': self.begin.strftime(ts_fmt),
               'e': self.end.strftime(ts_fmt),
               'm': self.deployment,
               'f': self.frac_time_digits,
               'v': self.sig_figs,
               }

        # It looks like the form keeps the 'l' param with "" passed if not specified
        if self.bin_limit is None:
            out['l'] = ""
        else:
            out['l'] = self.bin_limit

        # myquery app assumes its own default if 't' is missing.  Don't need to send anything.
        if self.sample_type is not None:
            out['t'] = self.sample_type

        # API takes presence of some params to mean == true, and the web form uses 'on' instead of a boolean.
        if self.data_updates_only:
            out['d'] = 'on'
        if self.prior_point:
            out['p'] = 'on'
        if self.enums_as_str:
            out['s'] = 'on'
        if self.unix_timestamps_ms:
            out['u'] = 'on'
        if self.adjust_time_to_server_offset:
            out['a'] = 'on'
        # only valid for float events, but that's left to the user
        if self.integrate:
            out['i'] = 'on'

        # Allow the user to add extra options if they so choose.
        if self.extra_opts is not None:
            out.update(self.extra_opts)

        return out


class MySamplerQuery(Query):
    """A class for containing the arguments needed by mySampler."""

    # noinspection PyMissingConstructor
    def __init__(self, start: datetime, interval: int, num_samples: int, pvlist: List[str],
                 deployment: Optional[str] = None, **kwargs):
        self.start = start.replace(microsecond=0).isoformat().replace("T", " ")
        self.interval = interval
        self.num_samples = num_samples
        self.pvlist = pvlist
        self.deployment = deployment
        self.extra_opts = kwargs

    @staticmethod
    def from_config(start: str, interval: str, num_samples: str, pvlist: List[str], **kwargs):
        return MySamplerQuery(start=datetime.strptime(start, "%Y-%m-%d %H:%M:%S"),
                              interval=int(interval),
                              num_samples=int(num_samples),
                              pvlist=pvlist, **kwargs)

    def to_web_params(self) -> Dict[str, str]:
        """Convert the objects command line parameters to their web counterparts"""
        out = {'c': ",".join(self.pvlist),
               'b': self.start.replace(" ", "T"),
               'n': self.num_samples,
               'm': self.deployment, 's': self.interval
               }

        if self.extra_opts is not None:
            out.update(self.extra_opts)

        return out
