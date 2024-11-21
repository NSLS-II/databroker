from __future__ import print_function
from functools import partial
import six  # noqa
from collections import defaultdict, deque
from datetime import datetime
from pims import FramesSequence, Frame
import logging
from warnings import warn
from importlib import import_module
import itertools
import warnings
import numbers
import doct
import pandas as pd
import sys
import os
import glob
import tempfile
import copy
from .eventsource import EventSourceShim
from .headersource import HeaderSourceShim, safe_get_stop
import humanize
import jinja2
import time
from .utils import (ALL, get_fields, wrap_in_deprecated_doct, wrap_in_doct,
                    DeprecatedDoct, DOCT_NAMES, lookup_config, list_configs,
                    describe_configs, SPECIAL_NAME)

from databroker.assets.core import DatumNotFound, EventDatumNotFound


try:
    from types import SimpleNamespace
except ImportError:
    class SimpleNamespace:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def __repr__(self):
            keys = sorted(self.__dict__)
            items = ("{}={!r}".format(k, self.__dict__[k]) for k in keys)
            return "{}({})".format(type(self).__name__, ", ".join(items))

        def __eq__(self, other):
            return self.__dict__ == other.__dict__


try:
    from functools import singledispatch
except ImportError:
    try:
        # We are running on Python 2.6, 2.7, or 3.3
        from singledispatch import singledispatch
    except ImportError:
        raise ImportError(
            "Please install singledispatch from PyPI"
            "\n\n   pip install singledispatch"
            "\n\nThen run your program again."
        )
try:
    from collections.abc import MutableSequence
except ImportError:
    # This will error on python < 3.3
    from collections import MutableSequence


logger = logging.getLogger(__name__)


class InvalidDocumentSequence(Exception):
    pass


class Header(object):
    """
    A dictionary-like object summarizing metadata for a run.
    """

    _name = 'header'
    def __init__(self, db, start, stop, ext=None, _cache=None):
        self.db = db
        self.start = start
        self.stop = stop
        if ext is None:
            ext = {}
        self.ext = ext
        if _cache is None:
            _cache = {}
        self._cache = _cache

    def __eq__(self, other):
        if not isinstance(other, Header):
            return False
        return self.start == other.start

    @classmethod
    def from_run_start(cls, db, run_start, run_stop=None):
        """
        Build a Header from a RunStart Document.

        Parameters
        ----------
        db : Broker

        run_start : dict or string
            RunStart document or uid of one

        Returns
        -------
        header : databroker.core.Header
        """
        hs = db.hs
        if isinstance(run_start, six.string_types):
            run_start = hs.run_start_given_uid(run_start)

        if run_stop is None:
            run_stop = safe_get_stop(hs, run_start['uid'])

        d = {'start': db.prepare_hook('start', run_start)}
        if run_stop is not None:
            d['stop'] = db.prepare_hook('stop', run_stop or {})

        d['ext'] = SimpleNamespace(**db.fetch_external(run_start, run_stop))
        h = cls(db, **d)
        return h

    def _repr_html_(self):
        env = jinja2.Environment()
        env.filters['human_time'] = _pretty_print_time
        template = env.from_string(_HTML_TEMPLATE)
        return template.render(document=self)

    # ## dict-like methods ###

    def __getitem__(self, k):
        if k in ('start', 'descriptors', 'stop', 'ext'):
            return getattr(self, k)
        else:
            raise KeyError(k)

    def get(self, *args, **kwargs):
        return getattr(self, *args, **kwargs)

    def items(self):
        for k in self.keys():
            yield k, getattr(self, k)

    def values(self):
        for k in self.keys():
            yield getattr(self, k)

    def keys(self):
        for k in ('start', 'descriptors', 'stop', 'ext'):
            yield k

    def to_name_dict_pair(self):
        return self._name, {'start': self.start, 'stop': self.stop,
                            'descriptors': self.descriptors,
                            'ext': self.ext}

    def __len__(self):
        return 4

    def __iter__(self):
        return self.keys()

    # ## convenience methods and properties, encapsulating one-liners ## #

    @property
    def descriptors(self):
        if 'desc' not in self._cache:
            self._cache['desc'] = sum((es.descriptors_given_header(self)
                                       for es in self.db.event_sources),
                                      [])
        prepare = partial(self.db.prepare_hook, 'descriptor')
        return list(map(prepare, self._cache['desc']))

    @property
    def stream_names(self):
        return self.db.stream_names_given_header(self)

    def fields(self, stream_name=ALL):
        """
        Return the names of the fields ('data keys') in this run.

        Parameters
        ----------
        stream_name : string or ``ALL``, optional
            Filter results by stream name (e.g., 'primary', 'baseline'). The
            default, ``ALL``, combines results from all streams.

        Returns
        -------
        fields : set

        Examples
        --------
        Load the most recent run and list its fields.

        >>> h = db[-1]
        >>> h.fields()
        {'eiger_stats1_total', 'eiger_image'}

        See Also
        --------
        :meth:`Header.devices`
        """
        fields = set()
        for es in self.db.event_sources:
            fields.update(es.fields_given_header(header=self,
                                                 stream_name=stream_name))
        return fields

    def devices(self, stream_name=ALL):
        """
        Return the names of the devices in this run.

        Parameters
        ----------
        stream_name : string or ``ALL``, optional
            Filter results by stream name (e.g., 'primary', 'baseline'). The
            default, ``ALL``, combines results from all streams.

        Returns
        -------
        devices : set

        Examples
        --------
        Load the most recent run and list its devices.

        >>> h = db[-1]
        >>> h.devices()
        {'eiger'}

        See Also
        --------
        :meth:`Header.fields`
        """
        result = set()
        for d in self.descriptors:
            if stream_name is ALL or stream_name == d.get('name', 'primary'):
                result.update(d['object_keys'])
        return result

    def config_data(self, device_name):
        """
        Extract device configuration data from Event Descriptors.

        This refers to the data obtained from ``device.read_configuration()``.

        See example below. The result is structed as a [...deep breath...]
        dictionary of lists of dictionaries because:

        * The device might have been read in multiple event streams
          ('primary', 'baseline', etc.). Each stream name is a key in the
          outer dictionary.
        * The configuration is typically read once per event stream, but in
          general may be read multiple times if the configuration is changed
          mid-stream. Thus, a list is needed.
        * Each device typically produces multiple configuration fields
          ('exposure_time', 'period', etc.). These are the keys of the inner
          dictionary.

        Parameters
        ----------
        device_name : string
            device name (originally obtained from the ``name`` attribute of
            some readable Device)

        Returns
        -------
        result : dict
            mapping each stream name (such as 'primary' or 'baseline') to a
            list of data dictionaries

        Examples
        --------
        Get the device configuration recorded for the device named 'eiger'.

        >>> h.config_data('eiger')
        {'primary': [{'exposure_time': 1.0}]}

        Assign the exposure time to a variable.

        >>> exp_time = h.config_data('eiger')['primary'][0]['exposure_time']

        How did we know that ``'eiger'`` was a valid argument? We can query for
        the complete list of device names:

        >>> h.devices()
        {'eiger', 'cs700'}
        """
        result = defaultdict(list)
        for d in sorted(self.descriptors, key=lambda d: d['time']):
            config = d['configuration'].get(device_name)
            if config:
                result[d.get('name')].append(config['data'])
        return dict(result)  # strip off defaultdict behavior

    def documents(self, stream_name=ALL, fields=None, fill=False):
        """
        Load all documents from the run.

        This is a generator the yields ``(name, doc)``.

        Parameters
        ----------
        stream_name : string or ``ALL``, optional
            Filter results by stream name (e.g., 'primary', 'baseline'). The
            default, ``ALL``, combines results from all streams.
        fill : bool, optional
            Whether externally-stored data should be filled in. False by
            default.

        Yields
        ------
        name, doc : (string, dict)

        Examples
        --------
        Loop through the documents from a run.

        >>> h = db[-1]
        >>> for name, doc in h.documents():
        ...     # do something
        """
        gen = self.db.get_documents(self, fields=fields,
                                    stream_name=stream_name,
                                    fill=fill)
        for payload in gen:
            yield payload

    def stream(self, *args, **kwargs):
        warn("The 'stream' method been renamed to 'documents'. The old name "
             "will be removed in the future.")
        for payload in self.documents(*args, **kwargs):
            yield payload

    def events(self, stream_name='primary', fields=None, fill=False):
        """
        Load all Event documents from one event stream.

        This is a generator the yields Event documents.

        Parameters
        ----------
        stream_name : str, optional
            Get events from only "event stream" with this name.

            Default is 'primary'

        fields : List[str], optional
            whitelist of field names of interest; if None, all are returned

            Default is None

        fill : bool or Iterable[str], optional
            Which fields to fill.  If `True`, fill all
            possible fields.

            Each event will have the data filled for the intersection
            of it's external keys and the fields requested filled.

            Default is False

        Yields
        ------
        doc : dict

        Examples
        --------
        Loop through the Event documents from a run. This is 'lazy', meaning
        that only one Event at a time is loaded into memory.

        >>> h = db[-1]
        >>> for event in h.events():
        ...    # do something

        List the Events documents from a run, loading them all into memory at
        once.

        >>> events = list(h.events())
        """
        ev_gen = self.db.get_events([self], stream_name=stream_name,
                                    fields=fields, fill=fill)
        for ev in ev_gen:
            yield ev

    def table(self, stream_name='primary', fields=None, fill=False,
              timezone=None, convert_times=True, localize_times=True):
        '''
        Load the data from one event stream as a table (``pandas.DataFrame``).

        Parameters
        ----------
        stream_name : str, optional
            Get events from only "event stream" with this name.

            Default is 'primary'

        fields : List[str], optional
            whitelist of field names of interest; if None, all are returned

            Default is None

        fill : bool or Iterable[str], optional
            Which fields to fill.  If `True`, fill all
            possible fields.

            Each event will have the data filled for the intersection
            of it's external keys and the fields requested filled.

            Default is False

        handler_registry : dict, optional
            mapping filestore specs (strings) to handlers (callable classes)

        convert_times : bool, optional
            Whether to convert times from float (seconds since 1970) to
            numpy datetime64, using pandas. True by default.

        timezone : str, optional
            e.g., 'US/Eastern'; if None, use metadatastore configuration in
            `self.mds.config['timezone']`

        localize_times : bool, optional
            If the times should be localized to the 'local' time zone.  If
            True (the default) the time stamps are converted to the localtime
            zone (as configure in mds).

            This is problematic for several reasons:

              - apparent gaps or duplicate times around DST transitions
              - incompatibility with every other time stamp (which is in UTC)

            however, this makes the dataframe repr look nicer

            This implies convert_times.

            Defaults to True to preserve back-compatibility.

        Returns
        -------
        table : pandas.DataFrame

        Examples
        --------
        Load the 'primary' data stream from the most recent run into a table.

        >>> h = db[-1]
        >>> h.table()

        This is equivalent. (The default stream_name is 'primary'.)

        >>> h.table(stream_name='primary')
                                    time intensity
        0  2017-07-16 12:12:37.239582345       102
        1  2017-07-16 12:12:39.958385283       103

        Load the 'baseline' data stream.

        >>> h.table(stream_name='baseline')
                                    time temperature
        0  2017-07-16 12:12:35.128515999         273
        1  2017-07-16 12:12:40.128515999         274
        '''
        return self.db.get_table(self, fields=fields,
                                 stream_name=stream_name, fill=fill,
                                 timezone=timezone,
                                 convert_times=convert_times,
                                 localize_times=localize_times)

    def data(self, field, stream_name='primary', fill=True):
        """
        Extract data for one field. This is convenient for loading image data.

        Parameters
        ----------
        field : string
            such as 'image' or 'intensity'

        stream_name : string, optional
            Get data from a single "event stream." Default is 'primary'

        fill : bool, optional
             If the data should be filled.

        Yields
        ------
        data
        """
        if fill:
            fill = {field}
        for event in self.events(stream_name=stream_name,
                                 fields=[field],
                                 fill=fill):
            yield event['data'][field]


def register_builtin_handlers(reg):
    "Register all the handlers built in to databroker."
    from .assets import handlers
    # TODO This will blow up if any non-leaves in the class heirarchy
    # have non-empty specs. Make this smart later.
    for cls in vars(handlers).values():
        if isinstance(cls, type) and issubclass(cls, handlers.HandlerBase):
            logger.debug("Found Handler %r for specs %r", cls, cls.specs)
            for spec in cls.specs:
                logger.debug("Registering Handler %r for spec %r", cls, spec)
                reg.register_handler(spec, cls)


def get_images(db, headers, name, handler_registry=None,
               handler_override=None, stream_name='primary'):
    """
    This method is deprecated. Use Header.data instead.

    Load images from a detector for given Header(s).

    Parameters
    ----------
    headers : Header or list of Headers
    name : string
        field name (data key) of a detector
    handler_registry : dict, optional
        mapping spec names (strings) to handlers (callable classes)
    handler_override : callable class, optional
        overrides registered handlers


    Examples
    --------

    >>> header = db[-1]
    >>> images = Images(header, 'my_detector_lightfield')
    >>> for image in images:
            # do something
    """
    return Images(db.mds, db.es, db.reg, headers, name, handler_registry,
                  handler_override, stream_name='primary')


class Images(FramesSequence):
    def __init__(self, mds, reg, es, headers, name, handler_registry=None,
                 handler_override=None, stream_name='primary'):
        """
        This class is deprecated.

        Load images from a detector for given Header(s).

        Parameters
        ----------
        reg : RegistryRO
        headers : Header or list of Headers
        es : EventStoreRO
        name : str
            field name (data key) of a detector
        handler_registry : dict, optional
            mapping spec names (strings) to handlers (callable classes)
        handler_override : callable class, optional
            overrides registered handlers

        Example
        -------
        >>> header = db[-1]
        >>> images = Images(header, 'my_detector_lightfield')
        >>> for image in images:
                # do something
        """
        warn("Images and get_images are deprecated. Use Header.data('{}') "
             "instead.".format(name), stacklevel=3)
        self.reg = reg
        db = Broker(mds, reg)
        events = db.get_events(headers,
                               stream_name=stream_name,
                               fields=[name], fill=False)

        self._datum_ids = [event['data'][name] for event in events
                           if name in event['data']]
        self._len = len(self._datum_ids)
        first_uid = self._datum_ids[0]
        if handler_override is None:
            self.handler_registry = handler_registry or self.reg.handler_reg
        else:
            # mock a handler registry
            self.handler_registry = defaultdict(lambda: handler_override)
        with self.reg.handler_context(self.handler_registry) as reg:
            example_frame = reg.retrieve(first_uid)
        # Try to duck-type as a numpy array, but fall back as a general
        # Python object.
        try:
            self._dtype = example_frame.dtype
        except AttributeError:
            self._dtype = type(example_frame)
        try:
            self._shape = example_frame.shape
        except AttributeError:
            self._shape = None  # as in, unknown

    @property
    def fs(self):
        warnings.warn("fs is deprecated, use reg instead",
                      stacklevel=2)
        return self.reg

    @property
    def pixel_type(self):
        return self._dtype

    @property
    def frame_shape(self):
        return self._shape

    def __len__(self):
        return self._len

    def get_frame(self, i):
        with self.reg.handler_context(self.handler_registry) as reg:
            img = reg.retrieve(self._datum_ids[i])
        if hasattr(img, '__array__'):
            return Frame(img, frame_no=i)
        else:
            # some non-numpy-like type
            return img


class DocBuffer:
    '''Buffer a (name, document) sequence into parts

    '''
    def __init__(self, doc_gen, denormalize=False):

        class InnerDict(dict):
            def __getitem__(inner_self, key):
                while key not in inner_self:
                    try:
                        self._get_next()
                    except StopIteration:
                        raise Exception("this stream does not contain a "
                                        "descriptor with uid {}".format(key))
                return super(DocBuffer, self).__getitem__(key)

        self.denormalize = denormalize
        self.gen = doc_gen
        self._start = None
        self._stop = None
        self.descriptors = InnerDict()
        self._events = deque()

    @property
    def start(self):
        while self._start is None:
            try:
                self._get_next()
            except StopIteration:
                raise InvalidDocumentSequence(
                    "stream does not contain a start?!")

        return self._start

    @property
    def stop(self):
        while self._stop is None:
            try:
                self._get_next()
            except StopIteration:
                raise InvalidDocumentSequence(
                    "stream does not contain a start")

        return self._stop

    def _get_next(self):
        self.__stash_values(*next(self.gen))

    def __stash_values(self, name, doc):
        if name == 'start':
            if self._start is not None:
                raise Exception("only one start allowed")
            self._start = doc
        elif name == 'stop':
            if self._stop is not None:
                raise Exception("only one stop allowed")
            self._stop = doc
        elif name == 'descriptor':
            self.descriptors[doc['uid']] = doc
        elif name == 'event':
            self._events.append(doc)
        else:
            raise ValueError("{} is unknown document type".format(name))

    def __denormalize(self, ev):
        ev = dict(ev)
        desc = ev['descriptor']
        try:
            ev['descriptor'] = self.descriptors[desc]
        except StopIteration:
            raise InvalidDocumentSequence(
                "{} is on an event, but not in event stream".format(desc))
        return ev

    def __iter__(self):
        gen = self.gen
        while True:
            while len(self._events):
                ev = self._events.popleft()
                if self.denormalize:
                    ev = self.__denormalize(ev)
                yield ev

            try:
                name, doc = next(gen)
            except StopIteration:
                break

            if name == 'event':
                if self.denormalize:
                    doc = self.__denormalize(doc)
                yield doc
            else:
                self.__stash_values(name, doc)


@singledispatch
def search(key, db):
    logger.info('Using default search for key = %s' % key)
    raise ValueError("Must give an integer scan ID like [6], a slice "
                     "into past scans like [-5], [-5:], or [-5:-9:2], "
                     "a list like [1, 7, 13], a (partial) uid "
                     "like ['a23jslk'] or a full uid like "
                     "['f26efc1d-8263-46c8-a560-7bf73d2786e1'].")


@search.register(slice)
def _(key, db):
    # Interpret key as a slice into previous scans.
    logger.info('Interpreting key = %s as a slice' % key)
    if key.start is not None and key.start > -1:
        raise ValueError("slice.start must be negative. You gave me "
                         "key=%s The offending part is key.start=%s"
                         % (key, key.start))
    if key.stop is not None and key.stop > 0:
        raise ValueError("slice.stop must be <= 0. You gave me key=%s. "
                         "The offending part is key.stop = %s"
                         % (key, key.stop))
    if key.stop is not None:
        stop = -key.stop
    else:
        stop = None
    if key.start is None:
        raise ValueError("slice.start cannot be None because we do not "
                         "support slicing infinitely into the past; "
                         "the size of the result is non-deterministic "
                         "and could become too large.")
    start = -key.start
    result = list(db.hs.find_last(start))[stop::key.step]
    stop = list(safe_get_stop(db.hs, s) for s in result)
    return list(zip(result, stop))


@search.register(numbers.Integral)
def _(key, db):
    logger.info('Interpreting key = %s as an integer' % key)
    key = int(key)
    if key > -1:
        # Interpret key as a scan_id.
        gen = db.hs.find_run_starts(scan_id=key)
        try:
            result = next(gen)  # most recent match
        except StopIteration:
            raise KeyError("No such run found for key=%s which is "
                           "being interpreted as a scan id." % key)
    else:
        # Interpret key as the Nth last scan.
        gen = db.hs.find_last(-key)
        for i in range(-key):
            try:
                result = next(gen)
            except StopIteration:
                raise IndexError(
                    "There are only {0} runs.".format(i))
    return [(result, safe_get_stop(db.hs, result))]


@search.register(str)
@search.register(six.text_type)
@search.register(*six.string_types)
def _(key, db):
    logger.info('Interpreting key = %s as a str' % key)
    results = None
    if len(key) == 36:
        # Interpret key as a complete uid.
        # (Try this first, for performance.)
        logger.debug('Treating %s as a full uuid' % key)
        results = list(db.hs.find_run_starts(uid=key))
        logger.debug('%s runs found for key=%s treated as a full uuid'
                     % (len(results), key))
    if not results:
        # No dice? Try searching as if we have a partial uid.
        logger.debug('Treating %s as a partial uuid' % key)
        gen = db.hs.find_run_starts(uid={'$regex': '^{0}'.format(key)})
        results = list(gen)
    if not results:
        # Still no dice? Bail out.
        raise KeyError("No such run found for key=%r" % key)
    if len(results) > 1:
        raise ValueError("key=%r matches %d runs. Provide "
                         "more characters." % (key, len(results)))
    result, = results
    return [(result, safe_get_stop(db.hs, result))]


@search.register(set)
@search.register(tuple)
@search.register(MutableSequence)
def _(key, db):
    logger.info('Interpreting key = {} as a set, tuple or MutableSequence'
                ''.format(key))
    return sum((search(k, db) for k in key), [])


class Results(object):
    """
    Iterable object encapsulating a results set of Headers

    Parameters
    ----------
    res : iterable
        Iterable of ``(start_doc, stop_doc)`` pairs
    db : :class:`Broker`
    data_key : string or None
        Special query parameter that filters results
    """
    def __init__(self, res, db, data_key):
        self._db = db
        self._res = res
        self._data_key = data_key

    def __iter__(self):
        self._res, res = itertools.tee(self._res)
        for start, stop in res:
            header = Header.from_run_start(self._db, start, stop)
            if self._data_key is None:
                yield header
            else:
                # Only include this header in the result if `data_key` is found
                # in one of its descriptors' data_keys.
                for descriptor in header['descriptors']:
                    if self._data_key in descriptor['data_keys']:
                        yield header
                        break


def load_cls(config):
    # This is for loading a class from a configuration dict that provides
    # a module name and class name.
    modname = config['module']
    clsname = config['class']
    mod = import_module(modname)
    cls = getattr(mod, clsname)
    return cls


def temp_config():
    """
    Generate Broker configuration backed by temporary, disposable databases.

    This is suitable for testing and experimentation, but it is not recommended
    for large or important data.

    Returns
    -------
    config : dict

    Examples
    --------
    This is the fastest way to get up and running with a Broker.

    >>> c = temp_config()
    >>> db = Broker.from_config(c)
    """
    tempdir = tempfile.mkdtemp()
    config = {
        'description': 'temporary',
        'metadatastore': {
            'module': 'databroker.headersource.sqlite',
            'class': 'MDS',
            'config': {
                'directory': tempdir,
                'timezone': 'US/Eastern'}
        },
        'assets': {
            'module': 'databroker.assets.sqlite',
            'class': 'Registry',
            'config': {
                'dbpath': os.path.join(tempdir, 'assets.sqlite')}
        }
    }
    return config


class BrokerES(object):
    """
    Unified interface to data sources

    Parameters
    ----------
    hs : HeaderSource
    event_sources : List[EventSource]
        zero, one or more EventSource objects
    assets : List[AssetRegistry]
    external_fetchers : Dict[str, Callable[Start, Stop]]
    name : str, optional
        The name of the broker
    """
    def __init__(self, hs, event_sources, assets, external_fetchers,
                 name=None):
        self.hs = hs
        self.event_sources = event_sources
        self.assets = assets
        self.name = name
        self.external_fetchers = external_fetchers
        # Once we drop Python 2, we can accept initial filter and aliases as
        # keyword-only args if we want to.
        self.filters = {}
        self.aliases = {}
        self.event_source_for_insert = self.event_sources[0]
        self.registry_for_insert = self.event_sources[0]
        self.prepare_hook = wrap_in_deprecated_doct

    def fetch_external(self, start, stop):
        return {k: func(start, stop) for
                k, func in self.external_fetchers.items()}

    @property
    def event_sources_by_name(self):
        '''Mapping between names and EventSources'''
        es = {}
        for event_source in self.event_sources:
            es[event_source.name] = event_source
        return es

    def add_event_source(self, es):
        '''Add an EventSource to the Broker'''
        self.event_sources.append(es)

    def stream_names_given_header(self, header):
        return [n for es in self.event_sources
                for n in es.stream_names_given_header(header)]

    def insert(self, name, doc):
        """
        Insert a new document.

        Parameters
        ----------
        name : {'start', 'descriptor', 'event', 'stop'}
            Document type
        doc : dict
            Document
        """
        if name in {'event', 'bulk_events', 'descriptor'}:
            return self.event_source_for_insert.insert(name, doc)
        # We are transitioning from ophyd objects inserting directly into a
        # Registry to ophyd objects passing documents to the RunEngine which in
        # turn inserts them into a Registry. During the transition period, we
        # allow an ophyd object to attempt BOTH so that configuration files are
        # compatible with both the new model and the old model. Thus, we
        # need to ignore the second attempt to insert.
        elif name == 'datum':
            return self.reg.insert_datum(ignore_duplicate_error=True, **doc)
        elif name == 'bulk_datum':
            return self.reg.bulk_insert_datum(ignore_duplicate_error=True,
                                              **doc)
        elif name == 'resource':
            return self.reg.insert_resource(ignore_duplicate_error=True, **doc)
        elif name in {'start', 'stop'}:
            return self.hs.insert(name, doc)
        else:
            raise ValueError

    @property
    def mds(self):
        return self.hs.mds

    @property
    def reg(self):
        return self.assets['']

    @property
    def fs(self):
        warnings.warn("fs is deprecated, use `db.reg` instead",
                      stacklevel=2)
        return self.reg

    ALL = ALL  # sentinel used as default value for `stream_name`

    def add_filter(self, **kwargs):
        """
        Add query to the list of 'filter' queries.

        Any query passed to ``db.add_filter()`` is stashed and "AND-ed" with
        all future queries.

        ``db.add_filter(**kwargs)`` is just a convenient way to spell
        ``db.filters.update(**kwargs)``.

        Examples
        --------
        Filter all searches to restrict results to a specific user after a
        March 2017.

        >>> db.add_filter(user='Dan')
        >>> db.add_filter(since='2017-3')

        The following query is equivalent to
        ``db(user='Dan', plan_name='scan')``.

        >>> db(plan_name='scan')

        Review current filters.

        >>> db.filters
        {'user': 'Dan', 'since': '2017-3'}

        Clear filters.

        >>> db.clear_filters()

        See Also
        --------
        :meth:`Broker.clear_filters`

        """
        self.filters.update(**kwargs)

    def clear_filters(self, **kwargs):
        """
        Clear all 'filter' queries.

        Filter queries are combined with every given query using '$and',
        acting as a filter to restrict the results.

        ``Broker.clear_filters()`` is just a convenient way to spell
        ``Broker.filters.clear()``.

        See Also
        --------
        :meth:`Broker.add_filter`
        """
        self.filters.clear()

    def __getitem__(self, key):
        """
        Search runs based on recently, unique id, or counting number scan_id.

        This function returns a :class:`Header` object (or a list of them, if
        the input is a list or slice). Each Header encapsulates the metadata
        for a run -- start time, instruments used, and so on, and provides
        methods for loading the data.

        Examples
        --------
        Get the most recent run.

        >>> header = db[-1]

        Get the fifth most recent run.

        >>> header = db[-5]

        Get a list of all five most recent runs, using Python slicing syntax.

        >>> headers = db[-5:]

        Get a run whose unique ID ("RunStart uid") begins with 'x39do5'.

        >>> header = db['x39do5']

        Get a run whose integer scan_id is 42. Note that this might not be
        unique.  In the event of duplicates, the most recent match is returned.

        >>> header = db[42]
        """
        ret = [Header.from_run_start(self, start, stop)
               for start, stop in search(key, self)]
        squeeze = not isinstance(key, (set, tuple, MutableSequence, slice))
        if squeeze and len(ret) == 1:
            ret, = ret
        return ret

    def __getattr__(self, key):
        try:
            query = self.aliases[key]
        except KeyError:
            raise AttributeError(key)
        if callable(query):
            query = query()
        return self(**query)

    def alias(self, key, **query):
        """
        Create an alias for a query.

        Parameters
        ----------
        key : string
            must be a valid Python identifier
        query :
            keyword argument comprising a query

        Examples
        --------
        Define an alias that searches for headers with purpose='calibration'.

        >>> db.alias('cal', purpose='calibration')

        Use it.

        >>> headers = db.cal  # -> db(purpose='calibration')

        Review defined aliases.

        >>> db.aliases
        {'cal': {'purpose': 'calibration'}}
        """
        if hasattr(self, key) and key not in self.aliases:
            raise ValueError("'%s' is not a legal alias." % key)
        self.aliases[key] = query

    def dynamic_alias(self, key, func):
        """
        Create an alias for a "dynamic" query, a function that returns a query.

        Parameters
        ----------
        key : string
            must be a valid Python identifier
        func : callable
            When called with no arguments, must return a dict that is a valid
            query.

        Examples
        --------
        Define an alias to get headers from the last 24 hours.

        >>> import time
        >>> db.dynamic_alias('today',
        ...                  lambda: {'since': time.time() - 24*60*60})

        Use it.

        >>> headers = db.today

        Define an alias to get headers with the 'user' field in metadata
        matches the current logged-in user.

        >>> import getpass
        >>> db.dynamic_alias('mine', lambda: {'user': getpass.getuser()})

        Use it

        >>> headers = db.mine
        """
        if hasattr(self, key) and key not in self.aliases:
            raise ValueError("'%s' is not a legal alias." % key)
        self.aliases[key] = func

    def __call__(self, text_search=None, **kwargs):
        """Search runs based on metadata.

        This function returns an iterable of :class:`Header` objects. Each
        Header encapsulates the metadata for a run -- start time, instruments
        used, and so on, and provides methods for loading the data. In
        addition to the Parameters below, advanced users can specify arbitrary
        queries using the MongoDB query syntax.

        The ``since`` and ``until`` parameters accepts the following
        representations of time:

        * timestamps like ``time.time()`` and ``datetime.datetime.now()``
        * ``'2015'``
        * ``'2015-01'``
        * ``'2015-01-30'``
        * ``'2015-03-30 03:00:00'``

        Parameters
        ----------
        text_search : str, optional
            search full text of RunStart documents
        since: str, optional
            Restrict results to runs that started after this time.
        until : str, optional
            Restrict results to runs that started before this time.
        data_key : str, optional
            Restrict results to runs that contained this data_key (a.k.a field)
            such as 'intensity' or 'temperature'.
        **kwargs
            query parameters

        Returns
        -------
        data : :class:`Results`
            Iterable object encapsulating a results set of Headers

        Examples
        --------
        Search by plan name.

        >>> db(plan_name='scan')

        Search for runs involving a motor with the name 'eta'.

        >>> db(motor='eta')

        Search for runs operated by a given user---assuming this metadata was
        recorded in the first place!

        >>> db(operator='Dan')

        Search by time range. (These keywords have a special meaning.)

        >>> db(since='2015-03-05', until='2015-03-10')

        Perform text search on all values in the Run Start document.

        >>> db('keyword')

        Note that partial words are not matched, but partial phrases are. For
        example, 'good' will match to 'good sample' but 'goo' will not.

        Richer logic that simple 'key=value' searches are supported by the
        Mongo query syntax.

        Match all headers that include metadata for the key 'sample',
        regardless of what its value is.

        >>> db(sample={'$exists': True})

        Match all headers where the value of 'plan_name' is NOT
        'relative_scan'.

        >>> db(plan_name={'$ne': 'relative_scan'})

        Read the
        `MongoDB query documentation
        <http://docs.mongodb.org/manual/tutorial/query-documents/>`_ for more.
        """
        data_key = kwargs.pop('data_key', None)

        res = self.hs(text_search=text_search,
                      filters=self.filters,
                      **kwargs)

        return Results(res, self, data_key)

    def fill_event(self, event, inplace=True, handler_registry=None):
        """
        Deprecated, use `fill_events` instead.

        Populate events with externally stored data.

        Parameters
        ----------
        event : document
        inplace : bool, optional
            If the event should be filled 'in-place' by mutating the data
            dictionary.  Defaults to `True`.
        handler_registry : dict, optional
            mapping spec names (strings) to handlers (callable classes)
        """
        warnings.warn("fill_event is deprecated, use fill_events instead",
                      stacklevel=2)
        # TODO sort out how to (quickly) map events back to the
        # correct event Source
        desc_id = event['descriptor']
        descs = []
        for es in self.event_sources:
            try:
                d = es.descriptor_given_uid(desc_id)
            except es.NoEventDescriptors:
                pass
            else:
                descs.append(d)

        # dirty hack!
        with self.reg.handler_context(handler_registry):
            ev_out, = self.fill_events([event], descs, inplace=inplace)
        return ev_out

    def get_events(self,
                   headers, stream_name='primary', fields=None, fill=False,
                   handler_registry=None):
        """
        Get Event documents from one or more runs.

        Parameters
        ----------
        headers : Header or iterable of Headers
            The headers to fetch the events for

        stream_name : str, optional
            Get events from only "event stream" with this name.

            Default is 'primary'

        fields : List[str], optional
            whitelist of field names of interest; if None, all are returned

            Default is None

        fill : bool or Iterable[str], optional
            Which fields to fill.  If `True`, fill all
            possible fields.

            Each event will have the data filled for the intersection
            of it's external keys and the fields requested filled.

            Default is False

        handler_registry : dict, optional
            mapping asset specs (strings) to handlers (callable classes)

        Yields
        ------
        event : Event
            The event, optionally with non-scalar data filled in

        Raises
        ------
        ValueError if any key in `fields` is not in at least one descriptor
        pre header.
        """
        for name, doc in self.get_documents(headers,
                                            fields=fields,
                                            stream_name=stream_name,
                                            fill=fill,
                                            handler_registry=handler_registry):
            if name == 'event':
                yield doc

    def get_documents(self,
                      headers, stream_name=ALL, fields=None, fill=False,
                      handler_registry=None):
        """
        Get all documents from one or more runs.

        Parameters
        ----------
        headers : Header or iterable of Headers
            The headers to fetch the events for

        stream_name : str, optional
            Get events from only "event stream" with this name.

            Default is `ALL` which yields documents for all streams.

        fields : List[str], optional
            whitelist of field names of interest; if None, all are returned

            Default is None

        fill : bool or Iterable[str], optional
            Which fields to fill.  If `True`, fill all
            possible fields.

            Each event will have the data filled for the intersection
            of it's external keys and the fields requested filled.

            Default is False

        handler_registry : dict, optional
            mapping asset pecs (strings) to handlers (callable classes)

        Yields
        ------
        name : str
            The name of the kind of document

        doc : dict
            The payload, may be RunStart, RunStop, EventDescriptor, or Event.

        Raises
        ------
        ValueError if any key in `fields` is not in at least one descriptor
        pre header.
        """
        try:
            headers.items()
        except AttributeError:
            pass
        else:
            headers = [headers]

        # dirty hack!
        with self.reg.handler_context(handler_registry):
            for h in headers:
                external_keys = set()
                resources = set()
                if not isinstance(h, Header):
                    h = self[h['start']['uid']]
                # TODO filter fill by fields
                # TODO: eliminate this in favor of the Retrieve callback
                proc_gen = self._fill_events_coro(h.descriptors,
                                                  fields=fill,
                                                  inplace=True)
                proc_gen.send(None)
                for es in self.event_sources:
                    gen = es.docs_given_header(
                        header=h, stream_name=stream_name,
                        fields=fields)
                    for name, doc in gen:
                        # Find all the res/datum backed keys
                        if name == 'descriptor':
                            for k, v in six.iteritems(doc['data_keys']):
                                if 'external' in v:
                                    external_keys.add(k)
                        if name == 'event':
                            # for external keys get res/datum
                            for k, v in six.iteritems(doc['data']):
                                if k in external_keys:
                                    try:
                                        datum = self.reg.get_datum_from_datum_id(v)
                                        if datum['resource'] not in resources:
                                            res = self.reg.resource_given_datum_id(
                                                v)
                                            yield 'resource', self.prepare_hook(
                                                name, res)
                                            resources.add(res['uid'])
                                        yield 'datum', self.prepare_hook(name,
                                                                         datum)
                                    except DatumNotFound as dnf:
                                        raise EventDatumNotFound(
                                            event_uid=doc["uid"],
                                            datum_id=dnf.datum_id
                                        ) from dnf

                            doc = proc_gen.send(doc)

                        yield name, self.prepare_hook(name, doc)
                proc_gen.close()

    def get_table(self,
                  headers, stream_name='primary', fields=None, fill=False,
                  handler_registry=None,
                  convert_times=True, timezone=None, localize_times=True):
        """
        Load the data from one or more runs as a table (``pandas.DataFrame``).

        Parameters
        ----------
        headers : Header or iterable of Headers
            The headers to fetch the events for

        stream_name : str, optional
            Get events from only "event stream" with this name.

            Default is 'primary'

        fields : List[str], optional
            whitelist of field names of interest; if None, all are returned

            Default is None

        fill : bool or Iterable[str], optional
            Which fields to fill.  If `True`, fill all
            possible fields.

            Each event will have the data filled for the intersection
            of it's external keys and the fields requested filled.

            Default is False

        handler_registry : dict, optional
            mapping filestore specs (strings) to handlers (callable classes)

        convert_times : bool, optional
            Whether to convert times from float (seconds since 1970) to
            numpy datetime64, using pandas. True by default.

        timezone : str, optional
            e.g., 'US/Eastern'; if None, use metadatastore configuration in
            `self.mds.config['timezone']`

        handler_registry : dict, optional
            mapping asset specs (strings) to handlers (callable classes)

        localize_times : bool, optional
            If the times should be localized to the 'local' time zone.  If
            True (the default) the time stamps are converted to the localtime
            zone (as configure in mds).

            This is problematic for several reasons:

              - apparent gaps or duplicate times around DST transitions
              - incompatibility with every other time stamp (which is in UTC)

            however, this makes the dataframe repr look nicer

            This implies convert_times.

            Defaults to True to preserve back-compatibility.

        Returns
        -------
        table : pandas.DataFrame
        """
        try:
            headers.items()
        except AttributeError:
            pass
        else:
            headers = [headers]

        dfs = []
        # dirty hack!
        with self.reg.handler_context(handler_registry):
            for h in headers:
                if not isinstance(h, Header):
                    h = self[h['start']['uid']]

                # get the first descriptor for this event stream
                desc = next((d for d in h.descriptors
                             if d.get('name') == stream_name),
                            None)
                if desc is None:
                    continue
                for es in self.event_sources:
                    table = es.table_given_header(
                        header=h,
                        fields=fields,
                        stream_name=stream_name,
                        convert_times=convert_times,
                        timezone=timezone,
                        localize_times=localize_times)
                    if len(table):
                        if fill:
                            table = self.fill_table(table, desc, inplace=True)
                        dfs.append(table)

        if dfs:
            df = pd.concat(dfs)
        else:
            # edge case: no data
            df = pd.DataFrame()
        df.index.name = 'seq_num'

        return df

    def get_images(self, headers, name,
                   stream_name='primary',
                   handler_registry=None,):
        """
        This method is deprecated. Use Broker.get_documents instead.

        Load image data from one or more runs into a lazy array-like object.

        Parameters
        ----------
        headers : Header or list of Headers
        name : string
            field name (data key) of a detector
        handler_registry : dict, optional
            mapping spec names (strings) to handlers (callable classes)

        Examples
        --------
        >>> header = db[-1]
        >>> images = Images(header, 'my_detector_lightfield')
        >>> for image in images:
                # do something
        """

        # TODO sort out how to broadcast this
        return Images(mds=self.mds, reg=self.reg, es=self.event_sources[0],
                      headers=headers,
                      name=name, stream_name=stream_name,
                      handler_registry=handler_registry)

    def get_resource_uids(self, header):
        '''Given a Header, give back a list of resource uids

        These uids are required to move the underlying files.

        Parameters
        ----------
        header : Header

        Returns
        -------
        ret : set
            set of resource uids which are refereneced by this Header.
        '''
        external_keys = set()
        for d in header['descriptors']:
            for k, v in six.iteritems(d['data_keys']):
                if 'external' in v:
                    external_keys.add(k)
        ev_gen = self.get_events(header, stream_name=ALL,
                                 fields=external_keys, fill=False)
        resources = set()
        for ev in ev_gen:
            for k, v in six.iteritems(ev['data']):
                if k in external_keys:
                    res = self.reg.resource_given_datum_id(v)
                    resources.add(res['uid'])
        return resources

    def restream(self, headers, fields=None, fill=False):
        """
        Get all Documents from given run(s).

        This output can be used as a drop-in replacement for the output of the
        bluesky Run Engine.

        Parameters
        ----------
        headers : Header or iterable of Headers
            header or headers to fetch the documents for
        fields : list, optional
            whitelist of field names of interest; if None, all are returned
        fill : bool, optional
            Whether externally-stored data should be filled in. Defaults to
            False.

        Yields
        ------
        name, doc : tuple
            string name of the Document type and the Document itself.
            Example: ('start', {'time': ..., ...})

        Examples
        --------
        >>> def f(name, doc):
        ...     # do something
        ...
        >>> h = db[-1]  # most recent header
        >>> for name, doc in restream(h):
        ...     f(name, doc)

        See Also
        --------
        :meth:`Broker.process`
        """
        for payload in self.get_documents(headers, fields=fields, fill=fill):
            yield payload

    stream = restream  # compat

    def process(self, headers, func, fields=None, fill=False):
        """
        Pass all the documents from one or more runs into a callback.

        This output can be used as a drop-in replacement for the output of the
        bluesky Run Engine.

        Parameters
        ----------
        headers : Header or iterable of Headers
            header or headers to process documents from
        func : callable
            function with the signature `f(name, doc)`
            where `name` is a string and `doc` is a dict
        fields : list, optional
            whitelist of field names of interest; if None, all are returned
        fill : bool, optional
            Whether externally-stored data should be filled in. Defaults to
            False.

        Examples
        --------
        >>> def f(name, doc):
        ...     # do something
        ...
        >>> h = db[-1]  # most recent header
        >>> db.process(h, f)

        See Also
        --------
        :meth:`Broker.restream`
        """
        for name, doc in self.get_documents(headers, fields=fields, fill=fill):
            func(name, doc)

    get_fields = staticmethod(get_fields)  # for convenience

    def export(self, headers, db, new_root=None, copy_kwargs=None):
        """
        Export a list of headers.

        Parameters
        ----------
        headers : databroker.header
            one or more headers that are going to be exported
        db : databroker.Broker
            an instance of databroker.Broker class that will be the target to
            export info
        new_root : str
            optional. root directory of files that are going to
            be exported
        copy_kwargs : dict or None
            passed through to the ``copy_files`` method on Registry;
            None by default

        Returns
        ------
        file_pairs : list
            list of (old_file_path, new_file_path) pairs generated by
            ``copy_files`` method on Registry.
        """
        if copy_kwargs is None:
            copy_kwargs = {}
        try:
            headers.items()
        except AttributeError:
            pass
        else:
            headers = [headers]
        file_pairs = []
        for header in headers:
            # insert mds
            db.mds.insert_run_start(**_sanitize(header['start']))
            events = self.get_events(header)
            for descriptor in header['descriptors']:
                db.mds.insert_descriptor(**_sanitize(descriptor))
                for event in events:
                    db.mds.insert_event(**_sanitize(event))
            db.mds.insert_run_stop(**_sanitize(header['stop']))
            # insert assets
            res_uids = self.get_resource_uids(header)

            for uid in res_uids:
                fps = self.reg.copy_files(uid, new_root=new_root,
                                          **copy_kwargs)
                file_pairs.extend(fps)
                res = self.reg.resource_given_uid(uid)
                r_path = res['resource_path']
                out_root = new_root
                if os.path.isabs(r_path):
                    if not res['root']:
                        r_path = os.path.relpath(r_path, os.path.sep)
                        if new_root is None:
                            out_root = os.path.sep
                    else:
                        raise RuntimeError("Resource has non-trivial root "
                                           "and an absolute resource path.  "
                                           "This is inconsistent, aborting")
                else:
                    if new_root is None:
                        out_root = res['root']
                    else:
                        out_root = new_root

                new_res = db.reg.insert_resource(res['spec'],
                                                 r_path,
                                                 res['resource_kwargs'],
                                                 root=out_root)
                # Note that new_res has a different resource id than res.
                datums = self.reg.datum_gen_given_resource(uid)
                for datum in datums:
                    db.reg.insert_datum(new_res,
                                        datum['datum_id'],
                                        datum['datum_kwargs'])
        return file_pairs

    def export_size(self, headers):
        """
        Get the size of files associated with a list of headers.

        Parameters
        ----------
        headers : :class:databroker.Header:
            one or more headers that are going to be exported

        Returns
        -------
        total_size : float
            total size of all the files associated with the ``headers`` in Gb
        """
        try:
            headers.items()
        except AttributeError:
            pass
        else:
            headers = [headers]
        total_size = 0
        for header in headers:
            # get files from assets
            res_uids = self.get_resource_uids(header)
            for uid in res_uids:
                datum_gen = self.reg.datum_gen_given_resource(uid)
                datum_kwarg_gen = (datum['datum_kwargs'] for datum in
                                   datum_gen)
                files = self.reg.get_file_list(uid, datum_kwarg_gen)
                for file in files:
                    total_size += os.path.getsize(file)

        return total_size * 1e-9

    def fill_events(self, events, descriptors, fields=True, inplace=False):
        """Fill a sequence of events

        This method will be used both inside of other `Broker`
        methods and in user code.  If being used with *inplace=True* then
        we do not call `~Broker.prepare_hook` on the way out as either

          - we are inside another `Broker` method which will call
            it on the way out

          - being called from outside and then we assume the only way
            the user got an event was through another `Broker`
            method, thus `~Broker.prepare_hook` has already been called
            and we do not want to call it again.

        If *inplace=False* we are being called from user code and
        should receive as input the result of
        `~Broker.prepare_hook`.  We sanitize, copy, and then re-apply
        `~Broker.prepare_hook` to not change the type of the Event.

        If a field is filled, then the *filled* dict on the event is updated
        to hold the datum id and the *data* dict is update to hold the data.

        Parameters
        ----------
        events : Iterable[Event]
            An iterable of Event documents.

        descriptors : Iterable[EventDescriptor]
            An iterable of EventDescriptor documents.  This must
            contain the descriptor associated with each and every event
            you want to fill and may contain descriptors which are not
            used by any of the events.

        fields : bool or Iterable[str], optional
            Which fields to fill.  If `True`  fill all
            possible fields.

            Each event will have the data filled for the intersection
            of it's external keys and the fields requested filled.

            Default is True

        inplace : bool, optional
            If the input Events should be mutated inplace

        Yields
        ------
        ev : Event
            Event documents with filled data.

        """
        # create the processing generator
        proc_gen = self._fill_events_coro(descriptors,
                                          fields=fields, inplace=inplace)
        # and prime it
        proc_gen.send(None)

        try:
            for ev in events:
                yield proc_gen.send(ev)
        finally:
            proc_gen.close()

    def _fill_events_coro(self, descriptors, fields=True, inplace=False):
        if fields is True:
            fields = None
        elif fields is False:
            # if no fields, we got nothing to do!
            # just yield back as-is
            ev = yield
            while True:
                ev = yield ev
            return
        elif fields:
            fields = set(fields)
        registry_map = {}
        fill_map = defaultdict(list)
        for d in descriptors:
            fill_keys = set()
            desc_id = d['uid']
            for k, v in six.iteritems(d['data_keys']):
                ext = v.get('external', None)
                if ext:
                    # TODO sort this out!
                    # _, _, reg_name = ext.partition(':')
                    reg_name = ''
                    registry_map[(desc_id, k)] = self.assets[reg_name]
                    fill_keys.add(k)
            if fields is not None:
                fill_keys &= fields
            fill_map[desc_id] = fill_keys
        ev = yield

        while True:
            if not inplace:
                ev = _sanitize(ev)
                ev = copy.deepcopy(ev)
                ev = self.prepare_hook('event', ev)
            data = ev['data']
            filled = ev['filled']
            desc_id = ev['descriptor']
            for dk in fill_map[desc_id]:
                if not filled.get(dk, True):
                    d_id = data[dk]
                    data[dk] = (registry_map[(desc_id, dk)]
                                .retrieve(d_id))
                    filled[dk] = d_id

            ev = yield ev

    def fill_table(self, table, descriptor, fields=None, inplace=False):
        """Fill a table

        """
        if fields is True:
            fields = None
        elif fields is False:
            # if no fields, we got nothing to do!
            # just return the events as-is
            return table
        elif fields:
            fields = set(fields)
        # TODO unify this code with the code above.
        fill_keys = set()
        registry_map = {}
        for k, v in six.iteritems(descriptor['data_keys']):
            ext = v.get('external', None)
            if ext:
                # TODO sort this out!
                # _, _, reg_name = ext.partition(':')
                reg_name = ''
                registry_map[k] = self.assets[reg_name]
                fill_keys.add(k)
        if fields is not None:
            fill_keys &= fields

        if not inplace:
            table = table.copy()

        for k in fill_keys:
            reg = registry_map[k]
            # TODO someday we will have bulk retrieve on assets.Registry
            table[k] = [reg.retrieve(value) for value in table[k]]

        return table


class Broker(BrokerES):
    """
    Unified interface to data sources

    Eventually this API will change to
    ``__init__(self, hs, **event_sources)``

    Parameters
    ----------
    mds : object
        implementing the 'metadatastore interface'
    reg : object
        implementing the 'assets interface'
    auto_register : boolean, optional
        By default, automatically register built-in asset handlers (classes
        that handle I/O for externally stored data). Set this to ``False``
        to do all registration manually.
    external_fetchers, optional : Dict[str, Callable[Start, Stop]]
        Used to attach extra data to the returned Headers in the `h.ext`
        namespace.
    name : str, optional
        The name of the broker
    """

    def __init__(self, mds, reg=None, plugins=None, filters=None,
                 auto_register=True, external_fetchers=None,
                 name=None, event_sources=None):
        if plugins is not None:
            raise ValueError("The 'plugins' argument is no longer supported. "
                             "Use an EventSource instead.")
        if filters is None:
            filters = {}
        if external_fetchers is None:
            external_fetchers = {}
        if filters:
            warnings.warn("Future versions of the databroker will not accept "
                          "'filters' in __init__. Set them using the filters "
                          "attribute after initialization.", stacklevel=2)

        ess = [EventSourceShim(mds, reg)]
        if event_sources:
            ess += event_sources
        super(Broker, self).__init__(HeaderSourceShim(mds),
                                     ess,
                                     {'': reg}, external_fetchers, name=name)
        self.filters = filters
        if auto_register:
            register_builtin_handlers(self.reg)

    @classmethod
    def from_config(cls, config, auto_register=True, name=None):
        """
        Create a new Broker instance using a dictionary of configuration.

        Parameters
        ----------
        config : dict
        auto_register : boolean, optional
            By default, automatically register built-in asset handlers (classes
            that handle I/O for externally stored data). Set this to ``False``
            to do all registration manually.
        name : str, optional
            The name of the generated Broker
        Returns
        -------
        db : Broker

        Examples
        --------
        Create a Broker backed by sqlite databases. (This is configuration is
        not recommended for large or important deployments. See the
        configuration documentation for more.)

        >>> config = {
        ...     'description': 'lightweight personal database',
        ...     'metadatastore': {
        ...         'module': 'databroker.headersource.sqlite',
        ...         'class': 'MDS',
        ...         'config': {
        ...             'directory': 'some_directory',
        ...             'timezone': 'US/Eastern'}
        ...     },
        ...     'assets': {
        ...         'module': 'databroker.assets.sqlite',
        ...         'class': 'Registry',
        ...         'config': {
        ...             'dbpath': assets_dir + '/database.sql'}
        ...     }
        ... }
        ...

        >>> Broker.from_config(config)
        """
        # Import component classes.
        mds_cls = load_cls(config['metadatastore'])
        assets_cls = load_cls(config['assets'])
        # Instantiate component classes.
        mds = mds_cls(config['metadatastore']['config'])
        assets = assets_cls(config['assets']['config'])
        # Instantiate event sources.
        event_sources = []
        ess = []
        if 'event_sources' in config.keys():
            ess = config['event_sources']
        for es in ess:
            es_cls = load_cls(es)
            event_sources.append(es_cls(es['config']))
        # Instantiate Broker.
        db = cls(mds, assets, auto_register=auto_register,
                 name=name, event_sources=event_sources)
        # Register handlers included in the config, if any.
        for spec, handler in config.get('handlers', {}).items():
            cls = load_cls(handler)
            db.reg.register_handler(spec, cls)
        # if 'root_map' in config, set the root_map
        if 'root_map' in config:
            root_map = config['root_map']
            if not isinstance(root_map, dict):
                raise TypeError("root_map is not right type in config file. "
                                "It must be a dict of mappings"
                                " (see documentation)")
            db.reg.set_root_map(root_map)
        return db

    @classmethod
    def named(cls, name, auto_register=True):
        """
        Create a new Broker instance using a configuration file with this name.

        Configuration file search path:

        * ``~/.config/databroker/{name}.yml``
        * ``{python}/../etc/databroker/{name}.yml``
        * ``/etc/databroker/{name}.yml``

        where ``{python}`` is the location of the current Python binary, as
        reported by ``sys.executable``. It will use the first match it finds.

        Special Case: The name ``'temp'`` creates a new, temporary
        configuration. Subsequent calls to ``Broker.named('temp')`` will
        create separate configurations. Any data saved using this temporary
        configuration will not be accessible once the ``Broker`` instance has
        been deleted.

        Parameters
        ----------
        name : string
        auto_register : boolean, optional
            By default, automatically register built-in asset handlers (classes
            that handle I/O for externally stored data). Set this to ``False``
            to do all registration manually.

        Returns
        -------
        db : Broker
        """
        if name == 'temp':
            config = temp_config()
        else:
            config = lookup_config(name)
        db = cls.from_config(config, auto_register=auto_register, name=name)
        return db

    def get_config(self):
        ''' Get the config for this Broker.
            To be used by Broker.from_config(config)
        '''
        config = dict()
        for k, v in zip(['metadatastore', 'assets'], [self.mds, self.reg]):
            config[k] = {'config': v.config,
                         'module': v.__module__,
                         'class': v.__class__.__name__}
        config['root_map'] = self.reg.root_map
        return config


def _sanitize(doc):
    # Make this a plain dict and strip off doct.Document artifacts.
    d = dict(doc)
    d.pop('_name', None)
    return d


_HTML_TEMPLATE = """
{% macro rtable(doc, cap) -%}
<table>
<caption> {{ cap }} </caption>
{%- for key, value in doc | dictsort recursive -%}
  <tr>
    <th> {{ key }} </th>
    <td>
      {%- if value.items -%}
        <table>
          {{ loop(value | dictsort) }}
        </table>
      {%- elif value is iterable and value is not string -%}
        <table>
          {%- set outer_loop = loop -%}
          {%- for stuff in value  -%}
            {%- if stuff.items -%}
               {{ outer_loop(stuff | dictsort) }}
            {%- else -%}
              <tr><td>{{ stuff }}</td></tr>
            {%- endif -%}
          {%- endfor -%}
        </table>
      {%- else -%}
        {%- if key == 'time' -%}
          {{ value | human_time }}
        {%- else -%}
          {{ value }}
        {%- endif -%}
      {%- endif -%}
    </td>
  </tr>
{%- endfor -%}
</table>
{%- endmacro %}

<table>
  <tr>
    <td>{{ rtable(document.start, 'Start') }}</td>
  </tr
  <tr>
    <td>{{ rtable(document.stop, 'Stop') }}</td>
  </tr>
  <tr>
  <td>
      <table>
      <caption>Descriptors</caption>
         {%- for d in document.descriptors -%}
         <tr>
         <td> {{ rtable(d, d.get('name')) }} </td>
         </tr>
         {%- endfor -%}
      </table>
    </td>
</tr>
</table>
"""


def _pretty_print_time(timestamp):
    # timestamp needs to be a float or fromtimestamp() will barf
    timestamp = float(timestamp)
    dt = datetime.fromtimestamp(timestamp).isoformat()
    ago = humanize.naturaltime(time.time() - timestamp)
    return '{ago} ({date})'.format(ago=ago, date=dt)
