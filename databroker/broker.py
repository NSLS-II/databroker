from __future__ import print_function
from importlib import import_module
import itertools
import warnings
import six  # noqa
import logging
import numbers
import doct
import pandas as pd
import sys
import os
import yaml
import glob
import tempfile
import copy
from collections import defaultdict


from .core import (Header,
                   get_fields,  # for convenience
                   Images,
                   ALL, format_time,
                   register_builtin_handlers)
from .eventsource import EventSourceShim, check_fields_exist
from .headersource import HeaderSourceShim, safe_get_stop


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
    if key > -1:
        # Interpret key as a scan_id.
        gen = db.hs.find_run_starts(scan_id=key)
        try:
            result = next(gen)  # most recent match
        except StopIteration:
            raise ValueError("No such run found for key=%s which is "
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
@search.register(six.string_types,)
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
        gen = db.hs.find_run_starts(uid={'$regex': '{0}.*'.format(key)})
        results = list(gen)
    if not results:
        # Still no dice? Bail out.
        raise ValueError("No such run found for key=%r" % key)
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
            header = Header(start=self._db.prepare_hook('start', start),
                            stop=self._db.prepare_hook('stop', stop),
                            db=self._db)
            if self._data_key is None:
                yield header
            else:
                # Only include this header in the result if `data_key` is found
                # in one of its descriptors' data_keys.
                for descriptor in header['descriptors']:
                    if self._data_key in descriptor['data_keys']:
                        yield header
                        break

# Search order is:
# ~/.config/databroker
# <sys.executable directory>/../etc/databroker
# /etc/databroker


_user_conf = os.path.join(os.path.expanduser('~'), '.config', 'databroker')
_local_etc = os.path.join(os.path.dirname(os.path.dirname(sys.executable)),
                          'etc', 'databroker')
_system_etc = os.path.join('etc', 'databroker')
CONFIG_SEARCH_PATH = (_user_conf, _local_etc, _system_etc)


if six.PY2:
    FileNotFoundError = IOError


def list_configs():
    """
    List the names of the available configuration files.

    Returns
    -------
    names : list
    """
    names = []
    for path in CONFIG_SEARCH_PATH:
        files = glob.glob(os.path.join(path, '*.yml'))
        names.extend([os.path.basename(f)[:-4] for f in files])
    return sorted(names)


def lookup_config(name):
    """
    Search for a databroker configuration file with a given name.

    For exmaple, the name 'example' will cause the function to search for:

    * ``~/.config/databroker/example.yml``
    * ``{python}/../etc/databroker/example.yml``
    * ``/etc/databroker/example.yml``

    where ``{python}`` is the location of the current Python binary, as
    reported by ``sys.executable``. It will use the first match it finds.

    Parameters
    ----------
    name : string

    Returns
    -------
    config : dict
    """
    if not name.endswith('.yml'):
        name += '.yml'
    tried = []
    for path in CONFIG_SEARCH_PATH:
        filename = os.path.join(path, name)
        tried.append(filename)
        if os.path.isfile(filename):
            with open(filename) as f:
                return yaml.load(f)
    else:
        raise FileNotFoundError("No config file named {!r} could be found in "
                                "the following locations:\n{}"
                                "".format(name, '\n'.join(tried)))


def load_component(config):
    modname = config['module']
    clsname = config['class']
    config = config['config']
    mod = import_module(modname)
    cls = getattr(mod, clsname)
    return cls(config)


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


DOCT_NAMES = {'resource': 'Resource',
              'datum': 'Datum',
              'descriptor': 'Event Descriptor',
              'event': 'Event',
              'start': 'Run Start',
              'stop': 'Run Stop'}


def wrap_in_doct(name, doc):
    """
    Put document contents into a doct.Document object.

    A ``doct.Document`` is a subclass of dict that:

    * is immutable
    * provides human-readable :meth:`__repr__` and :meth:`__str__`
    * supports dot access (:meth:`__getattr__`) as a synonym for item access
      (:meth:`__getitem__`) whenever possible
    """
    return doct.Document(DOCT_NAMES[name], doc)


_STANDARD_DICT_ATTRS = dir(dict)


class DeprecatedDoct(doct.Document):
    "Subclass of doct.Document that warns that dot access may be removed."
    def __getattribute__(self, key):
        # Get the result first and let any errors be raised.
        res = super(DeprecatedDoct, self).__getattribute__(key)
        # Now warn before returning it.
        if key not in _STANDARD_DICT_ATTRS:
            # This is not a standard dict attribute.
            # Warn that dot access is deprecated.
            warnings.warn("Dot access may be removed in a future version."
                          "Use [{0}] instead of .{0}".format(key))
        return res


def wrap_in_deprecated_doct(name, doc):
    """
    Put document contents into a DeprecatedDoct object.

    See :func:`wrap_in_doct`. The difference between :class:`DeprecatedDoct`
    and :class:`doct.Document` is a warning that dot access
    (:meth:`__getattr__` as a synonym for :meth:`__getitem__`) may be removed
    in the future.
    """
    return DeprecatedDoct(DOCT_NAMES[name], doc)


class BrokerES(object):
    """
    Unified interface to data sources

    Parameters
    ----------
    hs : HeaderSource
    *event_sources :
        zero, one or more EventSource objects
    """
    def __init__(self, hs, event_sources, assets):
        self.hs = hs
        self.event_sources = event_sources
        self.assets = assets
        # Once we drop Python 2, we can accept initial filter and aliases as
        # keyword-only args if we want to.
        self.filters = []
        self.aliases = {}
        self.event_source_for_insert = self.event_sources[0]
        self.registry_for_insert = self.event_sources[0]
        self.prepare_hook = wrap_in_deprecated_doct

    def add_event_source(self, es):
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
        if name in {'start', 'stop'}:
            return self.hs.insert(name, doc)
        else:
            return self.event_source_for_insert.insert(name, doc)

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

    def _format_time(self, val):
        "close over the timezone config"
        # modifies a query dict in place, remove keys 'start_time' and
        # 'stop_time' and adding $lte and/or $gte queries on 'time' key
        format_time(val, self.hs.mds.config['timezone'])

    @property
    def filters(self):
        return self._filters

    @filters.setter
    def filters(self, val):
        for elem in val:
            self._format_time(elem)
        self._filters = val

    def add_filter(self, **kwargs):
        """
        Add query to the list of 'filter' queries.

        Filter queries are combined with every given query using '$and',
        acting as a filter to restrict the results.

        ``db.add_filter(**kwargs)`` is just a convenient way to spell
        ``db.filters.append(dict(**kwargs))``.

        Examples
        --------
        Filter all searches to restrict runs to a specific 'user'.

        >>> db.add_filter(user='Dan')

        See Also
        --------
        :meth:`Broker.clear_filters`

        """
        d = dict(**kwargs)
        for val in self.filters:
            for k,v in val.items():
                if k in d.keys():
                    raise ValueError('Filter key already exists.')
        self.filters.append(d)

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
        ret = [Header(start=self.prepare_hook('start', start),
                      stop=self.prepare_hook('stop', stop),
                      db=self)
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
        >>> db.alias('cal', purpose='calibration')
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
        Get headers from the last 24 hours.
        >>> import time
        >>> db.dynamic_alias('today',
                             lambda: {'start_time':
                                      start_time=time.time()- 24*60*60})
        """
        if hasattr(self, key) and key not in self.aliases:
            raise ValueError("'%s' is not a legal alias." % key)
        self.aliases[key] = func

    def __call__(self, text_search=None, **kwargs):
        """Search runs based on metadata.

        This function returns an iterable of :class:`Header` objects. Each
        Header encapsulates the metadata for a run -- start time, instruments
        used, and so on, and provides methods for loading the data. In
        addition to the Parameters below, advanced users can specifiy arbitrary
        queries using the MongoDB query syntax.

        The ``start_time`` and ``stop_time`` parameters accepts the following
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
        start_time : str, optional
            Restrict results to runs that started after this time.
        stop_time : str, optional
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

        >>> db(start_time='2015-03-05', stop_time='2015-03-10')

        Perform text search on all values in the Run Start document.

        >>> db('keyword')

        Note that partial words are not matched, but partial phrases are. For
        example, 'good' will match to 'good sample' but 'goo' will not.
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

        check_fields_exist(fields if fields else [], headers)
        # dirty hack!
        with self.reg.handler_context(handler_registry):
            for h in headers:
                if not isinstance(h, Header):
                    h = self[h['start']['uid']]
                # TODO filter fill by fields
                proc_gen = self._fill_events_coro(h.descriptors,
                                                  fields=fill,
                                                  inplace=True)
                proc_gen.send(None)
                for es in self.event_sources:
                    gen = es.docs_given_header(
                        header=h, stream_name=stream_name,
                        fields=fields)
                    for name, doc in gen:
                        if name == 'event':
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
                             if d.name == stream_name),
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
        >>> header = DataBroker[-1]
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
        >>> h = DataBroker[-1]  # most recent header
        >>> for name, doc in restream(h):
        ...     f(name, doc)

        Note
        ----
        This output can be used as a drop-in replacement for the output of the
        bluesky Run Engine.

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
        >>> h = DataBroker[-1]  # most recent header
        >>> process(h, f)

        Note
        ----
        This output can be used as a drop-in replacement for the output of the
        bluesky Run Engine.

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

        Parameters:
        -----------
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
                    event = event.to_name_dict_pair()[1]
                    # 'filled' is obtained from the descriptor, not stored
                    # in each event.
                    event.pop('filled', None)
                    db.mds.insert_event(**_sanitize(event))
            db.mds.insert_run_stop(**_sanitize(header['stop']))
            # insert assets
            res_uids = self.get_resource_uids(header)
            for uid in res_uids:
                fps = self.reg.copy_files(uid, new_root=new_root,
                                          **copy_kwargs)
                file_pairs.extend(fps)
                res = self.reg.resource_given_uid(uid)
                new_res = db.reg.insert_resource(res['spec'],
                                                 res['resource_path'],
                                                 res['resource_kwargs'],
                                                 root=new_root)
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

        Parameters:
        -----------
        headers : :class:databroker.Header:
            one or more headers that are going to be exported

        Returns
        ------
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
            contain the descriptor associated with each every event
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
            for k, v in six.iteritems(fill_map):
                for dk in v:
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

    """
    def __init__(self, mds, reg=None, plugins=None, filters=None,
                 auto_register=True):
        if plugins is not None:
            raise ValueError("The 'plugins' argument is no longer supported. "
                             "Use an EventSource instead.")
        if filters is None:
            filters = []
        if filters:
            warnings.warn("Future versions of the databroker will not accept "
                          "'filters' in __init__. Set them using the filters "
                          "attribute after initialization.", stacklevel=2)
        super(Broker, self).__init__(HeaderSourceShim(mds),
                                     [EventSourceShim(mds, reg)],
                                     {'': reg})
        self.filters = filters
        if auto_register:
            register_builtin_handlers(self.reg)

    @classmethod
    def from_config(cls, config, auto_register=True):
        """
        Create a new Broker instance using a dictionary of configuration.

        Parameters
        ----------
        config : dict
        auto_register : boolean, optional
            By default, automatically register built-in asset handlers (classes
            that handle I/O for externally stored data). Set this to ``False``
            to do all registration manually.

        Returns
        -------
        db : Broker

        Examples
        --------
        Create a Broker backed by sqlite databases. (This is configuration is
        not recommended for large or important deployments. See the
        configuration documentation for more.)

        >>> config = {
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
        mds = load_component(config['metadatastore'])
        assets = load_component(config['assets'])
        return cls(mds, assets, auto_register=auto_register)

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
        db = cls.from_config(lookup_config(name), auto_register=auto_register)
        return db


def _sanitize(doc):
    # Make this a plain dict and strip off doct.Document artifacts.
    d = dict(doc)
    d.pop('_name', None)
    return d
