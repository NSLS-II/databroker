import builtins
import collections
import collections.abc
import copy
from datetime import datetime, timedelta
import functools
import inspect
import itertools
import logging
import os
import sys
import threading

from bson.objectid import ObjectId, InvalidId
import cachetools
import entrypoints
import event_model
from dask.array.core import cached_cumsum, normalize_chunks
from jsonpatch import apply_patch as apply_json_patch
from json_merge_patch import merge as apply_merge_patch
import numpy
import pymongo
import pymongo.errors
import toolz.itertoolz
import xarray
from event_model import DocumentNames, schema_validators
from tiled.access_policies import ALL_ACCESS, ALL_SCOPES, NO_ACCESS, SpecialUsers
from tiled.adapters.array import ArrayAdapter
from tiled.adapters.xarray import DatasetAdapter
from tiled.structures.array import (
    ArrayStructure,
    BuiltinDtype,
    Kind,
    StructDtype,
)
from tiled.adapters.mapping import MapAdapter
from tiled.iterviews import KeysView, ItemsView, ValuesView
from tiled.query_registration import QueryTranslationRegistry
from tiled.queries import Contains, Comparison, Eq, FullText, In, NotEq, NotIn, Regex
from tiled.adapters.utils import (
    tree_repr,
    IndexersMixin,
)
from tiled.structures.core import Spec, StructureFamily
from tiled.utils import import_object, OneShotCachedMap, UNCHANGED

from .common import BlueskyEventStreamMixin, BlueskyRunMixin, CatalogOfBlueskyRunsMixin
from .queries import (
    BlueskyMapAdapter,
    _PartialUID,
    _ScanID,
    ScanIDRange,
    TimeRange,
    contains,
    comparison,
    eq,
    _in,
    not_eq,
    not_in,
    partial_uid,
    scan_id,
    scan_id_range,
    time_range,
    regex,
)
from .server import router


CHUNK_SIZE_LIMIT = os.getenv("DATABROKER_CHUNK_SIZE_LIMIT", "100MB")
MAX_AD_FRAMES_PER_CHUNK = int(os.getenv("DATABROKER_MAX_AD_FRAMES_PER_CHUNK", "10"))

logger = logging.getLogger(__name__)


def _try_descr(field_metadata):
    descr = field_metadata.get("dtype_descr")
    if descr:
        if len(descr) == 1 and descr[0][0] == "":
            return None
        numpy_dtype = numpy.dtype([tuple(field) for field in descr])
        dtype = StructDtype.from_numpy_dtype(numpy_dtype)
        if dtype.max_depth() > 1:
            raise RuntimeError(
                "We can not yet cope with multiple nested structured dtypes.  "
                f"{descr}"
            )
        return dtype
    else:
        return None


def structure_from_descriptor(descriptor, sub_dict, max_seq_num, unicode_columns=None):
    # Build the time coordinate.
    time_shape = (max_seq_num - 1,)
    time_chunks = normalize_chunks(
        ("auto",) * len(time_shape),
        shape=time_shape,
        limit=CHUNK_SIZE_LIMIT,
        dtype=FLOAT_DTYPE.to_numpy_dtype(),
    )
    time_variable = ArrayStructure(
        shape=time_shape,
        chunks=time_chunks,
        dims=["time"],
        data_type=FLOAT_DTYPE,
    )
    if unicode_columns is None:
        unicode_columns = {}
    dim_counter = itertools.count()
    structures = {"time": time_variable}
    metadata = {"time": {"attrs": {}}}

    for key, field_metadata in descriptor["data_keys"].items():
        # if the EventDescriptor doesn't provide names for the
        # dimensions (it's optional) use the same default dimension
        # names that xarray would.
        ndim = len(field_metadata["shape"])
        if "dims" in field_metadata and len(field_metadata["dims"]) == ndim:
            dims = ["time"] + field_metadata["dims"]
        else:
            dims = ["time"] + [f"dim_{next(dim_counter)}" for _ in range(ndim)]
        attrs = {}
        # Record which object (i.e. device) this column is associated with,
        # which enables one to find the relevant configuration, if any.
        for object_name, keys_ in descriptor.get("object_keys", {}).items():
            for item in keys_:
                if item == key:
                    attrs["object"] = object_name
                    break
        units = field_metadata.get("units")
        if units:
            if isinstance(units, str):
                attrs["units_string"] = units
            # TODO We may soon add a more structured units type, which
            # would likely be a dict here.
        if sub_dict == "data":
            shape = tuple((max_seq_num - 1, *field_metadata["shape"]))
            # if we have a descr, then this is a
            dtype = _try_descr(field_metadata)
            dt_np = field_metadata.get("dtype_numpy") or field_metadata.get("dtype_str")
            if dtype is not None:
                if len(shape) > 2:
                    raise RuntimeError(
                        "We do not yet support general structured arrays, only 1D ones."
                    )
            # if we have a detailed string, trust that
            elif dt_np is not None:
                dtype = BuiltinDtype.from_numpy_dtype(numpy.dtype(dt_np))
            # otherwise guess!
            else:
                dtype = JSON_DTYPE_TO_MACHINE_DATA_TYPE[field_metadata["dtype"]]
                if dtype.kind == Kind.unicode:
                    array = unicode_columns[key]
                    dtype = BuiltinDtype.from_numpy_dtype(
                        numpy.dtype(f"<U{array.itemsize // 4}")
                    )
        else:
            # assert sub_dict == "timestamps"
            shape = tuple((max_seq_num - 1,))
            dtype = FLOAT_DTYPE

        numpy_dtype = dtype.to_numpy_dtype()
        if "chunks" in field_metadata:
            # If the Event Descriptor tells us a preferred chunking, use that.
            suggested_chunks = [tuple(chunk) if isinstance(chunk, list)
                                else chunk for chunk in field_metadata['chunks']]
        elif (0 in shape) or (numpy_dtype.itemsize == 0):
            # special case to avoid warning from dask
            suggested_chunks = shape
        elif len(shape) == 4:
            # TEMP: Special-case 4D data in a way that optimzes single-frame
            # access of area detector data.
            # If we choose 1 that would make single-frame access fast
            # but many-frame access too slow.
            suggested_chunks = (
                min(MAX_AD_FRAMES_PER_CHUNK, shape[0]),
                min(MAX_AD_FRAMES_PER_CHUNK, shape[1]),
                "auto",
                "auto",
            )
        else:
            suggested_chunks = ("auto",) * len(shape)

        try:
            chunks = normalize_chunks(
                suggested_chunks,
                shape=shape,
                limit=CHUNK_SIZE_LIMIT,
                dtype=numpy_dtype,
            )
        except Exception as err:
            raise ValueError(
                "Failed to normalize chunks with suggested_chunks. Params: "
                f"suggested_chunks={suggested_chunks} "
                f"shape={shape} "
                f"limit={CHUNK_SIZE_LIMIT} "
                f"dtype={numpy_dtype}"
            ) from err

        structures[key] = ArrayStructure(
            shape=shape, chunks=chunks, dims=dims, data_type=dtype
        )
        metadata[key] = {"attrs": attrs}

    return structures, metadata


class DatasetMapAdapter(MapAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def inlined_contents_enabled(self, depth):
        # Tell the server to in-line the description of each array
        # (i.e. data_vars and coords) to avoid latency of a second
        # request.
        return True


BLUESKYRUN_SPEC = Spec("BlueskyRun", version="1")


class BlueskyRun(MapAdapter, BlueskyRunMixin):
    def __init__(
        self,
        *args,
        serializer,
        clear_from_cache,
        handler_registry,
        transforms,
        root_map,
        datum_collection,
        resource_collection,
        specs=None,
        **kwargs,
    ):
        if specs is None:
            specs = []
        specs = list(specs)
        specs.append(BLUESKYRUN_SPEC)
        super().__init__(*args, specs=specs, **kwargs)
        self.transforms = transforms or {}
        self.root_map = root_map
        self._datum_collection = datum_collection
        self._resource_collection = resource_collection
        # This is used to create the Filler on first access.
        self._init_handler_registry = handler_registry
        self._filler = None
        self._serializer = serializer
        self._clear_from_cache = clear_from_cache
        self._filler_creation_lock = threading.RLock()

    def must_revalidate(self):
        return self._metadata["stop"] is not None

    @property
    def metadata_stale_at(self):
        return datetime.utcnow() + timedelta(hours=1)

    @property
    def entries_stale_at(self):
        if self._metadata["stop"] is not None:
            return datetime.utcnow() + timedelta(hours=1)

    @property
    def key(self):
        return self._metadata["start"]["uid"]

    def metadata(self):
        "Metadata about this MongoAdapter."
        # If there are transforms configured, shadow the 'start' and 'stop' documents
        # with transfomed copies.
        transformed = {}
        if "start" in self.transforms:
            transformed["start"] = self.transforms["start"](self._metadata["start"])
        if "stop" in self.transforms:
            transformed["stop"] = self.transforms["stop"](self._metadata["stop"])
        metadata = dict(collections.ChainMap(transformed, self._metadata))
        return metadata

    async def replace_metadata(self, metadata=None, specs=None):
        if "start" not in metadata:
            raise NotImplementedError(
                "A start document is required when updating metadata."
            )
        elif specs is None:
            raise NotImplementedError("Updating of specs is not yet supported.")
        # Security : Key and relationship checks
        if self.key != metadata["start"]["uid"]:
            raise ValueError("The UID in the metadata must match the request's UID.")
        start = metadata["start"]
        stop = metadata["stop"] if "stop" in metadata else None
        schema_validators[DocumentNames.start].validate(start)
        if stop is not None:
            schema_validators[DocumentNames.stop].validate(stop)
        self._serializer.update("start", metadata["start"])
        if stop is not None:
            self._serializer.update("stop", metadata["stop"])
        self._clear_from_cache()

    async def patch_metadata(self, patch=None, specs=None):
        if patch is None:
            patch = []
        metadata = apply_json_patch(dict(self.metadata()), patch)
        await self.replace_metadata(metadata=metadata, specs=specs)

    async def merge_metadata(self, patch=None, specs=None):
        if patch is None:
            patch = {}
        metadata = apply_merge_patch(dict(self.metadata()), patch)
        await self.replace_metadata(metadata=metadata, specs=specs)

    @property
    def filler(self):
        # Often multiple requests prompt this to be created in parallel.
        # We need this to be threadsafe.
        with self._filler_creation_lock:
            if self._filler is None:
                filler = event_model.Filler(
                    handler_registry=self._init_handler_registry,
                    root_map=self.root_map,
                    inplace=False,
                )
                for descriptor in itertools.chain(
                    *(stream.metadata()["descriptors"] for stream in self.values())
                ):
                    filler("descriptor", descriptor)
                self._filler = filler
            return self._filler

    @property
    def register_handler(self):
        return self.filler.register_handler

    @property
    def deregister_handler(self):
        return self.filler.deregister_handler

    @property
    def handler_registry(self):
        return self.filler.handler_registry

    def new_variation(self, *args, **kwargs):
        return super().new_variation(
            *args,
            serializer=self._serializer,
            clear_from_cache=self._clear_from_cache,
            handler_registry=self.handler_registry,
            transforms=self.transforms,
            root_map=self.root_map,
            datum_collection=self._datum_collection,
            resource_collection=self._resource_collection,
            **kwargs,
        )

    def get_datum_for_resource(self, resource_uid):
        return self._datum_collection.find({"resource": resource_uid}, {"_id": False})

    def get_resource(self, uid):
        doc = self._resource_collection.find_one({"uid": uid}, {"_id": False})

        # Some old resource documents don't have a 'uid' and they are
        # referenced by '_id'.
        if doc is None:
            try:
                _id = ObjectId(uid)
            except InvalidId:
                pass
            else:
                doc = self._resource_collection.find_one({"_id": _id}, {"_id": False})
                doc["uid"] = uid

        if doc is None:
            raise ValueError(f"Could not find Resource with uid={uid}")
        if "resource" in self.transforms:
            transformed_doc = self.transforms["resource"](doc)
        else:
            transformed_doc = doc
        return transformed_doc

    def lookup_resource_for_datum(self, datum_id):
        doc = self._datum_collection.find_one({"datum_id": datum_id})
        if doc is None:
            raise ValueError(f"Could not find Datum with datum_id={datum_id}")
        return doc["resource"]

    def single_documents(self, fill):
        if fill:
            raise NotImplementedError("Only fill=False is implemented.")
        external_fields = {}  # map descriptor uid to set of external fields
        datum_cache = {}  # map datum_id to datum document
        # Track which Resource and Datum documents we have yielded so far.
        resource_uids = set()
        datum_ids = set()
        # Interleave the documents from the streams in time order.
        merged_iter = toolz.itertoolz.merge_sorted(
            *(stream.iter_descriptors_and_events() for stream in self.values()),
            key=lambda item: item[1]["time"],
        )
        yield ("start", self.metadata()["start"])
        for name, doc in merged_iter:
            # Insert Datum, Resource as needed, and then yield (name, doc).
            if name == "event":
                for field in external_fields[doc["descriptor"]]:
                    datum_id = doc["data"][field]
                    if datum_ids not in datum_ids:
                        # We haven't yielded this Datum yet. Look it up, and yield it.
                        try:
                            # Check to see if it's been pre-fetched.
                            datum = datum_cache.pop(datum_id)
                        except KeyError:
                            resource_uid = self.lookup_resource_for_datum(datum_id)
                            if resource_uid not in resource_uids:
                                # We haven't yielded this Resource yet. Look it up, and yield it.
                                resource = self.get_resource(resource_uid)
                                resource_uids.add(resource_uid)
                                yield ("resource", resource)
                                # Pre-fetch *all* the Datum documents for this resource in one query.
                                datum_cache.update(
                                    {
                                        doc["datum_id"]: doc
                                        for doc in self.get_datum_for_resource(
                                            resource_uid
                                        )
                                    }
                                )
                                # Now get the Datum we originally were looking for.
                                datum = datum_cache.pop(datum_id)
                            datum_ids.add(datum_id)
                        yield ("datum", datum)
            elif name == "descriptor":
                # Track which fields ("data keys") hold references to external data.
                external_fields[doc["uid"]] = {
                    key
                    for key, value in doc["data_keys"].items()
                    if value.get("external")
                }
            yield name, doc
        stop_doc = self.metadata()["stop"]
        if stop_doc is not None:
            yield ("stop", stop_doc)

    def documents(self, fill, size=25):
        """
        Yield ``(name, document)`` items from the run.

        Batch Event and Datum documents into pages of up to ``size`` rows,
        while preserving time-ordering.
        """
        yield from batch_documents(self.single_documents(fill=fill), size)


class BlueskyEventStream(MapAdapter, BlueskyEventStreamMixin):
    def __init__(
        self,
        *args,
        serializer,
        clear_from_cache,
        event_collection,
        cutoff_seq_num,
        run,
        specs=None,
        **kwargs,
    ):
        if specs is None:
            specs = []
        specs = list(specs)
        specs.append(Spec("BlueskyEventStream", version="1"))
        super().__init__(*args, specs=specs, **kwargs)
        self._serializer = serializer
        self._clear_from_cache = clear_from_cache
        self._event_collection = event_collection
        self._cutoff_seq_num = cutoff_seq_num
        self._run = run

    @property
    def must_revalidate(self):
        # The keys in this node are *always* stable.
        return False

    @property
    def metadata_stale_at(self):
        if self._run.metadata()["stop"] is not None:
            return datetime.utcnow() + timedelta(hours=1)
        return datetime.utcnow() + timedelta(hours=1)

    @property
    def entries_stale_at(self):
        if self._run.metadata()["stop"] is not None:
            return datetime.utcnow() + timedelta(hours=1)

    def metadata(self):
        # If there are transforms configured, shadow the 'descriptor' documents
        # with transfomed copies.
        transformed = {}
        transforms = self._run.transforms
        if "descriptor" in transforms:
            transformed["descriptors"] = [
                transforms["descriptor"](d) for d in self._metadata["descriptors"]
            ]
        metadata = dict(collections.ChainMap(transformed, self._metadata))
        return metadata

    @property
    def key(self):
        return self._metadata["descriptors"][0]["name"]

    async def replace_metadata(self, metadata=None, specs=None):
        if "descriptors" not in metadata:
            raise NotImplementedError("Update_metadata method requires descriptors.")
        # Update descriptors
        for descriptor in metadata["descriptors"]:
            schema_validators[DocumentNames.descriptor].validate(descriptor)
            self._serializer.update("descriptor", descriptor)
        self._clear_from_cache()

    def new_variation(self, **kwargs):
        return super().new_variation(
            serializer=self._serializer,
            clear_from_cache=self._clear_from_cache,
            event_collection=self._event_collection,
            cutoff_seq_num=self._cutoff_seq_num,
            run=self._run,
            **kwargs,
        )

    def iter_descriptors_and_events(self):
        for descriptor in sorted(
            self.metadata()["descriptors"], key=lambda d: d["time"]
        ):
            yield ("descriptor", descriptor)
            # TODO Grab paginated chunks.
            events = list(
                self._event_collection.find(
                    {
                        "descriptor": descriptor["uid"],
                        "seq_num": {"$lte": self._cutoff_seq_num},
                    },
                    {"_id": False},
                    sort=[("time", pymongo.ASCENDING)],
                )
            )
            for event in events:
                yield ("event", event)


class ArrayFromDocuments:
    """
    Represents one column
    """

    structure_family = "array"

    def __init__(self, dataset_adapter, field, specs=None):
        self._dataset_adapter = dataset_adapter
        self._field = field
        self._metadata = dataset_adapter.array_metadata[field]
        if specs is None:
            specs = []
        self.specs = specs

    def metadata(self):
        return self._metadata

    def read_block(self, block, slice=None):
        return self._dataset_adapter.read_block(self._field, block, slice=slice)

    def read(self, slice=None):
        array_adapter = self._dataset_adapter.read(fields=[self._field])[self._field]
        return array_adapter.read(slice)

    def structure(self):
        return self._dataset_adapter.array_structures[self._field]


class DatasetFromDocuments:
    """
    An xarray.Dataset from a sub-dict of an Event stream
    """

    structure_family = StructureFamily.container
    specs = [Spec("xarray_dataset")]

    def __init__(
        self,
        *,
        run,
        stream_name,
        cutoff_seq_num,
        event_descriptors,
        event_collection,
        root_map,
        sub_dict,
        validate_shape,
    ):
        self._run = run
        self._stream_name = stream_name
        self._cutoff_seq_num = cutoff_seq_num
        self._event_descriptors = event_descriptors
        self._event_collection = event_collection
        self._sub_dict = sub_dict
        self.root_map = root_map
        self.validate_shape = validate_shape

        # metadata should look like
        # {
        #     "stream_name": "...",
        #     "descriptors": [...],
        #     "attrs": {...},
        # }
        # We intentionally do not put the descriptors in attrs (ruins UI)
        # but we put the stream_name there.
        self._metadata = (
            self._run[self._stream_name].metadata().copy()
        )  # {"descriptors": [...], "stream_name: "..."}
        # Put the stream_name in attrs so it shows up in the xarray repr.
        self._metadata["attrs"] = {"stream_name": self.metadata()["stream_name"]}

        # The `data_keys` in a series of Event Descriptor documents with the same
        # `name` MUST be alike, so we can choose one arbitrarily.
        # IMPORTANT: Access via self.metadata so that the transforms are applied.
        descriptor, *_ = self._metadata["descriptors"]
        unicode_columns = {}
        if self._sub_dict == "data":
            # Collect the keys (column names) that are of unicode data type.
            unicode_keys = []
            for key, field_metadata in descriptor["data_keys"].items():
                if field_metadata["dtype"] == "string":
                    # Skip this if it has a dtype_numpy with an itemsize.
                    dt_np = field_metadata.get("dtype_numpy") or field_metadata.get("dtype_str")
                    if dt_np is not None:
                        if numpy.dtype(dt_np).itemsize != 0:
                            continue
                    unicode_keys.append(key)
            # Load the all the data for unicode columns to figure out the itemsize.
            # We have no other choice, except to *guess* but we'd be in
            # trouble if our guess were too small, and we'll waste space
            # if our guess is too large.
            if unicode_keys:
                unicode_columns.update(self.get_columns(unicode_keys, slices=None))

        self.array_structures, self.array_metadata = structure_from_descriptor(
            descriptor, self._sub_dict, self._cutoff_seq_num, unicode_columns
        )
        self._contents = MapAdapter(
            OneShotCachedMap(
                {
                    field: functools.partial(
                        ArrayFromDocuments,
                        self,
                        field,
                        specs=[Spec("xarray_coord")]
                        if field == "time"
                        else [Spec("xarray_data_var")],
                    )
                    for field in self.array_structures
                }
            )
        )

    def metadata(self):
        return self._metadata

    def structure(self):
        return None

    def keys(self):
        return self._contents.keys()

    def values(self):
        return self._contents.values()

    def items(self):
        return self._contents.items()

    def __len__(self):
        return len(self._contents)

    def __repr__(self):
        return f"<{type(self).__name__}>"

    @property
    def metadata_stale_at(self):
        if self._run.metadata()["stop"] is not None:
            return datetime.utcnow() + timedelta(hours=1)
        return datetime.utcnow() + timedelta(hours=1)

    @property
    def content_stale_at(self):
        if self._run.metadata()["stop"] is not None:
            return datetime.utcnow() + timedelta(hours=1)

    def inlined_contents_enabled(self, depth):
        # Tell the server to in-line the description of each array
        # (i.e. data_vars and coords) to avoid latency of a second
        # request.
        return True

    def read(self, fields=None):
        # The client may be requesting multiple fields.
        # Make batched requests to MongoDB to avoid making
        # one request per field.
        if fields is None:
            keys_to_fetch = list(self.array_structures)
        else:
            keys_to_fetch = list(fields)
        columns = {}
        # Since "time" come from a different place in a Event schema,
        # it is handled specially.
        if "time" in keys_to_fetch:
            keys_to_fetch.remove("time")
            columns["time"] = self._get_time_coord(slice_params=None)
        if keys_to_fetch:
            columns.update(self.get_columns(keys_to_fetch, slices=None))
        mapping = {}
        for key, structure in self.array_structures.items():
            if (fields is not None) and (key not in fields):
                continue
            dtype = structure.data_type.to_numpy_dtype()
            raw_array = columns[key]
            if raw_array.dtype != dtype:
                logger.warning(
                    f"{key!r} actually has dtype {raw_array.dtype.str!r} "
                    f"but was reported as having dtype {dtype.str!r}. "
                    "It will be converted to the reported type, "
                    "but this should be fixed by setting 'dtype_numpy' "
                    "in the data_key of the EventDescriptor. "
                    f"RunStart UID: {self._run.metadata()['start']['uid']!r}"
                )
                array = raw_array.astype(dtype)
            else:
                array = raw_array
            specs = (
                [Spec("xarray_coord")] if key == "time" else [Spec("xarray_data_var")]
            )
            mapping[key] = ArrayAdapter(
                array,
                metadata=self.array_metadata[key],
                structure=structure,
                specs=specs,
            )
        return DatasetMapAdapter(mapping, metadata=self.metadata(), specs=self.specs)

    def __getitem__(self, key):
        return self._contents[key]

    def read_block(self, variable, block, slice=None):
        structure = self.array_structures[variable]
        if variable == "time":
            chunks = structure.chunks
            cumdims = [cached_cumsum(bds, initial_zero=True) for bds in chunks]
            slices_for_chunks = [
                [builtins.slice(s, s + dim) for s, dim in zip(starts, shapes)]
                for starts, shapes in zip(cumdims, chunks)
            ]
            (slice_,) = [s[index] for s, index in zip(slices_for_chunks, block)]
            return self._get_time_coord(slice_params=(slice_.start, slice_.stop))
        dtype = structure.data_type.to_numpy_dtype()
        chunks = structure.chunks
        cumdims = [cached_cumsum(bds, initial_zero=True) for bds in chunks]
        slices_for_chunks = [
            [builtins.slice(s, s + dim) for s, dim in zip(starts, shapes)]
            for starts, shapes in zip(cumdims, chunks)
        ]
        slices = [s[index] for s, index in zip(slices_for_chunks, block)]
        raw_array = self.get_columns([variable], slices=slices)[variable]
        if raw_array.dtype != dtype:
            logger.warning(
                f"{variable!r} actually has dtype {raw_array.dtype.str!r} "
                f"but was reported as having dtype {dtype.str!r}. "
                "It will be converted to the reported type, "
                "but this should be fixed by setting 'dtype_numpy' "
                "in the data_key of the EventDescriptor. "
                f"RunStart UID: {self._run.metadata()['start']['uid']!r}"
            )
            array = raw_array.astype(dtype)
        else:
            array = raw_array
        if slice is not None:
            array = array[slice]
        return array

    def _get_time_coord(self, slice_params):
        if slice_params is None:
            min_seq_num = 1
            max_seq_num = self._cutoff_seq_num
        else:
            min_seq_num = 1 + slice_params[0]
            max_seq_num = 1 + slice_params[1]
        column = []
        descriptor_uids = [doc["uid"] for doc in self.metadata()["descriptors"]]

        def populate_column(min_seq_num, max_seq_num):
            cursor = self._event_collection.aggregate(
                [
                    # Select Events for this Descriptor with the appropriate seq_num range.
                    {
                        "$match": {
                            "descriptor": {"$in": descriptor_uids},
                            # It's important to use a half-open interval here
                            # so that the boundaries work.
                            "seq_num": {"$gte": min_seq_num, "$lt": max_seq_num},
                        },
                    },
                    # Include only the fields of interest.
                    {
                        "$project": {"descriptor": 1, "seq_num": 1, "time": 1},
                    },
                    # Sort by time.
                    {"$sort": {"time": 1}},
                    # If seq_num is repeated, take the latest one.
                    {
                        "$group": {
                            "_id": "$seq_num",
                            "doc": {"$last": "$$ROOT"},
                        },
                    },
                    # Re-sort, now by seq_num which *should* be equivalent to
                    # sorting by time but could not be in weird cases
                    # (which I'm not aware have ever occurred) where an NTP sync
                    # moves system time backward mid-run.
                    {"$sort": {"doc.seq_num": 1}},
                    # Extract the column of interest as an array.
                    {
                        "$group": {
                            "_id": "$descriptor",
                            "column": {"$push": "$doc.time"},
                        },
                    },
                ]
            )
            (result,) = cursor
            column.extend(result["column"])

        # Aim for 8 MB pages to stay safely clear the MongoDB's hard limit
        # of 16 MB.
        TARGET_PAGE_BYTESIZE = 8_000_000

        page_size = TARGET_PAGE_BYTESIZE // 8  # estimated row byte size is 8
        boundaries = list(range(min_seq_num, 1 + max_seq_num, page_size))
        if boundaries[-1] != max_seq_num:
            boundaries.append(max_seq_num)
        for min_, max_ in zip(boundaries[:-1], boundaries[1:]):
            populate_column(min_, max_)

        return numpy.array(column)

    def get_columns(self, keys, slices):
        if slices is None:
            min_seq_num = 1
            max_seq_num = self._cutoff_seq_num
        else:
            slice_ = slices[0]
            min_seq_num = 1 + slice_.start
            max_seq_num = 1 + slice_.stop

        to_stack = self._inner_get_columns(tuple(keys), min_seq_num, max_seq_num)

        result = {}
        for key, value in to_stack.items():
            array = numpy.stack(value)
            if slices:
                sliced_array = array[(..., *slices[1:])]
            else:
                sliced_array = array
            result[key] = sliced_array

        return result

    def _inner_get_columns(self, keys, min_seq_num, max_seq_num):
        columns = {key: [] for key in keys}
        # IMPORTANT: Access via self.metadata so that transforms are applied.
        descriptors = self.metadata()["descriptors"]
        descriptor_uids = [doc["uid"] for doc in descriptors]
        # The `data_keys` in a series of Event Descriptor documents with the
        # same `name` MUST be alike, so we can just use the first one.
        data_keys = [descriptors[0]["data_keys"][key] for key in keys]
        is_externals = ["external" in data_key for data_key in data_keys]
        expected_shapes = [tuple(data_key["shape"] or []) for data_key in data_keys]

        def populate_columns(keys, min_seq_num, max_seq_num):
            # This closes over the local variable columns and appends to its
            # contents.
            cursor = self._event_collection.aggregate(
                [
                    # Select Events for this Descriptor with the appropriate seq_num range.
                    {
                        "$match": {
                            "descriptor": {"$in": descriptor_uids},
                            # It's important to use a half-open interval here
                            # so that the boundaries work.
                            "seq_num": {"$gte": min_seq_num, "$lt": max_seq_num},
                        },
                    },
                    # Include only the fields of interest.
                    {
                        "$project": {
                            "descriptor": 1,
                            "seq_num": 1,
                            **{f"{self._sub_dict}.{key}": 1 for key in keys},
                        },
                    },
                    # Sort by time.
                    {"$sort": {"time": 1}},
                    # If seq_num is repeated, take the latest one.
                    {
                        "$group": {
                            "_id": "$seq_num",
                            "doc": {"$last": "$$ROOT"},
                        },
                    },
                    # Re-sort, now by seq_num which *should* be equivalent to
                    # sorting by time but could not be in weird cases
                    # (which I'm not aware have ever occurred) where an NTP sync
                    # moves system time backward mid-run.
                    {"$sort": {"doc.seq_num": 1}},
                    # Extract the column of interest as an array.
                    {
                        "$group": {
                            "_id": "$descriptor",
                            **{
                                key: {"$push": f"$doc.{self._sub_dict}.{key}"}
                                for key in keys
                            },
                        },
                    },
                ]
            )
            (result,) = cursor
            for key, expected_shape, is_external in zip(
                keys, expected_shapes, is_externals
            ):
                if expected_shape and (not is_external):
                    validated_column = list(
                        map(
                            lambda item: self.validate_shape(
                                key, numpy.asarray(item), expected_shape
                            ) if 'uid' in inspect.signature(self.validate_shape).parameters
                            else self.validate_shape(
                                key, numpy.asarray(item), expected_shape, uid=self._run.metadata()['start']['uid']
                            ),
                            result[key],
                        )
                    )
                else:
                    validated_column = result[key]
                columns[key].extend(validated_column)

        scalars = []
        nonscalars = []
        estimated_nonscalar_row_bytesizes = []
        estimated_scalar_row_bytesize = 0
        for key, data_key, is_external in zip(keys, data_keys, is_externals):
            if (not data_key["shape"]) or is_external:
                # This is either a literal scalar value of a datum_id.
                scalars.append(key)
                if data_key["dtype"] == "string":
                    # Give a generous amount of headroom here.
                    estimated_scalar_row_bytesize += 10_000  # 10 kB
                else:
                    # 64-bit integer or float
                    estimated_scalar_row_bytesize += 8
            else:
                nonscalars.append(key)
                estimated_nonscalar_row_bytesizes.append(
                    numpy.prod(data_key["shape"]) * 8
                )

        # Aim for 8 MB pages to stay safely clear the MongoDB's hard limit
        # of 16 MB.
        TARGET_PAGE_BYTESIZE = 8_000_000

        # Fetch scalars all together.
        if scalars:
            page_size = TARGET_PAGE_BYTESIZE // estimated_scalar_row_bytesize
            boundaries = list(range(min_seq_num, 1 + max_seq_num, page_size))
            if boundaries[-1] != max_seq_num:
                boundaries.append(max_seq_num)
            for min_, max_ in zip(boundaries[:-1], boundaries[1:]):
                populate_columns(tuple(scalars), min_, max_)

        # Fetch each nonscalar column individually.
        # TODO We could batch a couple nonscalar columns at at ime based on
        # their size if we need to squeeze more performance out here. But maybe
        # we can get away with never adding that complexity.
        for key, est_row_bytesize in zip(nonscalars, estimated_nonscalar_row_bytesizes):
            page_size = max(1, TARGET_PAGE_BYTESIZE // est_row_bytesize)
            boundaries = list(range(min_seq_num, 1 + max_seq_num, page_size))
            if boundaries[-1] != max_seq_num:
                boundaries.append(max_seq_num)
            for min_, max_ in zip(boundaries[:-1], boundaries[1:]):
                populate_columns((key,), min_, max_)

        # If data is external, we now have a column of datum_ids, and we need
        # to look up the data that they reference.
        to_stack = collections.defaultdict(list)
        # Any arbitrary valid descriptor uid will work; we just need to satisfy
        # the Filler with our mocked Event below. So we pick the first one.
        descriptor_uid = descriptor_uids[0]
        for key, expected_shape, is_external in zip(
            keys, expected_shapes, is_externals
        ):
            column = columns[key]
            if is_external:
                filled_column = []
                for datum_id in column:
                    # HACK to adapt Filler which is designed to consume whole,
                    # streamed documents, to this column-based access mode.
                    mock_event = {
                        "data": {key: datum_id},
                        "descriptor": descriptor_uid,
                        "uid": "PLACEHOLDER",
                        "filled": {key: False},
                    }
                    filled_mock_event = _fill(
                        self._run.filler,
                        mock_event,
                        self._run.lookup_resource_for_datum,
                        self._run.get_resource,
                        self._run.get_datum_for_resource,
                        last_datum_id=None,
                    )
                    filled_data = filled_mock_event["data"][key]
                    validated_filled_data = self.validate_shape(
                        key, filled_data, expected_shape
                    )
                    filled_column.append(validated_filled_data)
                to_stack[key].extend(filled_column)
            else:
                to_stack[key].extend(column)

        return to_stack


class Config(MapAdapter):
    """
    MongoAdapter of configuration datasets, keyed on 'object' (e.g. device)
    """

    ...


def build_config_xarray(
    *,
    event_descriptors,
    sub_dict,
    object_name,
):
    first_descriptor, *_ = event_descriptors
    data_keys = first_descriptor["configuration"][object_name]["data_keys"]
    # All the data is stored in-line in the Event Descriptor documents.
    # The overwhelming majority of Runs have just one Event Descriptor,
    # and those that have more than one have only a couple, so we do
    # slow appending loop here knowing that we won't pay for too
    # many iterations except in vanishingly rare pathological cases.
    raw_columns = {key: [] for key in data_keys}
    for descriptor in event_descriptors:
        for key in data_keys:
            raw_columns[key].append(
                descriptor["configuration"][object_name][sub_dict][key]
            )
    # Enforce dtype.
    columns = {}
    for key, column in raw_columns.items():
        field_metadata = data_keys[key]
        dtype = _try_descr(field_metadata)
        dt_np = field_metadata.get("dtype_numpy") or field_metadata.get("dtype_str")
        if dtype is not None:
            if len(getattr(column[0], "shape", ())) > 2:
                raise RuntimeError(
                    "We do not yet support general structured arrays, only 1D ones."
                )
            numpy_dtype = dtype.to_numpy_dtype()
        # if we have a detailed string, trust that
        elif dt_np is not None:
            numpy_dtype = numpy.dtype(dt_np)
        # otherwise guess!
        else:
            numpy_dtype = JSON_DTYPE_TO_MACHINE_DATA_TYPE[
                field_metadata["dtype"]
            ].to_numpy_dtype()
        columns[key] = numpy.array(column, dtype=numpy_dtype)
    data_arrays = {}
    dim_counter = itertools.count()
    for key, field_metadata in data_keys.items():
        attrs = {}
        if sub_dict == "data":
            # if the EventDescriptor doesn't provide names for the
            # dimensions (it's optional) use the same default dimension
            # names that xarray would.
            ndim = len(field_metadata["shape"])
            if "dims" in field_metadata and len(field_metadata["dims"]) == ndim:
                dims = ["time"] + field_metadata["dims"]
            else:
                dims = ["time"] + [f"dim_{next(dim_counter)}" for _ in range(ndim)]
            units = field_metadata.get("units")
            if units:
                if isinstance(units, str):
                    attrs["units_string"] = units
                # TODO We may soon add a more structured units type, which
                # would likely be a dict here.
        else:
            dims = ["time"]
        data_array = xarray.DataArray(columns[key], dims=dims, attrs=attrs)
        data_arrays[key] = data_array
    ds = xarray.Dataset(data_arrays)
    return DatasetAdapter.from_dataset(ds)


class MongoAdapter(collections.abc.Mapping, CatalogOfBlueskyRunsMixin, IndexersMixin):
    structure_family = StructureFamily.container
    specs = [Spec("CatalogOfBlueskyRuns", version="1")]

    # Define classmethods for managing what queries this MongoAdapter knows.
    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register
    register_query_lazy = query_registry.register_lazy

    # Add a /documents route to the server.
    include_routers = [router]

    @classmethod
    def from_uri(
        cls,
        uri,
        *,
        asset_registry_uri=None,
        handler_registry=None,
        root_map=None,
        transforms=None,
        metadata=None,
        access_policy=None,
        cache_ttl_complete=60,  # seconds
        cache_ttl_partial=2,  # seconds
        validate_shape=None,
    ):
        """
        Create a MongoAdapter from MongoDB with the "normalized" (original) layout.

        Parameters
        ----------
        uri: str
            MongoDB URI with database name, formed like "mongodb://HOSTNAME:27017/DATABASE_NAME"
            or "mongodb://USER:PASSWORD@HOSTNAME:27017/DATABASE_NAME?authSource=admin".
        asset_registry_uri: str
            A separate URI for legacy deployments that place Resource and Datum documents
            in a different database from the rest of the documents. (This is believed to
            only be needed for old deployments at BNL NSLS-II.)
        handler_registry: dict, optional
            Maps each 'spec' (a string identifying a given type or external
            resource) to a handler class.

            A 'handler class' may be any callable with the signature::

                handler_class(resource_path, root, **resource_kwargs)

            It is expected to return an object, a 'handler instance', which is also
            callable and has the following signature::

                handler_instance(**datum_kwargs)

            As the names 'handler class' and 'handler instance' suggest, this is
            typically implemented using a class that implements ``__init__`` and
            ``__call__``, with the respective signatures. But in general it may be
            any callable-that-returns-a-callable.
        root_map: dict, optional
            str -> str mapping to account for temporarily moved/copied/remounted
            files.  Any resources which have a ``root`` in ``root_map`` will be
            loaded using the mapped ``root``.
        transforms: dict
            A dict that maps any subset of the keys {start, stop, resource, descriptor}
            to a function that accepts a document of the corresponding type and
            returns it, potentially modified. This feature is for patching up
            erroneous metadata. It is intended for quick, temporary fixes that
            may later be applied permanently to the data at rest
            (e.g., via a database migration).
        cache_ttl_partial : float
            Time (in seconds) to cache a *partial* (i.e. incomplete, ongoing)
            BlueskyRun before checking the database for updates. Default 2.
        cache_ttl_complete : float
            Time (in seconds) to cache a *complete* BlueskyRun before checking
            the database for updates. Default 60.
        validate_shape: func
            function that will be used to validate that the shape of the data matches
            the shape in the descriptor document
        """
        metadatastore_db = _get_database(uri)
        if asset_registry_uri is None:
            asset_registry_db = metadatastore_db
        else:
            asset_registry_db = _get_database(asset_registry_uri)
        root_map = root_map or {}
        transforms = parse_transforms(transforms)
        if handler_registry is None:
            handler_registry = discover_handlers()
        handler_registry = parse_handler_registry(handler_registry)
        # Two different caches with different eviction rules.
        cache_of_complete_bluesky_runs = cachetools.TTLCache(
            ttl=cache_ttl_complete, maxsize=100
        )
        cache_of_partial_bluesky_runs = cachetools.TTLCache(
            ttl=cache_ttl_partial, maxsize=100
        )
        return cls(
            metadatastore_db=metadatastore_db,
            asset_registry_db=asset_registry_db,
            handler_registry=handler_registry,
            root_map=root_map,
            transforms=transforms,
            cache_of_complete_bluesky_runs=cache_of_complete_bluesky_runs,
            cache_of_partial_bluesky_runs=cache_of_partial_bluesky_runs,
            metadata=metadata,
            access_policy=access_policy,
            validate_shape=validate_shape,
        )

    @classmethod
    def from_mongomock(
        cls,
        *,
        handler_registry=None,
        root_map=None,
        transforms=None,
        metadata=None,
        access_policy=None,
        cache_ttl_complete=60,  # seconds
        cache_ttl_partial=2,  # seconds
        validate_shape=None,
    ):
        """
        Create a transient MongoAdapter from backed by "mongomock".

        This is intended for testing, teaching, an demos. The data does not
        persistent. Do not use this for anything important.

        Parameters
        ----------
        handler_registry: dict, optional
            Maps each 'spec' (a string identifying a given type or external
            resource) to a handler class.

            A 'handler class' may be any callable with the signature::

                handler_class(resource_path, root, **resource_kwargs)

            It is expected to return an object, a 'handler instance', which is also
            callable and has the following signature::

                handler_instance(**datum_kwargs)

            As the names 'handler class' and 'handler instance' suggest, this is
            typically implemented using a class that implements ``__init__`` and
            ``__call__``, with the respective signatures. But in general it may be
            any callable-that-returns-a-callable.
        root_map: dict, optional
            str -> str mapping to account for temporarily moved/copied/remounted
            files.  Any resources which have a ``root`` in ``root_map`` will be
            loaded using the mapped ``root``.
        transforms: dict
            A dict that maps any subset of the keys {start, stop, resource, descriptor}
            to a function that accepts a document of the corresponding type and
            returns it, potentially modified. This feature is for patching up
            erroneous metadata. It is intended for quick, temporary fixes that
            may later be applied permanently to the data at rest
            (e.g., via a database migration).
        cache_ttl_partial : float
            Time (in seconds) to cache a *partial* (i.e. incomplete, ongoing)
            BlueskyRun before checking the database for updates. Default 2.
        cache_ttl_complete : float
            Time (in seconds) to cache a *complete* BlueskyRun before checking
            the database for updates. Default 60.
        validate_shape: func
            function that will be used to validate that the shape of the data matches
            the shape in the descriptor document
        """
        import mongomock

        client = mongomock.MongoClient()
        metadatastore_db = asset_registry_db = client["mock_database"]
        root_map = root_map or {}
        transforms = parse_transforms(transforms)
        if handler_registry is None:
            handler_registry = discover_handlers()
        handler_registry = parse_handler_registry(handler_registry)
        # Two different caches with different eviction rules.
        cache_of_complete_bluesky_runs = cachetools.TTLCache(
            ttl=cache_ttl_complete, maxsize=100
        )
        cache_of_partial_bluesky_runs = cachetools.TTLCache(
            ttl=cache_ttl_partial, maxsize=100
        )
        return cls(
            metadatastore_db=metadatastore_db,
            asset_registry_db=asset_registry_db,
            handler_registry=handler_registry,
            root_map=root_map,
            transforms=transforms,
            cache_of_complete_bluesky_runs=cache_of_complete_bluesky_runs,
            cache_of_partial_bluesky_runs=cache_of_partial_bluesky_runs,
            metadata=metadata,
            access_policy=access_policy,
            validate_shape=validate_shape,
        )

    def __init__(
        self,
        metadatastore_db,
        asset_registry_db,
        handler_registry,
        root_map,
        transforms,
        cache_of_complete_bluesky_runs,
        cache_of_partial_bluesky_runs,
        metadata=None,
        queries=None,
        sorting=None,
        access_policy=None,
        validate_shape=None,
    ):
        "This is not user-facing. Use MongoAdapter.from_uri."
        self._run_start_collection = metadatastore_db.get_collection("run_start")
        self._run_stop_collection = metadatastore_db.get_collection("run_stop")
        self._event_descriptor_collection = metadatastore_db.get_collection(
            "event_descriptor"
        )
        self._event_collection = metadatastore_db.get_collection("event")
        self._resource_collection = asset_registry_db.get_collection("resource")
        self._datum_collection = asset_registry_db.get_collection("datum")
        self._metadatastore_db = metadatastore_db
        self._asset_registry_db = asset_registry_db

        self._handler_registry = handler_registry
        self.handler_registry = event_model.HandlerRegistryView(self._handler_registry)
        self.root_map = root_map
        self.transforms = transforms or {}
        self._cache_of_complete_bluesky_runs = cache_of_complete_bluesky_runs
        self._cache_of_partial_bluesky_runs = cache_of_partial_bluesky_runs
        self._cache_of_bluesky_runs = collections.ChainMap(
            cache_of_complete_bluesky_runs, cache_of_partial_bluesky_runs
        )
        self._metadata = metadata or {}
        self.queries = list(queries or [])
        if sorting is None:
            sorting = [("time", 1)]
        self._sorting = sorting
        self.access_policy = access_policy
        self._serializer = None
        if validate_shape is None:
            validate_shape = default_validate_shape
        elif isinstance(validate_shape, str):
            validate_shape = import_object(validate_shape)
        self.validate_shape = validate_shape
        super().__init__()

    @property
    def database(self):
        """
        The underlying MongoDB database object

        Note: Very old deployments use two separate databases. We believe these
        are rare "in the wild", mostly or entirely restricted to the earliest
        facility to use databroker, BNL NSLS-II.

        In the event that two databases are used, this raises
        NotImplementedError and instructs the callers to use two semi-internal
        attributes instead.
        """
        if self._metadatastore_db != self._asset_registry_db:
            raise NotImplementedError(
                "This MongoAdapter is backed by two databases. This is no longer "
                "necessary or recommended. As a result, the `database` property "
                "is undefined. Use the attributes _metadatastore_db and "
                "_asset_registry_db directly."
            )
        return self._metadatastore_db

    def get_serializer(self):
        from suitcase.mongo_normalized import Serializer

        if self._serializer is None:
            self._serializer = Serializer(
                self._metadatastore_db, self._asset_registry_db
            )

        return self._serializer

    def register_handler(self, spec, handler, overwrite=False):
        if (not overwrite) and (spec in self._handler_registry):
            original = self._handler_registry[spec]
            if original is handler:
                return
            raise event_model.DuplicateHandler(
                f"There is already a handler registered for the spec {spec!r}. "
                f"Use overwrite=True to deregister the original.\n"
                f"Original: {original}\n"
                f"New: {handler}"
            )
        self._handler_registry[spec] = handler

    def deregister_handler(self, spec):
        self._handler_registry.pop(spec, None)

    def new_variation(
        self,
        *args,
        metadata=UNCHANGED,
        queries=UNCHANGED,
        sorting=UNCHANGED,
        **kwargs,
    ):
        if metadata is UNCHANGED:
            metadata = self._metadata
        if queries is UNCHANGED:
            queries = self.queries
        if sorting is UNCHANGED:
            sorting = self._sorting
        return type(self)(
            *args,
            metadatastore_db=self._metadatastore_db,
            asset_registry_db=self._asset_registry_db,
            handler_registry=self.handler_registry,
            root_map=self.root_map,
            transforms=self.transforms,
            cache_of_complete_bluesky_runs=self._cache_of_complete_bluesky_runs,
            cache_of_partial_bluesky_runs=self._cache_of_partial_bluesky_runs,
            queries=queries,
            sorting=sorting,
            access_policy=self.access_policy,
            validate_shape=self.validate_shape,
            **kwargs,
        )

    @property
    def metadata_stale_at(self):
        return datetime.utcnow() + timedelta(seconds=600)

    @property
    def entries_stale_at(self):
        return None

    def metadata(self):
        "Metadata about this MongoAdapter."
        return self._metadata

    def structure(self):
        return None

    @property
    def sorting(self):
        return list(self._sorting)

    def __repr__(self):
        # Display up to the first N keys to avoid making a giant service
        # request. Use _keys_slicer because it is unauthenticated.
        N = 10
        return tree_repr(self, self._keys_slice(0, N, direction=1))

    def _get_run(self, run_start_doc):
        "Get a BlueskyRun, either from a cache or by making one if needed."
        uid = run_start_doc["uid"]
        try:
            return self._cache_of_bluesky_runs[uid]
        except KeyError:
            run = self._build_run(run_start_doc)
            # Choose a cache depending on whethter the run is complete (in
            # which case updates are rare) or incomplete/partial (in which case
            # more data is likely incoming soon).
            if run.metadata().get("stop") is None:
                self._cache_of_partial_bluesky_runs[uid] = run
            else:
                self._cache_of_complete_bluesky_runs[uid] = run
            return run

    def _clear_from_cache(self, uid):
        self._cache_of_partial_bluesky_runs.pop(uid, None)
        self._cache_of_complete_bluesky_runs.pop(uid, None)

    def _build_run(self, run_start_doc):
        "This should not be called directly, even internally. Use _get_run."
        # Instantiate a BlueskyRun for this run_start_doc.
        uid = run_start_doc["uid"]
        # This may be None; that's fine.
        run_stop_doc = self._get_stop_doc(uid)
        stream_names = self._event_descriptor_collection.distinct(
            "name",
            {"run_start": uid},
        )
        mapping = {}
        for stream_name in stream_names:
            mapping[stream_name] = functools.partial(
                self._build_event_stream,
                run_start_uid=uid,
                stream_name=stream_name,
                is_complete=(run_stop_doc is not None),
            )

        return BlueskyRun(
            OneShotCachedMap(mapping),
            metadata={
                "start": run_start_doc,
                "stop": run_stop_doc,
                "summary": build_summary(run_start_doc, run_stop_doc, stream_names),
            },
            serializer=self.get_serializer(),
            clear_from_cache=lambda: self._clear_from_cache(uid),
            handler_registry=self.handler_registry,
            transforms=copy.copy(self.transforms),
            root_map=copy.copy(self.root_map),
            datum_collection=self._datum_collection,
            resource_collection=self._resource_collection,
        )

    def _build_event_stream(self, *, run_start_uid, stream_name, is_complete):
        event_descriptors = list(
            self._event_descriptor_collection.find(
                {"run_start": run_start_uid, "name": stream_name}, {"_id": False}
            )
        )
        event_descriptor_uids = [doc["uid"] for doc in event_descriptors]
        # We need each of the sub-dicts to have a consistent length. If
        # Events are still being added, we need to choose a consistent
        # cutoff. If not, we need to know the length anyway. Note that this
        # is not the same thing as the number of Event documents in the
        # stream because seq_num may be repeated, nonunique.
        cursor = self._event_collection.aggregate(
            [
                {"$match": {"descriptor": {"$in": event_descriptor_uids}}},
                {
                    "$group": {
                        "_id": "descriptor",
                        "highest_seq_num": {"$max": "$seq_num"},
                    },
                },
            ]
        )
        results = list(cursor)
        if results:
            (result,) = results
            cutoff_seq_num = (
                1 + result["highest_seq_num"]
            )  # `1 +` because we use a half-open interval
        else:
            cutoff_seq_num = 1
        object_names = event_descriptors[0]["object_keys"]
        run = self[run_start_uid]
        mapping = OneShotCachedMap(
            {
                "data": lambda: DatasetFromDocuments(
                    run=run,
                    stream_name=stream_name,
                    cutoff_seq_num=cutoff_seq_num,
                    event_descriptors=event_descriptors,
                    event_collection=self._event_collection,
                    root_map=self.root_map,
                    sub_dict="data",
                    validate_shape=self.validate_shape,
                ),
                "timestamps": lambda: DatasetFromDocuments(
                    run=run,
                    stream_name=stream_name,
                    cutoff_seq_num=cutoff_seq_num,
                    event_descriptors=event_descriptors,
                    event_collection=self._event_collection,
                    root_map=self.root_map,
                    sub_dict="timestamps",
                    validate_shape=self.validate_shape,
                ),
                "config": lambda: Config(
                    OneShotCachedMap(
                        {
                            object_name: lambda object_name=object_name: build_config_xarray(
                                event_descriptors=event_descriptors,
                                object_name=object_name,
                                sub_dict="data",
                            )
                            for object_name in object_names
                        }
                    )
                ),
                "config_timestamps": lambda: Config(
                    OneShotCachedMap(
                        {
                            object_name: lambda object_name=object_name: build_config_xarray(
                                event_descriptors=event_descriptors,
                                object_name=object_name,
                                sub_dict="timestamps",
                            )
                            for object_name in object_names
                        }
                    )
                ),
            }
        )

        metadata = {"descriptors": event_descriptors, "stream_name": stream_name}
        return BlueskyEventStream(
            mapping,
            metadata=metadata,
            serializer=self._serializer,
            clear_from_cache=lambda: self._clear_from_cache(run_start_uid),
            event_collection=self._event_collection,
            cutoff_seq_num=cutoff_seq_num,
            run=run,
        )

    def __getitem__(self, key):
        # Lookup this key *within the search results* of this MongoAdapter.
        query = self._build_mongo_query({"uid": key})
        run_start_doc = self._run_start_collection.find_one(query, {"_id": False})
        if run_start_doc is None:
            raise KeyError(key)
        return self._get_run(run_start_doc)

    def _chunked_find(self, collection, query, *args, skip=0, limit=None, **kwargs):
        # This is an internal chunking that affects how much we pull from
        # MongoDB at a time.
        CURSOR_LIMIT = 100  # TODO Tune this for performance.
        if limit is not None and limit < CURSOR_LIMIT:
            initial_limit = limit
        else:
            initial_limit = CURSOR_LIMIT
        cursor = (
            collection.find(query, *args, **kwargs)
            .sort(self._sorting + [("_id", 1)])
            .skip(skip)
            .limit(initial_limit)
        )
        # Fetch in batches, starting each batch from where we left off.
        # https://medium.com/swlh/mongodb-pagination-fast-consistent-ece2a97070f3
        tally = 0
        items = []
        last_sorted_values = {}
        while True:
            # Greedily exhaust the cursor. The user may loop over this iterator
            # slowly and, if we don't pull it all into memory now, we'll be
            # holding a long-lived cursor that might get timed out by the
            # MongoDB server.
            items.extend(cursor)  # Exhaust cursor.
            if not items:
                break
            # Next time through the loop, we'll pick up where we left off.
            last_object_id = items[-1]["_id"]
            last_item = items[-1]
            for name, _ in self._sorting:
                # This supports sorting by sub-items like, for example,
                # "XDI.Element.edge".
                first_token, *tokens = name.split(".")
                value = last_item[first_token]
                for token in tokens:
                    value = value[token]
                last_sorted_values[name] = value
                page_cutoff_query = [
                    {
                        "$or": [
                            {
                                name: {
                                    (
                                        "$gt" if direction > 0 else "$lt"
                                    ): last_sorted_values[name]
                                }
                            },
                            {
                                name: last_sorted_values[name],
                                "_id": {"$gt": last_object_id},
                            },
                        ]
                    }
                    for name, direction in self._sorting
                ]
                if "$and" in query:
                    query["$and"].extend(page_cutoff_query)
                else:
                    query["$and"] = page_cutoff_query
            for item in items:
                item.pop("_id")
                yield item
            tally += len(items)
            if limit is not None:
                if tally == limit:
                    break
                if limit - tally < CURSOR_LIMIT:
                    this_limit = limit - tally
                else:
                    this_limit = CURSOR_LIMIT
            else:
                this_limit = CURSOR_LIMIT
            # Get another batch and go round again.
            cursor = (
                collection.find(query, *args, **kwargs)
                .sort(self._sorting + [("_id", 1)])
                .limit(this_limit)
            )
            items.clear()

    def _build_mongo_query(self, *queries):
        combined = self.queries + list(queries)
        if combined:
            return {"$and": combined}
        else:
            return {}

    def _get_stop_doc(self, run_start_uid):
        "This may return None."
        return self._run_stop_collection.find_one(
            {"run_start": run_start_uid}, {"_id": False}
        )

    def __iter__(self):
        for run_start_doc in self._chunked_find(
            self._run_start_collection, self._build_mongo_query()
        ):
            yield run_start_doc["uid"]

    def __len__(self):
        return self._run_start_collection.count_documents(self._build_mongo_query())

    def __length_hint__(self):
        # https://www.python.org/dev/peps/pep-0424/
        return self._run_start_collection.estimated_document_count(
            self._build_mongo_query(),
        )

    def search(self, query):
        """
        Return a MongoAdapter with a subset of the mapping.
        """
        return self.query_registry(query, self)

    def apply_mongo_query(self, query):
        return self.new_variation(
            queries=self.queries + [query],
        )

    def get_distinct(self, metadata, structure_families, specs, counts):
        data = {}

        select = {"$match": self._build_mongo_query()}

        if counts:
            project = {"$project": {"_id": 0, "value": "$_id", "count": "$count"}}
        else:
            project = {"$project": {"_id": 0, "value": "$_id"}}

        if metadata:
            data["metadata"] = {}
            for metadata_key in metadata:
                group = {"$group": {"_id": f"${metadata_key}", "count": {"$sum": 1}}}

                start_list = list(
                    self._run_start_collection.aggregate([select, group, project])
                )
                for item in start_list:
                    if item["value"] is None:
                        start_list.remove(item)
                if len(start_list) > 0:
                    data["metadata"][f"start.{metadata_key}"] = start_list

                stop_list = list(
                    self._run_stop_collection.aggregate([select, group, project])
                )
                for item in stop_list:
                    if item["value"] is None:
                        stop_list.remove(item)
                if len(stop_list) > 0:
                    data["metadata"][f"stop.{metadata_key}"] = stop_list

        if structure_families or specs:
            node_size = len(self.apply_mongo_query({}))

        if structure_families:
            distinct_structure_families = {
                "value": StructureFamily.container,
                "count": node_size,
            }
            data["structure_families"] = [distinct_structure_families]

        if specs:
            distinct_specs = {
                "value": [
                    {"name": BLUESKYRUN_SPEC.name, "version": BLUESKYRUN_SPEC.version}
                ],
                "count": node_size,
            }
            data["specs"] = [distinct_specs]

        return data

    def sort(self, sorting):
        return self.new_variation(sorting=sorting)

    def keys(self):
        return KeysView(lambda: len(self), self._keys_slice)

    def values(self):
        return ValuesView(lambda: len(self), self._items_slice)

    def items(self):
        return ItemsView(lambda: len(self), self._items_slice)

    def _keys_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None
        for run_start_doc in self._chunked_find(
            self._run_start_collection,
            self._build_mongo_query(),
            skip=skip,
            limit=limit,
        ):
            # TODO Fetch just the uid.
            yield run_start_doc["uid"]

    def _items_slice(self, start, stop, direction):
        assert direction == 1, "direction=-1 should be handled by the client"
        skip = start or 0
        if stop is not None:
            limit = stop - skip
        else:
            limit = None
        for run_start_doc in self._chunked_find(
            self._run_start_collection,
            self._build_mongo_query(),
            skip=skip,
            limit=limit,
        ):
            uid = run_start_doc["uid"]
            run = self._get_run(run_start_doc)
            yield (uid, run)


def full_text_search(query, catalog):
    # First if this catalog is backed by mongomock, which does not support $text queries.
    # Avoid importing mongomock if it is not already imported.
    if "mongomock" in sys.modules:
        import mongomock

        if isinstance(catalog.database.client, mongomock.MongoClient):
            # Do the query in memory.
            # For huge MongoAdapters this will be slow, but if you are attempting
            # full text search on a large mongomock-backed MongoAdapter,
            # you have made your choices! :-)
            return BlueskyMapAdapter(dict(catalog)).search(query)

    return catalog.apply_mongo_query(
        {"$text": {"$search": query.text, "$caseSensitive": query.case_sensitive}},
    )


MongoAdapter.register_query(_PartialUID, partial_uid)
MongoAdapter.register_query(_ScanID, scan_id)
MongoAdapter.register_query(ScanIDRange, scan_id_range)
MongoAdapter.register_query(Comparison, comparison)
MongoAdapter.register_query(Contains, contains)
MongoAdapter.register_query(Eq, eq)
MongoAdapter.register_query(FullText, full_text_search)
MongoAdapter.register_query(In, _in)
MongoAdapter.register_query(NotEq, not_eq)
MongoAdapter.register_query(NotIn, not_in)
MongoAdapter.register_query(Regex, regex)
MongoAdapter.register_query(TimeRange, time_range)


class SimpleAccessPolicy:
    """
    Refer to a mapping of user names to lists of entries they can access.

    By Run Start UID
    >>> SimpleAccessPolicy({"alice": [<uuid>, <uuid>], "bob": [<uuid>]}, key="uid")

    By Data Session
    >>> SimpleAccessPolicy({"alice": [<data_session>, key="data_session")
    """

    ALL = ALL_ACCESS

    def __init__(self, access_lists, *, key, provider, scopes=None):
        self.access_lists = {}
        self.key = key
        self.provider = provider
        self.scopes = scopes if (scopes is not None) else ALL_SCOPES
        for key, value in access_lists.items():
            if isinstance(value, str):
                value = import_object(value)
            self.access_lists[key] = value

    def _get_id(self, principal):
        # Get the id (i.e. username) of this Principal for the
        # associated authentication provider.
        for identity in principal.identities:
            if identity.provider == self.provider:
                id = identity.id
                break
        else:
            raise ValueError(
                f"Principcal {principal} has no identity from provider {self.provider}. "
                f"Its identities are: {principal.identities}"
            )
        return id

    def allowed_scopes(self, node, principal):
        # The simple policy does not provide for different Principals to
        # have different scopes on different Nodes. If the Principal has access,
        # they have the same hard-coded access everywhere.
        return self.scopes

    def filters(self, node, principal, scopes):
        if not scopes.issubset(self.scopes):
            return NO_ACCESS
        id = self._get_id(principal)
        access_list = self.access_lists.get(id, [])
        queries = []
        if not ((principal is SpecialUsers.admin) or (access_list == self.ALL)):
            try:
                allowed = set(access_list or [])
            except TypeError:
                # Provide rich debugging info because we have encountered a confusing
                # bug here in a previous implementation.
                raise TypeError(
                    f"Unexpected access_list {access_list} of type {type(access_list)}. "
                    f"Expected iterable or {self.ALL}, instance of {type(self.ALL)}."
                )
            queries.append(In(self.key, allowed))
        return queries


def _get_database(uri):
    if not pymongo.uri_parser.parse_uri(uri)["database"]:
        raise ValueError(
            f"Invalid URI: {uri!r} " f"Did you forget to include a database?"
        )
    else:
        client = pymongo.MongoClient(uri)
        return client.get_database()


def discover_handlers(entrypoint_group_name="databroker.handlers", skip_failures=True):
    """
    Discover handlers via entrypoints.

    Parameters
    ----------
    entrypoint_group_name: str
        Default is 'databroker.handlers', the "official" databroker entrypoint
        for handlers.
    skip_failures: boolean
        True by default. Errors loading a handler class are converted to
        warnings if this is True.

    Returns
    -------
    handler_registry: dict
        A suitable default handler registry
    """
    group = entrypoints.get_group_named(entrypoint_group_name)
    group_all = entrypoints.get_group_all(entrypoint_group_name)
    if len(group_all) != len(group):
        # There are some name collisions. Let's go digging for them.
        for name, matches in itertools.groupby(group_all, lambda ep: ep.name):
            matches = list(matches)
            if len(matches) != 1:
                winner = group[name]
                logger.warning(
                    f"There are {len(matches)} entrypoints for the "
                    f"databroker handler spec {name!r}. "
                    f"They are {matches}. The match {winner} has won the race."
                )
    handler_registry = {}
    for name, entrypoint in group.items():
        try:
            handler_class = entrypoint.load()
        except Exception as exc:
            if skip_failures:
                logger.warning(
                    f"Skipping {entrypoint!r} which failed to load. "
                    f"Exception: {exc!r}"
                )
                continue
            else:
                raise
        handler_registry[name] = handler_class

    return handler_registry


def parse_handler_registry(handler_registry):
    """
    Parse mapping of spec name to 'import path' into mapping to class itself.

    Parameters
    ----------
    handler_registry : dict
        Values may be string 'import paths' to classes or actual classes.

    Examples
    --------
    Pass in name; get back actual class.

    >>> parse_handler_registry({'my_spec': 'package.module.ClassName'})
    {'my_spec': <package.module.ClassName>}

    """
    return _parse_dict_of_objs_or_importable_strings(handler_registry)


def _parse_dict_of_objs_or_importable_strings(d):
    result = {}
    for key, value in d.items():
        if isinstance(value, str):
            try:
                class_ = import_object(value)
            except Exception:
                # For back-compat, trying transforming 'a.b.c' into 'a.b:c'.
                edited_value = ":".join(value.rsplit(".", 1))
                class_ = import_object(edited_value)
                # TODO Warn.
        else:
            class_ = value
        result[key] = class_
    return result


def parse_transforms(transforms):
    """
    Parse mapping of spec name to 'import path' into mapping to class itself.

    Parameters
    ----------
    transforms : collections.abc.Mapping or None
        A collections.abc.Mapping or subclass, that maps any subset of the
        keys {start, stop, resource, descriptor} to a function (or a string
        import path) that accepts a document of the corresponding type and
        returns it, potentially modified. This feature is for patching up
        erroneous metadata. It is intended for quick, temporary fixes that
        may later be applied permanently to the data at rest (e.g via a
        database migration).

    Examples
    --------
    Pass in name; get back actual class.

    >>> parse_transforms({'descriptor': 'package.module.function_name'})
    {'descriptor': <package.module.function_name>}

    """
    if transforms is None:
        return
    elif isinstance(transforms, collections.abc.Mapping):
        allowed_keys = {"start", "stop", "resource", "descriptor"}
        if transforms.keys() - allowed_keys:
            raise NotImplementedError(
                f"Transforms for {transforms.keys() - allowed_keys} "
                f"are not supported."
            )
    else:
        raise ValueError(
            f"Invalid transforms argument {transforms}. "
            f"transforms must be None or a dictionary."
        )
    return _parse_dict_of_objs_or_importable_strings(transforms)


# These are fallback guesses when all we have is a general jsonschema "dtype"
# like "array" no specific "dtype_numpy" like "<u2".
BOOLEAN_DTYPE = BuiltinDtype.from_numpy_dtype(numpy.dtype("bool"))
FLOAT_DTYPE = BuiltinDtype.from_numpy_dtype(numpy.dtype("float64"))
INT_DTYPE = BuiltinDtype.from_numpy_dtype(numpy.dtype("int64"))
STRING_DTYPE = BuiltinDtype.from_numpy_dtype(numpy.dtype("<U"))
JSON_DTYPE_TO_MACHINE_DATA_TYPE = {
    "boolean": BOOLEAN_DTYPE,
    "number": FLOAT_DTYPE,
    "integer": INT_DTYPE,
    "string": STRING_DTYPE,
    "array": FLOAT_DTYPE,  # If this is wrong, set 'dtype_numpy' in data_key to override.
}


def _fill(
    filler,
    event,
    lookup_resource_for_datum,
    get_resource,
    get_datum_for_resource,
    last_datum_id=None,
):
    try:
        _, filled_event = filler("event", event)
        return filled_event
    except event_model.UnresolvableForeignKeyError as err:
        datum_id = err.key
        if datum_id == last_datum_id:
            # We tried to fetch this Datum on the last trip
            # trip through this method, and apparently it did not
            # work. We are in an infinite loop. Bail!
            raise

        # try to fast-path looking up the resource uid if this works
        # it saves us a a database hit (to get the datum document)
        if "/" in datum_id:
            resource_uid, _ = datum_id.split("/", 1)
        # otherwise do it the standard way
        else:
            resource_uid = lookup_resource_for_datum(datum_id)

        # but, it might be the case that the key just happens to have
        # a '/' in it and it does not have any semantic meaning so we
        # optimistically try
        try:
            resource = get_resource(uid=resource_uid)
        # and then fall back to the standard way to be safe
        except ValueError:
            resource = get_resource(lookup_resource_for_datum(datum_id))

        filler("resource", resource)
        # Pre-fetch all datum for this resource.
        for datum in get_datum_for_resource(resource_uid=resource_uid):
            filler("datum", datum)
        # TODO -- When to clear the datum cache in filler?

        # Re-enter and try again now that the Filler has consumed the
        # missing Datum. There might be another missing Datum in this same
        # Event document (hence this re-entrant structure) or might be good
        # to go.
        return _fill(
            filler,
            event,
            lookup_resource_for_datum,
            get_resource,
            get_datum_for_resource,
            last_datum_id=datum_id,
        )


def batch_documents(singles, size):
    # Acculuate rows for Event Pages or Datum Pages in a cache.
    # Drain the cache and emit the page when any of the following conditions
    # are met:
    # (1) We reach a document of a different type.
    # (2) The associated Event Descriptor or Resource changes.
    # (3) The number of rows in the cache reaches `size`.
    cache = collections.deque()
    current_uid = None
    current_type = None

    def handle_item(name, doc):
        nonlocal current_uid
        nonlocal current_type
        if name == "event":
            if (
                (current_type == "event_page")
                and (current_uid == doc["descriptor"])
                and len(cache) < size
            ):
                cache.append(doc)
            elif current_type is None:
                current_type = "event_page"
                current_uid = doc["descriptor"]
                cache.append(doc)
            else:
                # Emit the cache and recurse.
                if current_type == "event_page":
                    yield (current_type, event_model.pack_event_page(*cache))
                if current_type == "datum_page":
                    yield (current_type, event_model.pack_datum_page(*cache))
                cache.clear()
                current_uid = None
                current_type = None
                yield from handle_item(name, doc)
        elif name == "datum":
            if (
                (current_type == "datum_page")
                and (current_uid == doc["resource"])
                and len(cache) < size
            ):
                cache.append(doc)
            elif current_type is None:
                current_type = "datum_page"
                current_uid = doc["resource"]
                cache.append(doc)
            else:
                # Emit the cache and recurse
                if current_type == "event_page":
                    yield (current_type, event_model.pack_event_page(*cache))
                if current_type == "datum_page":
                    yield (current_type, event_model.pack_datum_page(*cache))
                cache.clear()
                current_uid = None
                current_type = None
                yield from handle_item(name, doc)
        else:
            # Emit the cached page (if nonempty) and then this item.
            if cache:
                if current_type == "event_page":
                    yield (current_type, event_model.pack_event_page(*cache))
                elif current_type == "datum_page":
                    yield (current_type, event_model.pack_datum_page(*cache))
                else:
                    assert False
                cache.clear()
                current_uid = None
                current_type = None
            yield (name, doc)

    for name, doc in singles:
        yield from handle_item(name, doc)


class BadShapeMetadata(Exception):
    pass


def default_validate_shape(key, data, expected_shape, uid=None):
    """
    Check that data.shape == expected.shape.

    * If number of dimensions differ, raise BadShapeMetadata
    * If any dimension differs by more than MAX_SIZE_DIFF, raise BadShapeMetadata.
    * If some dimensions are smaller than expected,, pad "right" edge of each
      dimension that falls short with zeros.
    """
    MAX_SIZE_DIFF = 2
    if data.shape == expected_shape:
        return data
    if len(data.shape) != len(expected_shape):
        # The number of dimensions are different; padding can't fix this.
        raise BadShapeMetadata(
            f"For data key {key} "
            f"shape {data.shape} does not "
            f"match expected shape {expected_shape}."
        )
    # Pad at the "end" along any dimension that is too short.
    padding = []
    for actual, expected in zip(data.shape, expected_shape):
        margin = expected - actual
        # Limit how much padding or trimming we are willing to do.
        if abs(margin) > MAX_SIZE_DIFF:
            raise BadShapeMetadata(
                f"For data key {key} "
                f"shape {data.shape} does not "
                f"match expected shape {expected_shape}."
            )
        if margin > 0:
            padding.append((0, margin))
        elif margin < 0:
            padding.append((0, 0))
        else:  # margin == 0
            padding.append((0, 0))
    padded = numpy.pad(data, padding, "edge")

    logger.warning(f"The data.shape: {data.shape} did not match the expected_shape: "
                   f"{expected_shape} for key: '{key}'. This data has been zero-padded "
                   "to match the expected_shape! RunStart UID: {uid}")

    return padded


def build_summary(run_start_doc, run_stop_doc, stream_names):
    summary = {
        "uid": run_start_doc["uid"],
        "scan_id": run_start_doc.get("scan_id"),
        "timestamp": run_start_doc["time"],
        "datetime": datetime.fromtimestamp(run_start_doc["time"]),
        "plan_name": run_start_doc.get("plan_name"),
        "stream_names": stream_names,
    }
    if run_stop_doc is None:
        summary["duration"] = None
    else:
        summary["duration"] = run_stop_doc["time"] - run_start_doc["time"]
    return summary


# for back-compat with old config file
Tree = MongoAdapter
