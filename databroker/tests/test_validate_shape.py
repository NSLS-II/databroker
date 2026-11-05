from bluesky import RunEngine
from bluesky.plans import count
from ophyd.sim import det, img, DirectImage
from tiled.client import Context, from_context
from tiled.server.app import build_app

from ..mongo_normalized import MongoAdapter, BadShapeMetadata

import numpy as np
import pytest


@pytest.mark.parametrize(
    "shape,expected_shape",
    [
        ((10,), (11,)),  # Short by 1, 1d.
        ((10, 20), (10, 21)),  # Short by 1, 2d.
        ((10, 20, 30), (10, 21, 30)),  # Short by 1, 3d.
        ((10, 20, 30), (10, 20, 31)),  # Short by 1, 3d.
        ((10, 20), (10, 19)),  # Too-big by 1, 2d.
        ((20, 20, 20, 20), (20, 21, 20, 22)),  # 4d example.
    ],
)
def test_padding(tmpdir, shape, expected_shape):
    adapter = MongoAdapter.from_mongomock()

    direct_img = DirectImage(
        func=lambda: np.array(np.ones(shape)), name="direct", labels={"detectors"}
    )
    direct_img.img.name = "img"

    with Context.from_app(build_app(adapter)) as context:
        client = from_context(context)

        def post_document(name, doc):
            if name == "descriptor":
                doc["data_keys"]["img"]["shape"] = expected_shape

            client.post_document(name, doc)

        RE = RunEngine()
        RE.subscribe(post_document)
        (uid,) = RE(count([direct_img]))
        assert client[uid]["primary"]["data"]["img"][0].shape == expected_shape


@pytest.mark.parametrize(
    "chunks,shape,expected_chunks",
    [
        ([1, 2], (10,), ((1,), (2, 2, 2, 2, 2))),  # 1D image
        ([1, 3], (10,), ((1,), (3, 3, 3, 1))),  # not evenly divisible.
        ([1, 2, 2], (10, 10), ((1,), (2, 2, 2, 2, 2), (2, 2, 2, 2, 2))),  # 2D
        ([1, 2, -1], (10, 10), ((1,), (2, 2, 2, 2, 2), (10,))),  # -1 for max size.
        ([1, 2, "auto"], (10, 10), ((1,), (2, 2, 2, 2, 2), (10,))),  # auto
        (
            ((1,), (2, 2, 2, 2, 2), (2, 2, 2, 2, 2)),
            (10, 10),
            ((1,), (2, 2, 2, 2, 2), (2, 2, 2, 2, 2)),
        ),  # normalized chunks
        (
            [1, 5, "auto", -1, 5],
            (10, 10, 10, 10),
            ((1,), (5, 5), (10,), (10,), (5, 5))
        ),  # mixture of things.
    ],
)
def test_custom_chunking(tmpdir, chunks, shape, expected_chunks):
    adapter = MongoAdapter.from_mongomock()

    direct_img = DirectImage(
        func=lambda: np.array(np.ones(shape)), name="direct", labels={"detectors"}
    )
    direct_img.img.name = "img"

    with Context.from_app(build_app(adapter)) as context:
        client = from_context(context, "dask")

        def post_document(name, doc):
            if name == "descriptor":
                doc["data_keys"]["img"]["chunks"] = chunks

            client.post_document(name, doc)

        RE = RunEngine()
        RE.subscribe(post_document)
        (uid,) = RE(count([direct_img]))
        assert client[uid]["primary"]["data"]["img"].chunks == expected_chunks


@pytest.mark.parametrize(
    "shape,expected_shape",
    [
        ((10,), (11, 12)),  # Different number of dimensions.
        ((10, 20), (10, 200)),  # Dimension sizes differ by more than 2.
        ((20, 20, 20, 20), (20, 21, 20, 200)),  # Dimension sizes differ by more than 2.
        ((10, 20), (5, 20)),  # Data is bigger than expected.
    ],
)
def test_validate_shape_exceptions(tmpdir, shape, expected_shape):
    adapter = MongoAdapter.from_mongomock()

    direct_img = DirectImage(
        func=lambda: np.array(np.ones(shape)), name="direct", labels={"detectors"}
    )
    direct_img.img.name = "img"

    with Context.from_app(build_app(adapter)) as context:
        client = from_context(context)

        def post_document(name, doc):
            if name == "descriptor":
                doc["data_keys"]["img"]["shape"] = expected_shape

            client.post_document(name, doc)

        RE = RunEngine()
        RE.subscribe(post_document)
        (uid,) = RE(count([direct_img]))
        with pytest.raises(BadShapeMetadata):
            client[uid]["primary"]["data"]["img"][:]


def test_custom_validate_shape(tmpdir):
    # custom_validate_shape will mutate this to show it has been called
    shapes = []

    def custom_validate_shape(key, data, expected_shape):
        shapes.append(expected_shape)
        return data

    adapter = MongoAdapter.from_mongomock(validate_shape=custom_validate_shape)

    with Context.from_app(build_app(adapter)) as context:
        client = from_context(context)

        def post_document(name, doc):
            client.post_document(name, doc)

        RE = RunEngine()
        RE.subscribe(post_document)
        (uid,) = RE(count([img]))
        assert not shapes
        client[uid]["primary"]["data"]["img"][:]
        assert shapes


def test_batched_read_keeps_each_key_shape():
    # container_full calls DatasetFromDocuments.read with every field.
    # An image and a scalar in that batch must keep their own event shapes.
    # DatasetClient.read fetches one array at a time and does not hit this path.
    adapter = MongoAdapter.from_mongomock()
    wide = DirectImage(
        func=lambda: np.arange(12).reshape(3, 4), name="wide", labels={"detectors"}
    )
    wide.img.name = "wide_img"
    narrow = DirectImage(
        func=lambda: np.arange(2), name="narrow", labels={"detectors"}
    )
    narrow.img.name = "narrow_img"

    with Context.from_app(build_app(adapter)) as context:
        client = from_context(context)

        def post_document(name, doc):
            client.post_document(name, doc)

        RE = RunEngine()
        RE.subscribe(post_document)
        (uid,) = RE(count([wide, narrow, det], num=5))
        dataset = adapter[uid]["primary"]["data"]
        loaded = dataset.read()
        wide_img = np.asarray(loaded["wide_img"].read())
        narrow_img = np.asarray(loaded["narrow_img"].read())
        scalar = np.asarray(loaded["det"].read())
    assert wide_img.shape == (5, 3, 4)
    assert narrow_img.shape == (5, 2)
    assert scalar.shape == (5,)
    assert np.array_equal(wide_img[0], np.arange(12).reshape(3, 4))
    assert np.array_equal(narrow_img[0], np.arange(2))
