"""B1b: the FastAPI control plane.

Uses httpx against the real ASGI app — no running server, but every layer
below it is real: a real Store, a real Topic, real segments on tmp_path.
"""

import httpx
import pytest

from pyka.broker.admin import create_app
from pyka.broker.store import Store
from pyka.topic.policy import SYNC_EVERY_RECORD


@pytest.fixture
def store(tmp_path):
    return Store(tmp_path / "data", partitions=2, sync_policy=SYNC_EVERY_RECORD)


@pytest.fixture
async def client(store):
    await store.open()
    transport = httpx.ASGITransport(app=create_app(store))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        yield client


# --------------------------------------------------------------------------
# probes — what Kubernetes calls
# --------------------------------------------------------------------------


async def test_healthz_is_ok_even_before_recovery(store):
    """Liveness must not depend on storage.

    A broker still scanning segments is alive; restarting it only makes it
    begin the same scan again. Conflating this with readiness is what turns a
    slow recovery into a crash loop.
    """
    transport = httpx.ASGITransport(app=create_app(store))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        assert not store.ready
        assert (await client.get("/healthz")).status_code == 200


async def test_readyz_is_503_until_recovery_finishes(store):
    transport = httpx.ASGITransport(app=create_app(store))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        response = await client.get("/readyz")
        assert response.status_code == 503
        assert response.json() == {"ready": False}

        await store.open()
        response = await client.get("/readyz")
        assert response.status_code == 200
        assert response.json() == {"ready": True}


# --------------------------------------------------------------------------
# topics
# --------------------------------------------------------------------------


async def test_a_new_broker_has_no_topics(client):
    assert (await client.get("/topics")).json() == []


async def test_create_then_list(client):
    response = await client.post("/topics", json={"name": "orders"})
    assert response.status_code == 201
    assert response.json() == {"name": "orders", "partitions": 2}
    assert (await client.get("/topics")).json() == ["orders"]


async def test_create_honours_an_explicit_partition_count(client):
    response = await client.post("/topics", json={"name": "orders", "partitions": 4})
    assert response.json()["partitions"] == 4


async def test_create_is_idempotent_and_never_repartitions(client):
    await client.post("/topics", json={"name": "orders", "partitions": 4})
    again = await client.post("/topics", json={"name": "orders", "partitions": 9})
    assert again.json()["partitions"] == 4  # the real count, not the request


@pytest.mark.parametrize("name", ["../escape", "a/b", "", "."])
async def test_an_invalid_topic_name_is_400(client, name):
    # The security boundary, over HTTP: these arrive from outside.
    response = await client.post("/topics", json={"name": name})
    assert response.status_code in (400, 422)


async def test_describe_a_topic_reports_its_partition_count(client):
    await client.post("/topics", json={"name": "orders", "partitions": 3})
    assert (await client.get("/topics/orders")).json() == {
        "name": "orders",
        "partitions": 3,
    }


async def test_describe_an_unknown_topic_is_404(client):
    assert (await client.get("/topics/nope")).status_code == 404


# --------------------------------------------------------------------------
# the segment view — where the storage layer becomes observable
# --------------------------------------------------------------------------


async def test_a_fresh_partition_has_one_empty_unsealed_segment(client):
    await client.post("/topics", json={"name": "orders"})
    body = (await client.get("/topics/orders/partitions/0")).json()

    assert body["partition"] == 0
    assert body["next_offset"] == 0
    assert len(body["segments"]) == 1
    assert body["segments"][0] == {
        "base_offset": 0,
        "next_offset": 0,
        "size_bytes": 0,
        "index_entries": 0,
        "sealed": False,
    }


async def test_the_segment_view_tracks_appends(client, store):
    await client.post("/topics", json={"name": "orders"})
    partition, _ = await store.append("orders", b"user-1", b"v" * 100)

    body = (await client.get(f"/topics/orders/partitions/{partition}")).json()
    assert body["next_offset"] == 1
    assert body["size_bytes"] > 100
    assert body["segments"][-1]["sealed"] is False


async def test_rolling_seals_the_old_segment_and_opens_a_new_one(client, store):
    await client.post("/topics", json={"name": "orders"})
    # A key, not None: null keys round-robin, so two of them would land in
    # different partitions and this test would be describing the wrong log.
    partition, _ = await store.append("orders", b"user-1", b"payload")

    before = (await client.get(f"/topics/orders/partitions/{partition}")).json()
    assert len(before["segments"]) == 1

    rolled = await client.post(f"/topics/orders/partitions/{partition}/roll")
    assert rolled.status_code == 200
    assert rolled.json()["sealed"] is False  # the NEW segment

    after = (await client.get(f"/topics/orders/partitions/{partition}")).json()
    assert len(after["segments"]) == 2
    assert after["segments"][0]["sealed"] is True   # sealed = closed
    assert after["segments"][1]["sealed"] is False
    # the chain stays continuous across the roll
    assert after["segments"][1]["base_offset"] == after["segments"][0]["next_offset"]


async def test_the_offset_sequence_survives_a_roll(client, store):
    await client.post("/topics", json={"name": "orders"})
    first_partition, first = await store.append("orders", b"user-1", b"a")
    await client.post(f"/topics/orders/partitions/{first_partition}/roll")
    same_partition, second = await store.append("orders", b"user-1", b"b")

    # Same key, so the same partition — and the offsets kept counting rather
    # than restarting inside the new segment.
    assert (same_partition, first, second) == (first_partition, 0, 1)
    body = (await client.get(f"/topics/orders/partitions/{first_partition}")).json()
    assert body["next_offset"] == 2


async def test_rolling_an_unknown_topic_is_404(client):
    assert (await client.post("/topics/nope/partitions/0/roll")).status_code == 404


async def test_an_out_of_range_partition_is_400(client):
    await client.post("/topics", json={"name": "orders"})
    assert (await client.get("/topics/orders/partitions/9")).status_code == 400


async def test_sync_forces_durability_on_every_partition(client, store):
    # The escape hatch from SyncPolicy — how an operator makes the tail
    # durable now, rather than when the OS gets round to it.
    await client.post("/topics", json={"name": "orders"})
    await store.append("orders", None, b"payload")
    assert (await client.post("/topics/orders/sync")).status_code == 200


async def test_sync_on_an_unknown_topic_is_404(client):
    assert (await client.post("/topics/nope/sync")).status_code == 404


# --------------------------------------------------------------------------
# the read seam — batched, because a generator would leak blocking I/O
# back onto the event loop
# --------------------------------------------------------------------------


async def test_store_read_returns_records_from_an_offset(client, store):
    await client.post("/topics", json={"name": "orders"})
    partition, _ = await store.append("orders", b"user-1", b"a")
    await store.append("orders", b"user-1", b"b")
    await store.append("orders", b"user-1", b"c")

    records = await store.read("orders", 1, partition)
    assert [r.value for r in records] == [b"b", b"c"]


async def test_store_read_respects_its_limit(client, store):
    await client.post("/topics", json={"name": "orders"})
    partition, _ = await store.append("orders", b"user-1", b"a")
    for value in (b"b", b"c", b"d"):
        await store.append("orders", b"user-1", value)

    assert len(await store.read("orders", 0, partition, limit=2)) == 2
    assert len(await store.read("orders", 0, partition, limit=0)) == 4  # 0 = all


# --------------------------------------------------------------------------
# broker info
# --------------------------------------------------------------------------


async def test_broker_info_reports_the_ring_and_topic_count(client):
    await client.post("/topics", json={"name": "orders"})
    body = (await client.get("/")).json()
    assert body == {"broker_id": 0, "brokers": 1, "ready": True, "topics": 1}


async def test_openapi_docs_are_served(client):
    # The reason FastAPI is here rather than a hand-rolled handler: the
    # control plane documents itself.
    schema = (await client.get("/openapi.json")).json()
    assert "/topics/{name}/partitions/{partition}" in schema["paths"]


# --------------------------------------------------------------------------
# restart — the thing to watch in Kubernetes
# --------------------------------------------------------------------------


async def test_a_new_store_on_the_same_directory_recovers_everything(tmp_path, store):
    """`kubectl delete pod`, in miniature.

    The PersistentVolumeClaim outlives the pod, so the replacement process
    opens the same directory and rebuilds its state by scanning. That is why
    /readyz exists, and why recovery is eager rather than lazy.
    """
    await store.open()
    await store.create("orders")
    for n in range(5):
        await store.append("orders", f"k{n}".encode(), b"payload")
    await store.close()

    reborn = Store(tmp_path / "data", partitions=2)
    await reborn.open()
    transport = httpx.ASGITransport(app=create_app(reborn))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        assert (await client.get("/topics")).json() == ["orders"]
        totals = [
            (await client.get(f"/topics/orders/partitions/{p}")).json()["next_offset"]
            for p in range(2)
        ]
        assert sum(totals) == 5  # every record still there, across partitions
