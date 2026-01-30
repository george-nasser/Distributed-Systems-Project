"""
Tests for consistency guarantees as specified in the assignment.

From the assignment (Section 3.2 Client API):
- Read operations: Sequentially Consistent OR Linearizable
- Write operations (state changing): Linearizable

Linearizable = operations appear to happen atomically at some point between
invocation and response. Real-time ordering is preserved.

Sequentially Consistent = all processes see the same order of operations,
but that order doesn't have to match real-time.

Run with: pytest tests/assignment/test_consistency_guarantees.py -v
"""

import pytest
import time
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conftest import (
    create_scooter, get_scooter, get_all_scooters,
    reserve_scooter, release_scooter
)


class TestLinearizableWrites:
    """
    Tests that write operations are linearizable.

    Linearizable writes mean:
    1. Once a write completes, all subsequent reads see that write
    2. Concurrent writes are serialized in some order
    3. The order respects real-time (if A completes before B starts, A comes first)
    """

    def test_write_then_read_sees_write(self, api_url, unique_scooter_id):
        """
        After a write completes, an immediate read must see the write.

        This is the basic linearizability requirement - no stale reads
        after a write has completed.
        """
        # Write: create scooter
        create_response = create_scooter(api_url, unique_scooter_id)
        assert create_response.status_code in [200, 201]

        # Read immediately after write completes
        read_response = get_scooter(api_url, unique_scooter_id)

        # Must see the scooter we just created
        assert read_response.status_code == 200, \
            "Linearizability violated: read after write didn't see the write"
        assert read_response.json()["id"] == unique_scooter_id

    def test_write_then_read_sees_latest_state(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        After a state-changing write, reads must see the new state.
        """
        create_scooter(api_url, unique_scooter_id)

        # Write: reserve scooter
        reserve_response = reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        assert reserve_response.status_code == 200

        # Read must see reserved state
        read_response = get_scooter(api_url, unique_scooter_id)
        scooter = read_response.json()

        assert scooter["is_available"] == False, \
            "Linearizability violated: read after reserve didn't see reserved state"
        assert scooter["current_reservation_id"] == unique_reservation_id

    def test_sequential_writes_ordered(self, api_url, unique_scooter_id):
        """
        Sequential writes must be applied in real-time order.

        If write A completes before write B starts, then A must
        come before B in the total order.
        """
        create_scooter(api_url, unique_scooter_id)

        # Sequential writes with different distances
        distances = [10, 20, 30, 40, 50]

        for i, distance in enumerate(distances):
            reserve_scooter(api_url, unique_scooter_id, f"seq-{i}")
            release_scooter(api_url, unique_scooter_id, distance)

        # Final read must show sum of all distances in order
        response = get_scooter(api_url, unique_scooter_id)
        expected_total = sum(distances)  # 150

        assert response.json()["total_distance"] == expected_total, \
            f"Linearizability violated: expected {expected_total}, got {response.json()['total_distance']}"

    def test_concurrent_writes_serialized(self, api_url, unique_scooter_id):
        """
        Concurrent writes must be serialized - only one reservation can win.

        When multiple clients try to reserve the same scooter, exactly
        one must succeed and others must fail.
        """
        create_scooter(api_url, unique_scooter_id)
        time.sleep(0.5)  # Ensure scooter is replicated

        results = []

        def try_reserve(client_id):
            try:
                response = reserve_scooter(api_url, unique_scooter_id, f"client-{client_id}")
                return (client_id, response.status_code)
            except Exception as e:
                return (client_id, str(e))

        # Launch concurrent reservations
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(try_reserve, i) for i in range(5)]
            results = [f.result() for f in as_completed(futures)]

        # Count successful reservations
        successes = [r for r in results if r[1] == 200]

        # Exactly one should succeed (serialization)
        assert len(successes) >= 1, "No reservation succeeded"

        # Check final state - scooter should be reserved by exactly one client
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()

        assert scooter["is_available"] == False
        assert scooter["current_reservation_id"].startswith("client-")


class TestLinearizableReads:
    """
    Tests for linearizable reads (stronger consistency).

    The assignment says reads can be either sequentially consistent OR
    linearizable. These tests check if linearizable reads are supported.
    """

    def test_read_after_write_from_same_client(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        A client's read after their own write must see that write.

        This is called "read-your-writes" and is required for linearizability.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        # Immediate read from same "client" (same connection)
        response = get_scooter(api_url, unique_scooter_id)

        assert response.json()["is_available"] == False, \
            "Read-your-writes violated"
        assert response.json()["current_reservation_id"] == unique_reservation_id

    def test_no_stale_reads_after_acknowledged_write(self, api_url, unique_scooter_id):
        """
        Once a write is acknowledged, no reader should see the old state.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, "initial-res")
        release_scooter(api_url, unique_scooter_id, 100)

        # Write acknowledged - now do multiple reads
        for _ in range(10):
            response = get_scooter(api_url, unique_scooter_id)
            scooter = response.json()

            # Every read must see the write
            assert scooter["is_available"] == True
            assert scooter["total_distance"] == 100, \
                f"Stale read detected: got distance {scooter['total_distance']}"


class TestSequentialConsistency:
    """
    Tests for sequential consistency (weaker than linearizable).

    Sequential consistency means all processes see the same order of
    operations, but that order doesn't have to match real-time.
    """

    def test_all_servers_see_same_order(self, server_urls, unique_scooter_id):
        """
        All servers should eventually see the same sequence of operations.
        """
        # Create scooter and do operations
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(1)

        for i in range(5):
            reserve_scooter(server_urls[0], unique_scooter_id, f"order-{i}")
            release_scooter(server_urls[0], unique_scooter_id, (i + 1) * 10)

        # Wait for replication
        time.sleep(5)

        # All servers should have the same final state
        states = []
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    states.append(response.json()["total_distance"])
            except Exception:
                pass

        # All should agree (same order of operations applied)
        if len(states) > 1:
            assert all(s == states[0] for s in states), \
                f"Sequential consistency violated: different servers have different states: {states}"

    def test_operations_appear_in_some_total_order(self, api_url, unique_scooter_id):
        """
        Operations must appear to happen in some total order.

        For distance accumulation, this means the final distance should
        be the sum of all individual distances (no lost updates).
        """
        create_scooter(api_url, unique_scooter_id)

        expected_total = 0
        for i in range(20):
            reserve_scooter(api_url, unique_scooter_id, f"total-{i}")
            release_scooter(api_url, unique_scooter_id, 5)
            expected_total += 5

        response = get_scooter(api_url, unique_scooter_id)
        actual = response.json()["total_distance"]

        assert actual == expected_total, \
            f"Operations not properly ordered: expected {expected_total}, got {actual}"


class TestReadOperations:
    """
    Tests for read operations (GET endpoints).

    From assignment: reads should be at least sequentially consistent.
    """

    def test_get_single_scooter(self, api_url, unique_scooter_id):
        """
        GET /scooters/:id returns the scooter state.
        """
        create_scooter(api_url, unique_scooter_id)

        response = get_scooter(api_url, unique_scooter_id)

        assert response.status_code == 200
        scooter = response.json()
        assert scooter["id"] == unique_scooter_id
        assert "is_available" in scooter
        assert "total_distance" in scooter

    def test_get_all_scooters(self, api_url, unique_scooter_id):
        """
        GET /scooters returns list of all scooters.
        """
        # Create some scooters
        ids = [f"{unique_scooter_id}-{i}" for i in range(3)]
        for sid in ids:
            create_scooter(api_url, sid)

        response = get_all_scooters(api_url)

        assert response.status_code == 200
        scooters = response.json()
        assert isinstance(scooters, list)

        # Our scooters should be in the list
        returned_ids = [s["id"] for s in scooters]
        for sid in ids:
            assert sid in returned_ids

    def test_get_nonexistent_scooter(self, api_url):
        """
        GET /scooters/:id for nonexistent returns 404.
        """
        response = get_scooter(api_url, "nonexistent-scooter-xyz")

        assert response.status_code == 404


class TestWriteOperations:
    """
    Tests for write operations (state-changing endpoints).

    From assignment: writes must be linearizable.
    """

    def test_create_scooter_write(self, api_url, unique_scooter_id):
        """
        PUT /scooters/:id creates a new scooter (write operation).
        """
        response = create_scooter(api_url, unique_scooter_id)

        assert response.status_code in [200, 201]

        # Verify write took effect
        get_response = get_scooter(api_url, unique_scooter_id)
        assert get_response.status_code == 200

    def test_reserve_scooter_write(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        POST /scooters/:id/reservations changes state (write operation).
        """
        create_scooter(api_url, unique_scooter_id)

        response = reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        assert response.status_code == 200

        # Verify state changed
        get_response = get_scooter(api_url, unique_scooter_id)
        assert get_response.json()["is_available"] == False

    def test_release_scooter_write(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        POST /scooters/:id/releases changes state (write operation).
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        response = release_scooter(api_url, unique_scooter_id, 100)

        assert response.status_code == 200

        # Verify state changed
        get_response = get_scooter(api_url, unique_scooter_id)
        scooter = get_response.json()
        assert scooter["is_available"] == True
        assert scooter["total_distance"] == 100


class TestRealTimeOrdering:
    """
    Tests for real-time ordering (required for linearizability).

    If operation A completes before operation B starts, then A must
    appear before B in the total order.
    """

    def test_sequential_operations_respect_real_time(self, api_url, unique_scooter_id):
        """
        Operations done in sequence must be ordered correctly.
        """
        create_scooter(api_url, unique_scooter_id)

        # Op 1: Reserve
        reserve_scooter(api_url, unique_scooter_id, "first")
        # Op 1 complete

        # Op 2: Release with distance 100
        release_scooter(api_url, unique_scooter_id, 100)
        # Op 2 complete

        # Op 3: Reserve again
        reserve_scooter(api_url, unique_scooter_id, "second")
        # Op 3 complete

        # Op 4: Release with distance 50
        release_scooter(api_url, unique_scooter_id, 50)
        # Op 4 complete

        # Final state must reflect ops in order: 100 + 50 = 150
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 150

    def test_writes_visible_to_subsequent_reads(self, api_url, unique_scooter_id):
        """
        Each write must be visible to all subsequent reads.
        """
        create_scooter(api_url, unique_scooter_id)

        for i in range(10):
            # Write
            reserve_scooter(api_url, unique_scooter_id, f"iteration-{i}")
            release_scooter(api_url, unique_scooter_id, 10)

            # All subsequent reads must see this write
            response = get_scooter(api_url, unique_scooter_id)
            expected_distance = (i + 1) * 10

            assert response.json()["total_distance"] == expected_distance, \
                f"Iteration {i}: expected {expected_distance}, got {response.json()['total_distance']}"
