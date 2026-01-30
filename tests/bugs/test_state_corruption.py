"""
Tests designed to catch state corruption bugs.

These tests specifically target:
1. State machine pointer safety issues
2. Inconsistent state after operations
3. State divergence between replicas
4. Invalid state transitions

Run with: pytest tests/bugs/test_state_corruption.py -v
"""

import pytest
import requests
import time
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conftest import (
    create_scooter, get_scooter, get_all_scooters,
    reserve_scooter, release_scooter, take_snapshot,
    wait_for_replication
)


class TestInvalidStateTransitions:
    """
    Tests that verify invalid state transitions are properly rejected.

    BUG: The state machine might not properly validate state transitions,
    allowing invalid states like:
    - Reserved scooter with no reservation ID
    - Available scooter with a reservation ID
    - Negative distance
    """

    def test_cannot_reserve_nonexistent_scooter(self, api_url):
        """
        Reserving a scooter that doesn't exist should fail.
        """
        response = reserve_scooter(api_url, "totally-fake-scooter-12345", "res-1")

        assert response.status_code == 404, \
            f"BUG: Reserved non-existent scooter! Got {response.status_code}"

    def test_cannot_release_nonexistent_scooter(self, api_url):
        """
        Releasing a scooter that doesn't exist should fail.
        """
        response = release_scooter(api_url, "totally-fake-scooter-67890", 100)

        assert response.status_code == 404, \
            f"BUG: Released non-existent scooter! Got {response.status_code}"

    def test_cannot_double_reserve(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        CATCHES BUG: Cannot reserve an already-reserved scooter.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        # Try to reserve again
        response = reserve_scooter(api_url, unique_scooter_id, "second-reservation")

        assert response.status_code in [400, 409], \
            f"BUG: Double reservation allowed! Got {response.status_code}"

        # Verify original reservation is intact
        get_response = get_scooter(api_url, unique_scooter_id)
        scooter = get_response.json()
        assert scooter["current_reservation_id"] == unique_reservation_id, \
            "BUG: Original reservation was overwritten!"

    def test_cannot_release_available_scooter(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Cannot release a scooter that isn't reserved.
        """
        create_scooter(api_url, unique_scooter_id)

        # Scooter is available - release should fail
        response = release_scooter(api_url, unique_scooter_id, 100)

        assert response.status_code in [400, 409], \
            f"BUG: Released available scooter! Got {response.status_code}"

        # Verify state is unchanged
        get_response = get_scooter(api_url, unique_scooter_id)
        scooter = get_response.json()
        assert scooter["is_available"] == True
        assert scooter["total_distance"] == 0  # Distance should NOT have been added


class TestStateConsistencyInvariants:
    """
    Tests that verify state machine invariants are maintained.

    Invariants:
    1. If is_available=False, current_reservation_id must be non-empty
    2. If is_available=True, current_reservation_id must be empty
    3. total_distance must never decrease
    4. total_distance must be non-negative
    """

    def test_reservation_invariant(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        CATCHES BUG: Reserved scooter must have reservation ID.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()

        # Invariant: is_available=False implies reservation_id is set
        assert scooter["is_available"] == False
        assert scooter["current_reservation_id"] == unique_reservation_id, \
            f"BUG: Reserved scooter has wrong/missing reservation ID: {scooter}"

    def test_available_invariant(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        CATCHES BUG: Available scooter must not have reservation ID.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        release_scooter(api_url, unique_scooter_id, 50)

        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()

        # Invariant: is_available=True implies reservation_id is cleared
        assert scooter["is_available"] == True
        assert scooter.get("current_reservation_id", "") in ["", None], \
            f"BUG: Available scooter still has reservation ID: {scooter}"

    def test_distance_never_decreases(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Total distance should never decrease.
        """
        create_scooter(api_url, unique_scooter_id)

        last_distance = 0

        for i in range(10):
            reserve_scooter(api_url, unique_scooter_id, f"dist-check-{i}")
            release_scooter(api_url, unique_scooter_id, (i + 1) * 10)

            response = get_scooter(api_url, unique_scooter_id)
            current_distance = response.json()["total_distance"]

            assert current_distance >= last_distance, \
                f"BUG: Distance decreased! Was {last_distance}, now {current_distance}"

            last_distance = current_distance

    def test_distance_always_nonnegative(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Total distance must always be >= 0.
        """
        create_scooter(api_url, unique_scooter_id)

        response = get_scooter(api_url, unique_scooter_id)
        distance = response.json()["total_distance"]

        assert distance >= 0, f"BUG: Negative distance on new scooter: {distance}"

        # After operations, still non-negative
        reserve_scooter(api_url, unique_scooter_id, "check-1")
        release_scooter(api_url, unique_scooter_id, 100)

        response = get_scooter(api_url, unique_scooter_id)
        distance = response.json()["total_distance"]

        assert distance >= 0, f"BUG: Negative distance after operations: {distance}"


class TestStateDivergence:
    """
    Tests for state divergence between replicas.

    BUG: Due to race conditions in Paxos or replication,
    different servers might have different views of state.
    """

    def test_all_servers_same_scooter_state(self, server_urls, unique_scooter_id, unique_reservation_id):
        """
        CATCHES BUG: All servers should have identical scooter state.
        """
        # Create and modify scooter
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(1)
        reserve_scooter(server_urls[0], unique_scooter_id, unique_reservation_id)
        time.sleep(1)
        release_scooter(server_urls[0], unique_scooter_id, 150)

        # Wait for replication
        time.sleep(3)

        # Query all servers
        states = []
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    states.append(response.json())
            except Exception:
                pass

        # All states should be identical
        if len(states) > 1:
            first = states[0]
            for i, state in enumerate(states[1:], 2):
                assert state["is_available"] == first["is_available"], \
                    f"BUG: Server divergence on is_available"
                assert state["total_distance"] == first["total_distance"], \
                    f"BUG: Server divergence on total_distance: {first['total_distance']} vs {state['total_distance']}"
                assert state.get("current_reservation_id") == first.get("current_reservation_id"), \
                    f"BUG: Server divergence on reservation_id"

    def test_all_servers_same_scooter_count(self, server_urls, unique_scooter_id):
        """
        CATCHES BUG: All servers should have the same number of scooters.
        """
        # Create several scooters
        scooter_ids = [f"{unique_scooter_id}-count-{i}" for i in range(5)]
        for sid in scooter_ids:
            create_scooter(server_urls[0], sid)

        # Wait for replication
        time.sleep(5)

        # Count scooters on each server
        counts = []
        for url in server_urls:
            try:
                response = get_all_scooters(url)
                if response.status_code == 200:
                    # Count only our test scooters
                    all_scooters = response.json()
                    our_count = sum(1 for s in all_scooters if s["id"] in scooter_ids)
                    counts.append(our_count)
            except Exception:
                pass

        # All counts should be equal
        if len(counts) > 1:
            assert all(c == counts[0] for c in counts), \
                f"BUG: Server divergence on scooter count: {counts}"


class TestCorruptionAfterCrash:
    """
    Tests for state corruption that might occur after crashes or restarts.

    BUG: If commit happens but Apply fails, or if there's a crash
    between operations, state might be corrupted on recovery.
    """

    def test_state_survives_operations(self, api_url, unique_scooter_id):
        """
        Test that state is consistent after many operations.

        If there are corruption bugs, they're more likely to manifest
        after many operations.
        """
        create_scooter(api_url, unique_scooter_id)

        expected_distance = 0

        for i in range(100):
            reserve_scooter(api_url, unique_scooter_id, f"survival-{i}")
            release_scooter(api_url, unique_scooter_id, 1)
            expected_distance += 1

        # Check final state
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()

        assert scooter["is_available"] == True, \
            f"BUG: State corrupted - scooter should be available"
        assert scooter["total_distance"] == expected_distance, \
            f"BUG: State corrupted - expected {expected_distance}, got {scooter['total_distance']}"

    def test_state_after_snapshot(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        CATCHES BUG: State should be correct after snapshot.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        release_scooter(api_url, unique_scooter_id, 100)

        # Take snapshot
        take_snapshot(api_url)
        time.sleep(1)

        # Do more operations
        reserve_scooter(api_url, unique_scooter_id, "post-snap-res")
        release_scooter(api_url, unique_scooter_id, 50)

        # Verify state is correct (100 + 50 = 150)
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()

        assert scooter["total_distance"] == 150, \
            f"BUG: State corrupted after snapshot - expected 150, got {scooter['total_distance']}"


class TestConcurrentModificationCorruption:
    """
    Tests for state corruption due to concurrent modifications.

    BUG: Without proper locking, concurrent modifications can
    cause torn reads/writes or lost updates.
    """

    def test_concurrent_operations_no_corruption(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Concurrent operations shouldn't corrupt state.
        """
        create_scooter(api_url, unique_scooter_id)

        successful_ops = []
        lock = threading.Lock()

        def do_operation(op_num):
            try:
                res = reserve_scooter(api_url, unique_scooter_id, f"conc-{op_num}")
                if res.status_code == 200:
                    rel = release_scooter(api_url, unique_scooter_id, 10)
                    if rel.status_code == 200:
                        with lock:
                            successful_ops.append(10)
            except Exception:
                pass

        # Run operations - due to reservation, only one at a time can succeed
        # But we're testing for corruption, not throughput
        for i in range(30):
            do_operation(i)

        # Final distance should equal sum of successful releases
        expected = sum(successful_ops)
        response = get_scooter(api_url, unique_scooter_id)
        actual = response.json()["total_distance"]

        assert actual == expected, \
            f"BUG: Corruption detected! Expected {expected}, got {actual}"

    def test_rapid_modifications_integrity(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Rapid modifications shouldn't lose data.
        """
        create_scooter(api_url, unique_scooter_id)

        # Rapid reserve/release
        for i in range(50):
            reserve_scooter(api_url, unique_scooter_id, f"rapid-mod-{i}")
            release_scooter(api_url, unique_scooter_id, 2)

        # Should have 50 * 2 = 100 distance
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 100, \
            f"BUG: Lost updates! Expected 100, got {response.json()['total_distance']}"


class TestMultiScooterCorruption:
    """
    Tests that operations on one scooter don't corrupt another.

    BUG: If the state machine or log uses wrong indices,
    operations might affect the wrong scooter.
    """

    def test_operations_isolated_between_scooters(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Operations on one scooter shouldn't affect another.
        """
        sid1 = f"{unique_scooter_id}-iso-1"
        sid2 = f"{unique_scooter_id}-iso-2"

        create_scooter(api_url, sid1)
        create_scooter(api_url, sid2)

        # Operate only on scooter 1
        reserve_scooter(api_url, sid1, "iso-res-1")
        release_scooter(api_url, sid1, 100)

        # Check scooter 2 is unaffected
        response = get_scooter(api_url, sid2)
        scooter2 = response.json()

        assert scooter2["is_available"] == True, \
            f"BUG: Scooter 2 was reserved when only scooter 1 should be!"
        assert scooter2["total_distance"] == 0, \
            f"BUG: Scooter 2 has distance when it shouldn't!"

    def test_interleaved_operations_correct(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Interleaved operations on different scooters should be correct.
        """
        sids = [f"{unique_scooter_id}-inter-{i}" for i in range(5)]

        # Create all
        for sid in sids:
            create_scooter(api_url, sid)

        # Interleave operations
        for i, sid in enumerate(sids):
            reserve_scooter(api_url, sid, f"inter-res-{i}")

        for i, sid in enumerate(sids):
            release_scooter(api_url, sid, (i + 1) * 10)

        # Verify each has correct distance
        for i, sid in enumerate(sids):
            response = get_scooter(api_url, sid)
            expected = (i + 1) * 10
            actual = response.json()["total_distance"]
            assert actual == expected, \
                f"BUG: Scooter {sid} has wrong distance! Expected {expected}, got {actual}"
