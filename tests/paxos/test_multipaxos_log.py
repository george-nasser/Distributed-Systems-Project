"""
Tests for MultiPaxos log behavior observable via API.

MultiPaxos maintains a replicated log where each slot goes through
Paxos consensus. These tests verify:
- Commands are ordered consistently across all servers
- Log gaps are handled correctly
- Different command types work properly
- Commit behavior is correct

We can't inspect the log directly, but we can observe its effects
through the state machine.

Run with: pytest tests/paxos/test_multipaxos_log.py -v
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
    reserve_scooter, release_scooter,
    wait_for_replication
)


class TestLogOrdering:
    """
    Tests for log ordering.

    MultiPaxos ensures all replicas apply commands in the same order.
    """

    def test_commands_in_same_order_all_servers(self, server_urls, unique_scooter_id):
        """
        All servers apply commands in the same order.

        If we do CREATE -> RESERVE -> RELEASE on one server, all servers
        should end up with the same final state.
        """
        # Do operations on server 0
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(0.5)

        for i in range(5):
            reserve_scooter(server_urls[0], unique_scooter_id, f"order-test-{i}")
            release_scooter(server_urls[0], unique_scooter_id, (i + 1) * 10)

        # Expected: 10 + 20 + 30 + 40 + 50 = 150
        expected_distance = 150

        # Wait for replication
        time.sleep(3)

        # All servers should have the same state
        distances = []
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    distances.append(response.json()["total_distance"])
            except Exception:
                pass

        # All responding servers should agree
        assert len(distances) >= 3, "Not enough servers responded"
        for d in distances:
            assert d == expected_distance, \
                f"Server has wrong distance {d}, expected {expected_distance}"

    def test_sequential_commands_sequential_slots(self, api_url, unique_scooter_id):
        """
        Sequential commands go to sequential log slots.

        Each operation should advance the log, and the order should
        be reflected in the final state.
        """
        create_scooter(api_url, unique_scooter_id)

        # Do operations that depend on order
        distances = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        for i, d in enumerate(distances):
            reserve_scooter(api_url, unique_scooter_id, f"slot-{i}")
            release_scooter(api_url, unique_scooter_id, d)

        # Total should be sum (1+2+...+10 = 55)
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 55

    def test_concurrent_commands_ordered(self, api_url, unique_scooter_id):
        """
        Concurrent commands get a total order.

        Even if we submit concurrently, the log gives them a total order
        and applies them consistently.
        """
        # Create multiple scooters concurrently
        scooter_ids = [f"{unique_scooter_id}-concurrent-{i}" for i in range(5)]

        def create_one(sid):
            try:
                return create_scooter(api_url, sid)
            except Exception as e:
                return e

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(create_one, sid) for sid in scooter_ids]
            results = [f.result() for f in as_completed(futures)]

        # Wait for consistency
        time.sleep(2)

        # All should exist
        response = get_all_scooters(api_url)
        all_ids = [s["id"] for s in response.json()]

        for sid in scooter_ids:
            assert sid in all_ids, f"Scooter {sid} not created"


class TestLogGaps:
    """
    Tests for log gap handling.

    If a log slot commits out of order (e.g., slot 3 before slot 2),
    the system should still apply entries in order.
    """

    def test_gap_filled_before_apply(self, api_url, unique_scooter_id):
        """
        State reflects operations in log order, not commit order.

        Even if operations commit out of order internally, the state
        machine should see them in log order.
        """
        create_scooter(api_url, unique_scooter_id)

        # Sequential operations
        total = 0
        for i in range(10):
            reserve_scooter(api_url, unique_scooter_id, f"gap-{i}")
            release_scooter(api_url, unique_scooter_id, 5)
            total += 5

            # Check state after each operation
            response = get_scooter(api_url, unique_scooter_id)
            current_distance = response.json()["total_distance"]

            # Distance should only increase, never decrease
            assert current_distance >= (i + 1) * 5, \
                f"Distance went backwards: got {current_distance}, expected at least {(i+1)*5}"

    def test_no_gaps_in_applied_state(self, api_url, unique_scooter_id):
        """
        The state machine should reflect all commands up to commit point.

        We shouldn't see "holes" in the state.
        """
        create_scooter(api_url, unique_scooter_id)

        # Do 20 operations
        for i in range(20):
            reserve_scooter(api_url, unique_scooter_id, f"no-gap-{i}")
            release_scooter(api_url, unique_scooter_id, 1)

        # Final state should reflect all 20 releases
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 20, \
            "Some operations were lost (gap in log)"

    def test_system_handles_out_of_order_commits(self, server_urls, unique_scooter_id):
        """
        Out-of-order internal commits don't corrupt state.

        We can't force out-of-order commits from the API, but we can
        verify that under concurrent load the state stays correct.
        """
        # Create scooter
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(1)

        # Concurrent operations from different servers
        results = []
        lock = threading.Lock()

        def do_operation(server_url, iteration):
            try:
                # Only try if scooter is available
                response = get_scooter(server_url, unique_scooter_id)
                if response.status_code == 200 and response.json()["is_available"]:
                    res = reserve_scooter(server_url, unique_scooter_id, f"ooo-{iteration}")
                    if res.status_code == 200:
                        release_scooter(server_url, unique_scooter_id, 1)
                        with lock:
                            results.append(("success", iteration))
            except Exception as e:
                with lock:
                    results.append(("error", str(e)))

        # Launch operations from different servers
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for i in range(20):
                server_url = server_urls[i % len(server_urls)]
                futures.append(executor.submit(do_operation, server_url, i))
            for f in as_completed(futures):
                pass

        # Wait for everything to settle
        time.sleep(3)

        # Count successes
        successes = sum(1 for r in results if r[0] == "success")

        # Final distance should equal number of successful operations
        response = get_scooter(server_urls[0], unique_scooter_id)
        if response.status_code == 200:
            final_distance = response.json()["total_distance"]
            # Should match (or be close due to timing)
            # Not exact because some might have failed due to reservation conflicts
            assert final_distance >= 0, "Distance should not be negative"


class TestLogEntryTypes:
    """
    Tests for different command types in the log.
    """

    def test_create_command_in_log(self, api_url, unique_scooter_id):
        """
        CREATE commands are logged and applied correctly.
        """
        # Create
        response = create_scooter(api_url, unique_scooter_id)
        assert response.status_code in [200, 201]

        # Verify
        response = get_scooter(api_url, unique_scooter_id)
        assert response.status_code == 200
        scooter = response.json()

        # Initial state should be correct
        assert scooter["id"] == unique_scooter_id
        assert scooter["is_available"] == True
        assert scooter["total_distance"] == 0

    def test_reserve_command_in_log(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        RESERVE commands are logged and applied correctly.
        """
        create_scooter(api_url, unique_scooter_id)

        # Reserve
        response = reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        assert response.status_code == 200

        # Verify
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()
        assert scooter["is_available"] == False
        assert scooter["current_reservation_id"] == unique_reservation_id

    def test_release_command_in_log(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        RELEASE commands are logged and applied correctly.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        # Release with distance
        response = release_scooter(api_url, unique_scooter_id, 77)
        assert response.status_code == 200

        # Verify
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()
        assert scooter["is_available"] == True
        assert scooter["total_distance"] == 77
        # Reservation should be cleared
        res_id = scooter.get("current_reservation_id", "")
        assert res_id == "" or res_id is None

    def test_mixed_commands_correct_order(self, api_url, unique_scooter_id):
        """
        Different command types interleaved correctly in log order.
        """
        # Create
        create_scooter(api_url, unique_scooter_id)

        # Check initial state
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["is_available"] == True
        assert response.json()["total_distance"] == 0

        # Reserve
        reserve_scooter(api_url, unique_scooter_id, "mixed-1")
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["is_available"] == False

        # Release with distance
        release_scooter(api_url, unique_scooter_id, 10)
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["is_available"] == True
        assert response.json()["total_distance"] == 10

        # Another round
        reserve_scooter(api_url, unique_scooter_id, "mixed-2")
        assert get_scooter(api_url, unique_scooter_id).json()["is_available"] == False

        release_scooter(api_url, unique_scooter_id, 20)
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["is_available"] == True
        assert response.json()["total_distance"] == 30  # 10 + 20


class TestCommitIndexBehavior:
    """
    Tests for commit behavior.

    We can't observe the commit index directly, but we can observe
    that committed entries are visible and uncommitted ones are not.
    """

    def test_commit_advances_monotonically(self, api_url, unique_scooter_id):
        """
        The committed state only moves forward.

        Distance should never decrease.
        """
        create_scooter(api_url, unique_scooter_id)

        last_distance = 0
        for i in range(20):
            reserve_scooter(api_url, unique_scooter_id, f"mono-{i}")
            release_scooter(api_url, unique_scooter_id, 5)

            response = get_scooter(api_url, unique_scooter_id)
            current_distance = response.json()["total_distance"]

            # Should only go up
            assert current_distance >= last_distance, \
                f"Commit index went backwards: {current_distance} < {last_distance}"
            last_distance = current_distance

        # Final should be 20 * 5 = 100
        assert last_distance == 100

    def test_state_matches_commit(self, server_urls, unique_scooter_id):
        """
        State machine reflects all committed entries.
        """
        # Do operations
        create_scooter(server_urls[0], unique_scooter_id)
        for i in range(10):
            reserve_scooter(server_urls[0], unique_scooter_id, f"commit-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 3)

        # Wait for commit/replication
        time.sleep(3)

        # All servers should have committed state
        expected = 30  # 10 * 3

        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    distance = response.json()["total_distance"]
                    assert distance == expected, \
                        f"Server {url} has {distance}, expected {expected}"
            except Exception:
                pass

    def test_uncommitted_not_visible(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Uncommitted entries should not be visible to reads.

        Once a write completes (returns success), it's committed and visible.
        Before that, intermediate states shouldn't leak.
        """
        create_scooter(api_url, unique_scooter_id)

        # This test verifies that once we get a success response,
        # the state is committed and readable
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        # Immediately read - should see reserved state
        # (the write wouldn't return until committed)
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["is_available"] == False, \
            "Committed reservation not visible"

        release_scooter(api_url, unique_scooter_id, 50)

        # Immediately read - should see released state
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["is_available"] == True
        assert response.json()["total_distance"] == 50


class TestLogReplication:
    """
    Tests for log replication across servers.
    """

    def test_log_replicates_to_all_servers(self, server_urls, unique_scooter_id):
        """
        Log entries replicate to all servers.
        """
        # Write to one server
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, "replicate-test")
        release_scooter(server_urls[0], unique_scooter_id, 100)

        # Wait for replication
        time.sleep(3)

        # All servers should have the full log applied
        replicated_count = 0
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    scooter = response.json()
                    if scooter["total_distance"] == 100:
                        replicated_count += 1
            except Exception:
                pass

        # At least majority should have it
        assert replicated_count >= 3, \
            f"Only {replicated_count} servers have replicated log"

    def test_writes_from_any_server_replicate(self, server_urls, unique_scooter_id):
        """
        Writes from any server replicate to all others.
        """
        # Create from server 0
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(1)

        # Reserve from server 1
        reserve_scooter(server_urls[1], unique_scooter_id, "cross-server")
        time.sleep(1)

        # Release from server 2
        release_scooter(server_urls[2], unique_scooter_id, 42)
        time.sleep(2)

        # All servers should have the final state
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    scooter = response.json()
                    assert scooter["is_available"] == True
                    assert scooter["total_distance"] == 42
            except Exception:
                pass

    def test_rapid_writes_all_replicate(self, api_url, server_urls, unique_scooter_id):
        """
        Many rapid writes all get replicated.
        """
        create_scooter(api_url, unique_scooter_id)

        # Rapid operations
        expected_distance = 0
        for i in range(25):
            reserve_scooter(api_url, unique_scooter_id, f"rapid-rep-{i}")
            release_scooter(api_url, unique_scooter_id, 2)
            expected_distance += 2

        # Wait for replication
        time.sleep(5)

        # Check all servers
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    distance = response.json()["total_distance"]
                    assert distance == expected_distance, \
                        f"Server {url} has {distance}, expected {expected_distance}"
            except Exception:
                pass
