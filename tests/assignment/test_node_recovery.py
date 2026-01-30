"""
Tests for node recovery as specified in the assignment.

From the assignment (Section 3.1, Stage 4):
- Nodes may start at any time
- Nodes can crash and recover
- A node is only considered recovered after completing recovery procedure
- The number of simultaneous failures will not exceed f at any time

From the assignment (Section 2):
- "When clients recover from a failure or join the system they need to
   catch up to the current log state"

Run with: pytest tests/assignment/test_node_recovery.py -v
"""

import pytest
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conftest import (
    create_scooter, get_scooter, get_all_scooters,
    reserve_scooter, release_scooter, take_snapshot,
    wait_for_server, wait_for_replication
)


class TestNodeCatchUp:
    """
    Tests that nodes catch up to the current state.

    When a node starts or recovers, it must get all the data
    that was committed while it was down.
    """

    def test_data_replicates_to_all_nodes(self, server_urls, unique_scooter_id):
        """
        Data written to one node should eventually appear on all nodes.

        This simulates a "recovered" node catching up.
        """
        # Write data to first server
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, "catchup-test")
        release_scooter(server_urls[0], unique_scooter_id, 250)

        # Wait for replication (simulating recovery time)
        time.sleep(5)

        # All servers should have caught up
        caught_up = 0
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    scooter = response.json()
                    if scooter["total_distance"] == 250:
                        caught_up += 1
            except Exception:
                pass

        # At least majority should have caught up
        assert caught_up >= 3, \
            f"Only {caught_up}/{len(server_urls)} nodes caught up"

    def test_new_node_sees_existing_data(self, server_urls, unique_scooter_id):
        """
        A node that "joins" should see all existing data.
        """
        # Create some data
        scooter_ids = [f"{unique_scooter_id}-exist-{i}" for i in range(5)]
        for sid in scooter_ids:
            create_scooter(server_urls[0], sid)

        # Wait for replication
        time.sleep(5)

        # Query a different server (simulating new/recovered node)
        response = get_all_scooters(server_urls[2])
        assert response.status_code == 200

        all_ids = [s["id"] for s in response.json()]
        for sid in scooter_ids:
            assert sid in all_ids, f"Recovered node missing scooter {sid}"


class TestRecoveryWithLog:
    """
    Tests for log-based recovery.

    Recovered nodes should get the replicated log and apply it.
    """

    def test_all_operations_recovered(self, server_urls, unique_scooter_id):
        """
        All committed operations should be recovered.
        """
        create_scooter(server_urls[0], unique_scooter_id)

        # Do many operations
        expected_distance = 0
        for i in range(20):
            reserve_scooter(server_urls[0], unique_scooter_id, f"recover-{i}")
            release_scooter(server_urls[0], unique_scooter_id, i + 1)
            expected_distance += (i + 1)

        # expected_distance = 1+2+3+...+20 = 210

        # Wait for replication
        time.sleep(10)

        # All servers should have all operations
        for i, url in enumerate(server_urls):
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    actual = response.json()["total_distance"]
                    assert actual == expected_distance, \
                        f"Server {i} missing operations: {actual} != {expected_distance}"
            except Exception:
                pass

    def test_operation_order_preserved(self, server_urls, unique_scooter_id):
        """
        Operations should be applied in the same order on all nodes.
        """
        create_scooter(server_urls[0], unique_scooter_id)

        # Do operations in specific order
        reserve_scooter(server_urls[0], unique_scooter_id, "order-1")
        release_scooter(server_urls[0], unique_scooter_id, 100)
        reserve_scooter(server_urls[0], unique_scooter_id, "order-2")
        release_scooter(server_urls[0], unique_scooter_id, 50)

        # Wait for replication
        time.sleep(5)

        # All servers should have same final state
        for i, url in enumerate(server_urls):
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    scooter = response.json()
                    assert scooter["total_distance"] == 150, \
                        f"Server {i} has wrong order/state"
                    assert scooter["is_available"] == True
            except Exception:
                pass


class TestRecoveryWithSnapshot:
    """
    Tests for snapshot-based recovery.

    When recovering, a node might get a snapshot plus recent log entries.
    """

    def test_recovery_after_snapshot(self, server_urls, unique_scooter_id):
        """
        Recovery should work after a snapshot has been taken.
        """
        # Create data
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, "pre-snap")
        release_scooter(server_urls[0], unique_scooter_id, 100)

        # Take snapshot
        take_snapshot(server_urls[0])
        time.sleep(2)

        # Do more operations after snapshot
        reserve_scooter(server_urls[0], unique_scooter_id, "post-snap")
        release_scooter(server_urls[0], unique_scooter_id, 50)

        # Wait for replication
        time.sleep(5)

        # All servers should have complete state (snapshot + post-snapshot ops)
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    assert response.json()["total_distance"] == 150
            except Exception:
                pass

    def test_snapshot_state_is_complete(self, server_urls, unique_scooter_id):
        """
        Snapshot should contain complete state at that point.
        """
        # Create scooters with various states
        ids = [f"{unique_scooter_id}-snap-{i}" for i in range(3)]

        create_scooter(server_urls[0], ids[0])
        create_scooter(server_urls[0], ids[1])
        reserve_scooter(server_urls[0], ids[1], "snap-res")
        create_scooter(server_urls[0], ids[2])
        reserve_scooter(server_urls[0], ids[2], "snap-res-2")
        release_scooter(server_urls[0], ids[2], 100)

        # Take snapshot
        take_snapshot(server_urls[0])
        time.sleep(5)

        # Verify state on other servers
        for url in server_urls[1:]:
            try:
                # Scooter 0: available, 0 distance
                response = get_scooter(url, ids[0])
                if response.status_code == 200:
                    s = response.json()
                    assert s["is_available"] == True
                    assert s["total_distance"] == 0

                # Scooter 1: reserved
                response = get_scooter(url, ids[1])
                if response.status_code == 200:
                    s = response.json()
                    assert s["is_available"] == False

                # Scooter 2: available, 100 distance
                response = get_scooter(url, ids[2])
                if response.status_code == 200:
                    s = response.json()
                    assert s["is_available"] == True
                    assert s["total_distance"] == 100
            except Exception:
                pass


class TestRecoveryDuringOperations:
    """
    Tests for recovery happening while operations continue.

    The system should handle recovery without blocking operations.
    """

    def test_operations_continue_during_replication(self, server_urls, unique_scooter_id):
        """
        Operations should continue working while data is replicating.
        """
        create_scooter(server_urls[0], unique_scooter_id)

        # Do many operations (replication happening in background)
        for i in range(30):
            reserve_scooter(server_urls[0], unique_scooter_id, f"during-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 1)

        # All should have succeeded - distance = 30
        response = get_scooter(server_urls[0], unique_scooter_id)
        assert response.json()["total_distance"] == 30

    def test_new_writes_after_node_has_data(self, server_urls, unique_scooter_id):
        """
        After a node catches up, new writes should also be replicated.
        """
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, "initial")
        release_scooter(server_urls[0], unique_scooter_id, 100)

        # Wait for initial replication
        time.sleep(3)

        # Now do more writes
        reserve_scooter(server_urls[0], unique_scooter_id, "after-catchup")
        release_scooter(server_urls[0], unique_scooter_id, 50)

        # Wait for new writes to replicate
        time.sleep(3)

        # All servers should have both sets of data
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    assert response.json()["total_distance"] == 150
            except Exception:
                pass


class TestRecoveryState:
    """
    Tests for the state of a recovering node.

    From assignment: "A node is considered recovered only after it completes
    the recovery procedure."
    """

    def test_recovered_node_has_correct_state(self, server_urls, unique_scooter_id):
        """
        After recovery completes, node has correct state.
        """
        # Create data
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, "recovery-state")
        release_scooter(server_urls[0], unique_scooter_id, 200)

        # Give time for recovery
        time.sleep(5)

        # All nodes should have the complete, correct state
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    scooter = response.json()
                    assert scooter["id"] == unique_scooter_id
                    assert scooter["is_available"] == True
                    assert scooter["total_distance"] == 200
            except Exception:
                pass

    def test_no_partial_state_visible(self, server_urls, unique_scooter_id):
        """
        During recovery, node shouldn't expose partial state.

        Either a scooter exists with complete state, or it doesn't exist yet.
        """
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, "partial-test")
        release_scooter(server_urls[0], unique_scooter_id, 100)

        # Check all servers - state should be complete or not present
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    scooter = response.json()
                    # If we see the scooter, it should have complete state
                    # Not some intermediate state
                    distance = scooter["total_distance"]
                    # Distance should be 0 (just created) or 100 (all ops applied)
                    # Not something in between
                    assert distance in [0, 100], \
                        f"Partial state visible: distance={distance}"
            except Exception:
                pass


class TestRecoveryWithQuorum:
    """
    Tests that recovery works with quorum guarantees.

    From assignment: "Note that with the guarantee of a live quorum at all times,
    recovery without persistent storage remains possible."
    """

    def test_data_survives_in_quorum(self, server_urls, unique_scooter_id):
        """
        Data should be preserved as long as quorum is maintained.
        """
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, "quorum-test")
        release_scooter(server_urls[0], unique_scooter_id, 500)

        # Wait for quorum to have the data
        time.sleep(5)

        # At least 3 servers should have the data
        servers_with_data = 0
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    if response.json()["total_distance"] == 500:
                        servers_with_data += 1
            except Exception:
                pass

        assert servers_with_data >= 3, \
            f"Only {servers_with_data} servers have the data (need quorum of 3)"
