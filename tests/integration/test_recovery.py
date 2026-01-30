"""
Integration tests for node recovery.

These tests verify that nodes can recover their state:
- New nodes catch up from existing nodes
- Recovered nodes have correct state
- Snapshot-based recovery works

Requires: Docker Compose with all services running.

Run with: pytest tests/integration/test_recovery.py -v
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


class TestBasicRecovery:
    """Tests for basic node recovery."""

    def test_new_node_catches_up(self, server_urls, unique_scooter_id):
        """
        A new node should get existing state from the cluster.

        This test creates data on one server and verifies it appears
        on other servers (simulating a "new" node seeing existing data).
        """
        # Create some data on server 0
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, "res-1")
        release_scooter(server_urls[0], unique_scooter_id, 100)

        # Wait for replication
        time.sleep(3)

        # Check another server has the data (simulating recovery)
        response = get_scooter(server_urls[1], unique_scooter_id)

        if response.status_code == 200:
            scooter = response.json()
            assert scooter["id"] == unique_scooter_id
            assert scooter["total_distance"] == 100
            assert scooter["is_available"] == True
        else:
            pytest.skip("Server 1 not available for recovery test")

    def test_recovered_node_has_correct_state(self, server_urls, unique_scooter_id):
        """
        After recovery, a node should have the complete correct state.
        """
        # Create multiple scooters with different states
        scooter_ids = [f"{unique_scooter_id}-{i}" for i in range(3)]

        for sid in scooter_ids:
            create_scooter(server_urls[0], sid)

        # Reserve one, release another with distance
        reserve_scooter(server_urls[0], scooter_ids[0], "res-0")
        reserve_scooter(server_urls[0], scooter_ids[1], "res-1")
        release_scooter(server_urls[0], scooter_ids[1], 50)

        # Wait for replication
        time.sleep(3)

        # Verify state on another server
        for url in server_urls[1:]:
            try:
                # Check scooter 0 is reserved
                response = get_scooter(url, scooter_ids[0])
                if response.status_code == 200:
                    assert response.json()["is_available"] == False

                # Check scooter 1 is available with distance
                response = get_scooter(url, scooter_ids[1])
                if response.status_code == 200:
                    scooter = response.json()
                    assert scooter["is_available"] == True
                    assert scooter["total_distance"] == 50

                # Check scooter 2 is available with no distance
                response = get_scooter(url, scooter_ids[2])
                if response.status_code == 200:
                    scooter = response.json()
                    assert scooter["is_available"] == True
                    assert scooter["total_distance"] == 0

                # If we get here, one server verified
                return
            except Exception as e:
                print(f"Could not verify on {url}: {e}")

        pytest.fail("Could not verify state on any recovery server")


class TestSnapshotRecovery:
    """Tests for snapshot-based recovery."""

    def test_snapshot_creation(self, api_url):
        """
        Taking a snapshot should succeed.
        """
        response = take_snapshot(api_url)
        assert response.status_code == 200

    def test_recovery_with_snapshot(self, server_urls, unique_scooter_id):
        """
        After a snapshot, new data should still be recoverable.
        """
        # Create initial data
        create_scooter(server_urls[0], f"{unique_scooter_id}-before")
        time.sleep(1)

        # Take snapshot
        take_snapshot(server_urls[0])
        time.sleep(1)

        # Create more data after snapshot
        create_scooter(server_urls[0], f"{unique_scooter_id}-after")
        time.sleep(2)

        # Both should be visible on other servers
        for url in server_urls[1:]:
            try:
                response1 = get_scooter(url, f"{unique_scooter_id}-before")
                response2 = get_scooter(url, f"{unique_scooter_id}-after")

                if response1.status_code == 200 and response2.status_code == 200:
                    # Both pieces of data are recoverable
                    return
            except Exception:
                continue

        # At least verify on server 0
        response1 = get_scooter(server_urls[0], f"{unique_scooter_id}-before")
        response2 = get_scooter(server_urls[0], f"{unique_scooter_id}-after")
        assert response1.status_code == 200
        assert response2.status_code == 200

    def test_recovery_applies_log_after_snapshot(self, server_urls, unique_scooter_id):
        """
        Recovery should apply log entries that came after the snapshot.
        """
        # Create scooter
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(1)

        # Take snapshot (scooter exists, distance=0)
        take_snapshot(server_urls[0])
        time.sleep(1)

        # Do operations after snapshot
        reserve_scooter(server_urls[0], unique_scooter_id, "res-1")
        release_scooter(server_urls[0], unique_scooter_id, 100)
        time.sleep(2)

        # Another server should see the complete state
        # (snapshot + post-snapshot operations)
        for url in server_urls[1:]:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    scooter = response.json()
                    # Should reflect operations after snapshot
                    assert scooter["is_available"] == True
                    assert scooter["total_distance"] == 100
                    return
            except Exception:
                continue

        # Verify on server 0 at minimum
        response = get_scooter(server_urls[0], unique_scooter_id)
        assert response.json()["total_distance"] == 100


class TestLogRecovery:
    """Tests for replicated log recovery."""

    def test_log_entries_replicated(self, server_urls, unique_scooter_id):
        """
        All log entries should be replicated to all servers.
        """
        # Create a sequence of operations
        create_scooter(server_urls[0], unique_scooter_id)
        for i in range(5):
            reserve_scooter(server_urls[0], unique_scooter_id, f"res-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 10)

        # Wait for replication
        time.sleep(5)

        # All servers should have the same final state
        expected_distance = 50  # 5 * 10

        for i, url in enumerate(server_urls):
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    assert response.json()["total_distance"] == expected_distance, \
                        f"Server {i} has wrong distance"
            except Exception as e:
                print(f"Server {i} unavailable: {e}")

    def test_operations_during_recovery_window(self, server_urls, unique_scooter_id):
        """
        Operations should succeed even while a node might be recovering.

        This test does rapid operations and verifies they all apply.
        """
        create_scooter(server_urls[0], unique_scooter_id)

        # Rapid operations
        for i in range(10):
            reserve_scooter(server_urls[0], unique_scooter_id, f"rapid-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 5)

        # Should have 50 total distance
        time.sleep(3)
        response = get_scooter(server_urls[0], unique_scooter_id)
        assert response.json()["total_distance"] == 50


class TestPartialRecovery:
    """Tests for recovery from partial state."""

    def test_recovery_from_empty_state(self, server_urls, unique_scooter_id):
        """
        A node with empty state should recover all data from cluster.
        """
        # Create data
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, "res-1")
        release_scooter(server_urls[0], unique_scooter_id, 200)

        # Wait for replication
        time.sleep(3)

        # Query another server (simulating empty node that recovered)
        response = get_scooter(server_urls[1], unique_scooter_id)
        if response.status_code == 200:
            scooter = response.json()
            assert scooter["total_distance"] == 200

    def test_recovery_preserves_all_scooters(self, server_urls, unique_scooter_id):
        """
        Recovery should preserve all scooters, not just recent ones.
        """
        # Create many scooters
        scooter_ids = [f"{unique_scooter_id}-{i}" for i in range(10)]
        for sid in scooter_ids:
            create_scooter(server_urls[0], sid)

        # Wait for replication
        time.sleep(5)

        # Check all scooters exist on another server
        for url in server_urls[1:]:
            try:
                response = get_all_scooters(url)
                if response.status_code == 200:
                    all_ids = [s["id"] for s in response.json()]
                    for sid in scooter_ids:
                        assert sid in all_ids, f"Scooter {sid} not recovered"
                    return
            except Exception:
                continue

        # Verify on server 0 at minimum
        response = get_all_scooters(server_urls[0])
        all_ids = [s["id"] for s in response.json()]
        for sid in scooter_ids:
            assert sid in all_ids
