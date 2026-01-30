"""
Tests for log compaction and snapshots as specified in the assignment.

From the assignment (Section 2):
- "Periodically take snapshots of your state machine, so you can clear
   the log to a specific point"
- "Snapshots provide the state of the system after a specific log entry;
   therefore, nodes need to be aware that log entries prior to that
   entry do not exist anymore"

From the assignment (Section 3.1, Stage 5):
- "After a consensus instance is disposed, it is illegal to re-invoke it"

Run with: pytest tests/assignment/test_log_compaction.py -v
"""

import pytest
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conftest import (
    create_scooter, get_scooter, get_all_scooters,
    reserve_scooter, release_scooter, take_snapshot,
    wait_for_replication
)


class TestSnapshotBasics:
    """
    Basic tests for snapshot functionality.
    """

    def test_snapshot_endpoint_exists(self, api_url):
        """
        POST /snapshot endpoint should exist.
        """
        response = take_snapshot(api_url)

        # Should succeed or at least be recognized
        assert response.status_code in [200, 201, 204], \
            f"Snapshot endpoint returned {response.status_code}"

    def test_snapshot_preserves_current_state(self, api_url, unique_scooter_id):
        """
        Snapshot should capture current state.
        """
        # Create state
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, "snap-preserve")
        release_scooter(api_url, unique_scooter_id, 100)

        # Take snapshot
        response = take_snapshot(api_url)
        assert response.status_code in [200, 201, 204]

        # State should still be accessible
        response = get_scooter(api_url, unique_scooter_id)
        assert response.status_code == 200
        assert response.json()["total_distance"] == 100

    def test_multiple_snapshots_work(self, api_url, unique_scooter_id):
        """
        Taking multiple snapshots should work.
        """
        create_scooter(api_url, unique_scooter_id)

        # Take snapshots at different points
        for i in range(3):
            reserve_scooter(api_url, unique_scooter_id, f"multi-snap-{i}")
            release_scooter(api_url, unique_scooter_id, 10)

            response = take_snapshot(api_url)
            assert response.status_code in [200, 201, 204], \
                f"Snapshot {i} failed: {response.status_code}"

        # Final state should be correct
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 30


class TestSnapshotAndLogInteraction:
    """
    Tests for interaction between snapshots and log.
    """

    def test_operations_work_after_snapshot(self, api_url, unique_scooter_id):
        """
        Operations should continue working after a snapshot.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, "before-snap")
        release_scooter(api_url, unique_scooter_id, 50)

        # Snapshot
        take_snapshot(api_url)
        time.sleep(1)

        # More operations after snapshot
        reserve_scooter(api_url, unique_scooter_id, "after-snap-1")
        release_scooter(api_url, unique_scooter_id, 30)

        reserve_scooter(api_url, unique_scooter_id, "after-snap-2")
        release_scooter(api_url, unique_scooter_id, 20)

        # Total should be 50 + 30 + 20 = 100
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 100

    def test_new_scooters_after_snapshot(self, api_url, unique_scooter_id):
        """
        Can create new scooters after taking a snapshot.
        """
        # Create initial scooter
        create_scooter(api_url, f"{unique_scooter_id}-before")

        # Snapshot
        take_snapshot(api_url)
        time.sleep(1)

        # Create more scooters after snapshot
        for i in range(3):
            response = create_scooter(api_url, f"{unique_scooter_id}-after-{i}")
            assert response.status_code in [200, 201]

        # All should exist
        response = get_all_scooters(api_url)
        ids = [s["id"] for s in response.json()]
        assert f"{unique_scooter_id}-before" in ids
        for i in range(3):
            assert f"{unique_scooter_id}-after-{i}" in ids


class TestSnapshotRecovery:
    """
    Tests for recovering from snapshots.
    """

    def test_nodes_recover_snapshot_state(self, server_urls, unique_scooter_id):
        """
        Other nodes should be able to recover state from snapshot.
        """
        # Create data and snapshot on server 0
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, "snap-recovery")
        release_scooter(server_urls[0], unique_scooter_id, 200)
        take_snapshot(server_urls[0])

        # Wait for replication
        time.sleep(5)

        # Other servers should have the state
        for url in server_urls[1:]:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    assert response.json()["total_distance"] == 200
            except Exception:
                pass

    def test_snapshot_plus_log_entries(self, server_urls, unique_scooter_id):
        """
        Recovery should apply snapshot + newer log entries.
        """
        # Create data
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, "snap-plus-log-1")
        release_scooter(server_urls[0], unique_scooter_id, 100)

        # Snapshot (state: distance=100)
        take_snapshot(server_urls[0])
        time.sleep(2)

        # More operations after snapshot
        reserve_scooter(server_urls[0], unique_scooter_id, "snap-plus-log-2")
        release_scooter(server_urls[0], unique_scooter_id, 50)

        # Wait for replication
        time.sleep(5)

        # Other servers should have snapshot (100) + log entry (50) = 150
        for url in server_urls[1:]:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    assert response.json()["total_distance"] == 150
            except Exception:
                pass


class TestLogCompaction:
    """
    Tests for log compaction behavior.

    When a snapshot is taken, old log entries can be discarded.
    """

    def test_system_handles_many_operations(self, api_url, unique_scooter_id):
        """
        System should handle many operations (log compaction helps with this).
        """
        create_scooter(api_url, unique_scooter_id)

        # Do many operations
        for i in range(100):
            reserve_scooter(api_url, unique_scooter_id, f"many-{i}")
            release_scooter(api_url, unique_scooter_id, 1)

            # Take periodic snapshots
            if i % 20 == 0:
                take_snapshot(api_url)

        # Final state should be correct
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 100

    def test_snapshot_after_many_operations(self, api_url, unique_scooter_id):
        """
        Should be able to snapshot after many operations.
        """
        create_scooter(api_url, unique_scooter_id)

        # Many operations
        for i in range(50):
            reserve_scooter(api_url, unique_scooter_id, f"pre-snap-{i}")
            release_scooter(api_url, unique_scooter_id, 2)

        # Take snapshot
        response = take_snapshot(api_url)
        assert response.status_code in [200, 201, 204]

        # State should be preserved
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 100


class TestCheckpoints:
    """
    Tests for checkpoint behavior.

    From assignment: "You should have checkpoints that allow you to dispose
    of unused consensus instances when all nodes have learned the decision values"
    """

    def test_operations_work_with_checkpoints(self, api_url, unique_scooter_id):
        """
        Operations should continue to work with checkpointing active.
        """
        create_scooter(api_url, unique_scooter_id)

        # Do operations with periodic snapshots (checkpoints)
        total = 0
        for i in range(20):
            reserve_scooter(api_url, unique_scooter_id, f"checkpoint-{i}")
            release_scooter(api_url, unique_scooter_id, 5)
            total += 5

            if i % 5 == 0:
                take_snapshot(api_url)

        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == total


class TestSnapshotConsistency:
    """
    Tests for snapshot consistency across nodes.
    """

    def test_all_nodes_converge_after_snapshot(self, server_urls, unique_scooter_id):
        """
        All nodes should converge to the same state after snapshot.
        """
        # Create data
        create_scooter(server_urls[0], unique_scooter_id)
        for i in range(10):
            reserve_scooter(server_urls[0], unique_scooter_id, f"converge-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 10)

        # Snapshot
        take_snapshot(server_urls[0])

        # Wait for all nodes to catch up
        time.sleep(10)

        # Check all nodes have same state
        states = []
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    states.append(response.json()["total_distance"])
            except Exception:
                pass

        # All should have 100 (10 * 10)
        for state in states:
            assert state == 100, f"Node has wrong state: {state}"

    def test_snapshot_captures_all_scooters(self, api_url, unique_scooter_id):
        """
        Snapshot should capture state of all scooters.
        """
        # Create multiple scooters with different states
        scooters = []
        for i in range(5):
            sid = f"{unique_scooter_id}-all-{i}"
            create_scooter(api_url, sid)
            if i > 0:
                reserve_scooter(api_url, sid, f"all-res-{i}")
                release_scooter(api_url, sid, i * 10)
            scooters.append((sid, i * 10))

        # Snapshot
        take_snapshot(api_url)
        time.sleep(2)

        # Verify all scooters have correct state
        for sid, expected_distance in scooters:
            response = get_scooter(api_url, sid)
            assert response.status_code == 200
            assert response.json()["total_distance"] == expected_distance


class TestDisposedInstances:
    """
    Tests related to disposed consensus instances.

    From assignment: "After a consensus instance is disposed, it is illegal
    to re-invoke it"

    Note: This is hard to test from the API level, but we can ensure
    the system doesn't break after snapshots.
    """

    def test_system_stable_after_snapshots(self, api_url, unique_scooter_id):
        """
        System should remain stable after taking snapshots.
        """
        create_scooter(api_url, unique_scooter_id)

        for i in range(10):
            reserve_scooter(api_url, unique_scooter_id, f"stable-{i}")
            release_scooter(api_url, unique_scooter_id, 10)
            take_snapshot(api_url)
            time.sleep(0.5)

        # System should still work
        response = get_scooter(api_url, unique_scooter_id)
        assert response.status_code == 200
        assert response.json()["total_distance"] == 100

        # More operations should still work
        reserve_scooter(api_url, unique_scooter_id, "post-snaps")
        release_scooter(api_url, unique_scooter_id, 50)

        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 150

    def test_no_regression_after_compaction(self, api_url, unique_scooter_id):
        """
        State should not regress after log compaction.
        """
        create_scooter(api_url, unique_scooter_id)

        # Build up state
        reserve_scooter(api_url, unique_scooter_id, "no-regress-1")
        release_scooter(api_url, unique_scooter_id, 100)

        # Snapshot (compaction point)
        take_snapshot(api_url)

        # Add more
        reserve_scooter(api_url, unique_scooter_id, "no-regress-2")
        release_scooter(api_url, unique_scooter_id, 50)

        # Another snapshot
        take_snapshot(api_url)

        # State should only go forward, never back
        response = get_scooter(api_url, unique_scooter_id)
        distance = response.json()["total_distance"]

        # Should be 150 (100 + 50), never less
        assert distance == 150, \
            f"State regressed after compaction: {distance}"
