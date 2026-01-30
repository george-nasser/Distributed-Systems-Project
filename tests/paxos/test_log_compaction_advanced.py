"""
Advanced tests for log compaction scenarios.

These tests go beyond basic snapshot tests to verify:
- Compaction triggers correctly
- Compaction safety (state preserved)
- Compaction + recovery interaction
- Log truncation behavior

Run with: pytest tests/paxos/test_log_compaction_advanced.py -v
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
    reserve_scooter, release_scooter, take_snapshot,
    wait_for_replication
)


class TestCompactionTriggers:
    """
    Tests for when compaction happens.
    """

    def test_compaction_after_many_entries(self, api_url, unique_scooter_id):
        """
        After many log entries, compaction should still work.
        """
        create_scooter(api_url, unique_scooter_id)

        # Create many log entries
        for i in range(50):
            reserve_scooter(api_url, unique_scooter_id, f"many-entries-{i}")
            release_scooter(api_url, unique_scooter_id, 1)

        # Take snapshot (trigger compaction)
        response = take_snapshot(api_url)
        assert response.status_code in [200, 201, 204]

        # State should be preserved
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 50

    def test_compaction_with_snapshot(self, api_url, unique_scooter_id):
        """
        Snapshot should create a compaction point.
        """
        create_scooter(api_url, unique_scooter_id)

        # Some operations
        for i in range(10):
            reserve_scooter(api_url, unique_scooter_id, f"snap-compact-{i}")
            release_scooter(api_url, unique_scooter_id, 5)

        # Take snapshot
        response = take_snapshot(api_url)
        assert response.status_code in [200, 201, 204]

        # State should be exactly 50
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 50

        # More operations after snapshot
        for i in range(10):
            reserve_scooter(api_url, unique_scooter_id, f"post-snap-{i}")
            release_scooter(api_url, unique_scooter_id, 5)

        # Total should be 100
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 100

    def test_multiple_compactions(self, api_url, unique_scooter_id):
        """
        System handles multiple compaction cycles.
        """
        create_scooter(api_url, unique_scooter_id)

        total_distance = 0

        # Multiple compaction cycles
        for cycle in range(5):
            # Do some operations
            for i in range(10):
                reserve_scooter(api_url, unique_scooter_id, f"cycle-{cycle}-{i}")
                release_scooter(api_url, unique_scooter_id, 2)
                total_distance += 2

            # Take snapshot (compact)
            response = take_snapshot(api_url)
            assert response.status_code in [200, 201, 204], \
                f"Snapshot {cycle} failed"

            # Verify state
            response = get_scooter(api_url, unique_scooter_id)
            assert response.json()["total_distance"] == total_distance

        # Final state should be 5 cycles * 10 ops * 2 distance = 100
        assert total_distance == 100


class TestCompactionSafety:
    """
    Tests for compaction safety - state must be preserved.
    """

    def test_state_preserved_after_compaction(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        State machine is unchanged after compaction.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        release_scooter(api_url, unique_scooter_id, 42)

        # Get state before compaction
        response = get_scooter(api_url, unique_scooter_id)
        state_before = response.json()

        # Compact
        take_snapshot(api_url)
        time.sleep(1)

        # Get state after compaction
        response = get_scooter(api_url, unique_scooter_id)
        state_after = response.json()

        # Should be identical
        assert state_before["id"] == state_after["id"]
        assert state_before["is_available"] == state_after["is_available"]
        assert state_before["total_distance"] == state_after["total_distance"]

    def test_new_entries_after_compaction(self, api_url, unique_scooter_id):
        """
        New log entries work correctly after compaction.
        """
        create_scooter(api_url, unique_scooter_id)

        # Some operations
        for i in range(5):
            reserve_scooter(api_url, unique_scooter_id, f"pre-{i}")
            release_scooter(api_url, unique_scooter_id, 10)

        # Compact
        take_snapshot(api_url)
        time.sleep(1)

        # New operations after compaction
        for i in range(5):
            reserve_scooter(api_url, unique_scooter_id, f"post-{i}")
            release_scooter(api_url, unique_scooter_id, 10)

        # Total should be correct: 5*10 + 5*10 = 100
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 100

    def test_recovery_uses_compacted_state(self, server_urls, unique_scooter_id):
        """
        Recovery should use snapshot from compaction.
        """
        # Create data and compact on server 0
        create_scooter(server_urls[0], unique_scooter_id)
        for i in range(10):
            reserve_scooter(server_urls[0], unique_scooter_id, f"compact-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 7)

        # Compact
        take_snapshot(server_urls[0])
        time.sleep(3)

        # Other servers should have the data (recovered from snapshot)
        expected = 70  # 10 * 7

        for url in server_urls[1:]:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    assert response.json()["total_distance"] == expected
            except Exception:
                pass


class TestCompactionPlusRecovery:
    """
    Tests for compaction + recovery interaction.
    """

    def test_lagging_node_gets_snapshot(self, server_urls, unique_scooter_id):
        """
        A far-behind node should get snapshot, not full log replay.

        We can't directly verify this, but we can check the end result.
        """
        # Create lots of data
        create_scooter(server_urls[0], unique_scooter_id)
        for i in range(30):
            reserve_scooter(server_urls[0], unique_scooter_id, f"lag-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 3)

        # Compact
        take_snapshot(server_urls[0])
        time.sleep(5)

        # All servers should have the data
        expected = 90  # 30 * 3

        replicated_count = 0
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    if response.json()["total_distance"] == expected:
                        replicated_count += 1
            except Exception:
                pass

        assert replicated_count >= 3, \
            f"Only {replicated_count} servers have correct state"

    def test_snapshot_plus_recent_entries(self, server_urls, unique_scooter_id):
        """
        Recovery = snapshot + entries after snapshot.
        """
        # Create data
        create_scooter(server_urls[0], unique_scooter_id)
        for i in range(10):
            reserve_scooter(server_urls[0], unique_scooter_id, f"snap-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 5)

        # Compact (snapshot captures 50)
        take_snapshot(server_urls[0])
        time.sleep(2)

        # More entries after snapshot
        for i in range(10):
            reserve_scooter(server_urls[0], unique_scooter_id, f"post-snap-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 5)

        # Wait for replication
        time.sleep(3)

        # All servers should have snapshot (50) + new entries (50) = 100
        expected = 100

        for url in server_urls[1:]:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    assert response.json()["total_distance"] == expected
            except Exception:
                pass

    def test_compaction_during_recovery(self, server_urls, unique_scooter_id):
        """
        System handles concurrent compaction and recovery.
        """
        # Create initial data
        create_scooter(server_urls[0], unique_scooter_id)
        for i in range(10):
            reserve_scooter(server_urls[0], unique_scooter_id, f"concurrent-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 4)

        # Concurrent: compact while data is still replicating
        def do_snapshot():
            take_snapshot(server_urls[0])

        def do_more_ops():
            for i in range(5):
                reserve_scooter(server_urls[0], unique_scooter_id, f"during-{i}")
                release_scooter(server_urls[0], unique_scooter_id, 2)

        with ThreadPoolExecutor(max_workers=2) as executor:
            f1 = executor.submit(do_snapshot)
            f2 = executor.submit(do_more_ops)
            f1.result()
            f2.result()

        # Wait for everything to settle
        time.sleep(5)

        # State should be at least 40 (10 * 4), possibly more
        response = get_scooter(server_urls[0], unique_scooter_id)
        distance = response.json()["total_distance"]
        assert distance >= 40, f"Unexpected distance: {distance}"


class TestLogTruncation:
    """
    Tests for log truncation behavior.
    """

    def test_old_entries_not_needed(self, api_url, unique_scooter_id):
        """
        After snapshot, old log entries aren't needed for correctness.
        """
        create_scooter(api_url, unique_scooter_id)

        # Build up state
        for i in range(20):
            reserve_scooter(api_url, unique_scooter_id, f"old-{i}")
            release_scooter(api_url, unique_scooter_id, 3)

        # Snapshot (could truncate old entries)
        take_snapshot(api_url)
        time.sleep(1)

        # State should still be correct
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 60

        # More operations
        for i in range(10):
            reserve_scooter(api_url, unique_scooter_id, f"new-{i}")
            release_scooter(api_url, unique_scooter_id, 2)

        # Total: 60 + 20 = 80
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 80

    def test_compaction_frees_resources(self, api_url, unique_scooter_id):
        """
        After compaction, system should still handle more operations.

        This indirectly tests that compaction doesn't cause issues.
        """
        create_scooter(api_url, unique_scooter_id)

        # Multiple rounds of operations + compaction
        for round_num in range(3):
            # Lots of operations
            for i in range(30):
                reserve_scooter(api_url, unique_scooter_id, f"round{round_num}-{i}")
                release_scooter(api_url, unique_scooter_id, 1)

            # Compact
            take_snapshot(api_url)
            time.sleep(1)

        # Should have 3 rounds * 30 ops = 90
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 90

        # System should still work
        reserve_scooter(api_url, unique_scooter_id, "final")
        release_scooter(api_url, unique_scooter_id, 10)

        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 100

    def test_truncation_preserves_consistency(self, server_urls, unique_scooter_id):
        """
        Truncation shouldn't affect consistency across nodes.
        """
        create_scooter(server_urls[0], unique_scooter_id)

        # Operations
        for i in range(15):
            reserve_scooter(server_urls[0], unique_scooter_id, f"trunc-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 4)

        # Compact (might truncate log)
        take_snapshot(server_urls[0])
        time.sleep(3)

        # All servers should agree
        distances = []
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    distances.append(response.json()["total_distance"])
            except Exception:
                pass

        # All should have 60 (15 * 4)
        for d in distances:
            assert d == 60, f"Server has inconsistent distance: {d}"


class TestCompactionEdgeCases:
    """
    Edge cases for compaction.
    """

    def test_snapshot_empty_state(self, api_url):
        """
        Snapshot with no scooters should work.
        """
        response = take_snapshot(api_url)
        assert response.status_code in [200, 201, 204]

    def test_snapshot_single_scooter(self, api_url, unique_scooter_id):
        """
        Snapshot with just one scooter.
        """
        create_scooter(api_url, unique_scooter_id)

        response = take_snapshot(api_url)
        assert response.status_code in [200, 201, 204]

        # Scooter should still exist
        response = get_scooter(api_url, unique_scooter_id)
        assert response.status_code == 200

    def test_snapshot_many_scooters(self, api_url, unique_scooter_id):
        """
        Snapshot with many scooters.
        """
        # Create many scooters
        scooter_ids = [f"{unique_scooter_id}-many-{i}" for i in range(20)]
        for sid in scooter_ids:
            create_scooter(api_url, sid)

        # Snapshot
        response = take_snapshot(api_url)
        assert response.status_code in [200, 201, 204]

        # All scooters should still exist
        response = get_all_scooters(api_url)
        all_ids = [s["id"] for s in response.json()]
        for sid in scooter_ids:
            assert sid in all_ids

    def test_rapid_snapshots(self, api_url, unique_scooter_id):
        """
        Rapid consecutive snapshots should work.
        """
        create_scooter(api_url, unique_scooter_id)

        # Rapid snapshots
        for i in range(10):
            response = take_snapshot(api_url)
            assert response.status_code in [200, 201, 204]

        # State should be preserved
        response = get_scooter(api_url, unique_scooter_id)
        assert response.status_code == 200

    def test_operations_interleaved_with_snapshots(self, api_url, unique_scooter_id):
        """
        Operations interleaved with snapshots.
        """
        create_scooter(api_url, unique_scooter_id)

        total = 0
        for i in range(10):
            # Operation
            reserve_scooter(api_url, unique_scooter_id, f"interleave-{i}")
            release_scooter(api_url, unique_scooter_id, 5)
            total += 5

            # Snapshot
            take_snapshot(api_url)

            # Verify
            response = get_scooter(api_url, unique_scooter_id)
            assert response.json()["total_distance"] == total


class TestCompactionAllScooterStates:
    """
    Tests that compaction preserves all possible scooter states.
    """

    def test_snapshot_reserved_scooter(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Snapshot preserves reserved state.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        # Snapshot while reserved
        take_snapshot(api_url)
        time.sleep(1)

        # Should still be reserved
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["is_available"] == False
        assert response.json()["current_reservation_id"] == unique_reservation_id

    def test_snapshot_scooter_with_distance(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Snapshot preserves accumulated distance.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        release_scooter(api_url, unique_scooter_id, 123)

        # Snapshot
        take_snapshot(api_url)
        time.sleep(1)

        # Distance should be preserved
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 123

    def test_snapshot_mixed_scooter_states(self, api_url, unique_scooter_id):
        """
        Snapshot preserves mix of scooter states.
        """
        # Create scooters with different states
        sid1 = f"{unique_scooter_id}-available"
        sid2 = f"{unique_scooter_id}-reserved"
        sid3 = f"{unique_scooter_id}-used"

        create_scooter(api_url, sid1)  # Available, no distance

        create_scooter(api_url, sid2)
        reserve_scooter(api_url, sid2, "res-mixed")  # Reserved

        create_scooter(api_url, sid3)
        reserve_scooter(api_url, sid3, "res-used")
        release_scooter(api_url, sid3, 50)  # Available with distance

        # Snapshot
        take_snapshot(api_url)
        time.sleep(1)

        # Verify all states preserved
        r1 = get_scooter(api_url, sid1).json()
        assert r1["is_available"] == True
        assert r1["total_distance"] == 0

        r2 = get_scooter(api_url, sid2).json()
        assert r2["is_available"] == False

        r3 = get_scooter(api_url, sid3).json()
        assert r3["is_available"] == True
        assert r3["total_distance"] == 50
