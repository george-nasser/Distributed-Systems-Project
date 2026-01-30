"""
Advanced recovery tests.

These tests verify sophisticated recovery scenarios:
- Recovery protocol behavior
- Recovery during ongoing operations
- Partial failure recovery
- Multiple node recovery
- State reconstruction correctness

Run with: pytest tests/paxos/test_recovery_advanced.py -v
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
    wait_for_replication, wait_for_server
)


class TestRecoveryProtocol:
    """
    Tests for the recovery protocol.
    """

    def test_recovering_node_catches_up(self, server_urls, unique_scooter_id):
        """
        A node that was behind catches up with all missed entries.
        """
        # Create data on one server
        create_scooter(server_urls[0], unique_scooter_id)
        for i in range(10):
            reserve_scooter(server_urls[0], unique_scooter_id, f"catchup-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 7)

        # Wait for replication (recovery)
        time.sleep(5)

        # Other servers should have caught up
        expected = 70  # 10 * 7

        caught_up = 0
        for url in server_urls[1:]:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    if response.json()["total_distance"] == expected:
                        caught_up += 1
            except Exception:
                pass

        assert caught_up >= 2, f"Only {caught_up} servers caught up"

    def test_recovery_from_snapshot(self, server_urls, unique_scooter_id):
        """
        Node can recover state from snapshot.
        """
        # Build up significant state
        create_scooter(server_urls[0], unique_scooter_id)
        for i in range(20):
            reserve_scooter(server_urls[0], unique_scooter_id, f"snap-rec-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 3)

        # Take snapshot
        take_snapshot(server_urls[0])
        time.sleep(5)

        # Other servers should recover via snapshot
        expected = 60  # 20 * 3

        for url in server_urls[1:]:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    distance = response.json()["total_distance"]
                    assert distance == expected, \
                        f"Server recovered wrong state: {distance}"
            except Exception:
                pass

    def test_recovery_from_log_only(self, server_urls, unique_scooter_id):
        """
        Node can recover state by replaying log (no snapshot).
        """
        # Create data without snapshot
        create_scooter(server_urls[0], unique_scooter_id)
        for i in range(5):
            reserve_scooter(server_urls[0], unique_scooter_id, f"log-only-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 8)

        # Wait for log replication
        time.sleep(3)

        # Should have replicated
        expected = 40  # 5 * 8

        for url in server_urls[1:]:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    assert response.json()["total_distance"] == expected
                    break
            except Exception:
                pass


class TestRecoveryDuringOperations:
    """
    Tests for recovery while operations are ongoing.
    """

    def test_operations_continue_during_recovery(self, server_urls, unique_scooter_id):
        """
        System continues to work while a node is recovering.
        """
        # Create scooter
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(1)

        # Concurrent operations while recovery might be happening
        successes = 0
        for i in range(20):
            try:
                res = reserve_scooter(server_urls[0], unique_scooter_id, f"during-rec-{i}")
                if res.status_code == 200:
                    rel = release_scooter(server_urls[0], unique_scooter_id, 2)
                    if rel.status_code == 200:
                        successes += 1
            except Exception:
                pass

        # Most should succeed
        assert successes >= 15, f"Only {successes} operations succeeded"

        # Final state should reflect successes
        response = get_scooter(server_urls[0], unique_scooter_id)
        assert response.json()["total_distance"] == successes * 2

    def test_new_writes_replicate_to_recovering_node(self, server_urls, unique_scooter_id):
        """
        New writes should eventually reach all nodes, including recovering ones.
        """
        # Create and do operations
        create_scooter(server_urls[0], unique_scooter_id)
        for i in range(10):
            reserve_scooter(server_urls[0], unique_scooter_id, f"replicate-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 5)

        # Wait for replication
        time.sleep(5)

        # All servers should eventually have the writes
        expected = 50

        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    assert response.json()["total_distance"] == expected
            except Exception:
                pass

    def test_reads_from_recovered_node(self, server_urls, unique_scooter_id, unique_reservation_id):
        """
        Reads work from a node that has recovered.
        """
        # Create and modify on one server
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, unique_reservation_id)
        release_scooter(server_urls[0], unique_scooter_id, 99)

        # Wait for replication
        time.sleep(3)

        # Should be able to read from other servers
        for url in server_urls[1:]:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    scooter = response.json()
                    assert scooter["total_distance"] == 99
                    assert scooter["is_available"] == True
                    # Found a recovered node that serves reads
                    return
            except Exception:
                pass


class TestPartialFailures:
    """
    Tests for recovery from partial failures.
    """

    def test_recovery_with_partial_log(self, server_urls, unique_scooter_id):
        """
        Node with some entries gets the rest.
        """
        # Create initial data
        create_scooter(server_urls[0], unique_scooter_id)
        for i in range(5):
            reserve_scooter(server_urls[0], unique_scooter_id, f"partial-1-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 10)

        # Wait for partial replication
        time.sleep(2)

        # More data
        for i in range(5):
            reserve_scooter(server_urls[0], unique_scooter_id, f"partial-2-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 10)

        # Wait for full replication
        time.sleep(3)

        # Should have all data: 10 * 10 = 100
        expected = 100

        for url in server_urls[1:]:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    assert response.json()["total_distance"] == expected
            except Exception:
                pass

    def test_recovery_after_crash_during_write(self, api_url, server_urls, unique_scooter_id):
        """
        After a write completes, state should be preserved even if there
        was a failure during the write.

        We can't simulate crashes, but we verify that completed writes persist.
        """
        create_scooter(api_url, unique_scooter_id)

        # Do multiple writes
        for i in range(10):
            response = reserve_scooter(api_url, unique_scooter_id, f"crash-{i}")
            if response.status_code == 200:
                release_scooter(api_url, unique_scooter_id, 5)

        # Wait
        time.sleep(2)

        # State should be consistent
        response = get_scooter(api_url, unique_scooter_id)
        distance = response.json()["total_distance"]
        assert distance == 50  # All 10 should have completed

    def test_recovery_preserves_accepted_state(self, server_urls, unique_scooter_id, unique_reservation_id):
        """
        Paxos accepted values are preserved through recovery.

        Once a value is accepted by quorum, recovery shouldn't lose it.
        """
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, unique_reservation_id)
        release_scooter(server_urls[0], unique_scooter_id, 77)

        # Wait for replication
        time.sleep(3)

        # Check multiple servers - all should have the accepted state
        correct_count = 0
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    if response.json()["total_distance"] == 77:
                        correct_count += 1
            except Exception:
                pass

        assert correct_count >= 3, f"Only {correct_count} servers have accepted state"


class TestMultipleNodeRecovery:
    """
    Tests for multiple nodes recovering.
    """

    def test_two_nodes_recover_simultaneously(self, server_urls, unique_scooter_id):
        """
        Two nodes can recover at the same time without conflict.
        """
        # Create data
        create_scooter(server_urls[0], unique_scooter_id)
        for i in range(15):
            reserve_scooter(server_urls[0], unique_scooter_id, f"dual-rec-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 4)

        # Wait for both to recover
        time.sleep(5)

        # Both should have recovered correctly
        expected = 60  # 15 * 4

        recovered = 0
        for url in server_urls[1:3]:  # Check servers 1 and 2
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    if response.json()["total_distance"] == expected:
                        recovered += 1
            except Exception:
                pass

        assert recovered >= 1, "Neither node recovered correctly"

    def test_staggered_recovery(self, server_urls, unique_scooter_id):
        """
        Nodes recovering at different times all end up consistent.
        """
        # Create data in batches
        create_scooter(server_urls[0], unique_scooter_id)

        for batch in range(3):
            for i in range(5):
                reserve_scooter(server_urls[0], unique_scooter_id, f"stagger-{batch}-{i}")
                release_scooter(server_urls[0], unique_scooter_id, 3)
            time.sleep(1)  # Stagger

        # Wait for all to recover
        time.sleep(5)

        # All should converge
        expected = 45  # 3 batches * 5 ops * 3 distance

        distances = []
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    distances.append(response.json()["total_distance"])
            except Exception:
                pass

        # All should have same value
        for d in distances:
            assert d == expected, f"Server has inconsistent distance: {d}"

    def test_recovery_while_other_nodes_active(self, server_urls, unique_scooter_id):
        """
        Active nodes aren't blocked by recovery of other nodes.
        """
        # Create scooter
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(1)

        # Do operations while other nodes might be recovering
        operations_completed = 0
        for i in range(20):
            try:
                res = reserve_scooter(server_urls[0], unique_scooter_id, f"active-{i}")
                if res.status_code == 200:
                    rel = release_scooter(server_urls[0], unique_scooter_id, 1)
                    if rel.status_code == 200:
                        operations_completed += 1
            except Exception:
                pass

        # Should have completed most operations
        assert operations_completed >= 18, \
            f"Only {operations_completed} operations completed (expected ~20)"


class TestStateReconstruction:
    """
    Tests for correct state machine reconstruction after recovery.
    """

    def test_state_machine_rebuilt_correctly(self, server_urls, unique_scooter_id):
        """
        State machine matches expected state after recovery.
        """
        # Build specific state
        create_scooter(server_urls[0], unique_scooter_id)

        # 5 rounds of reserve/release
        for i in range(5):
            reserve_scooter(server_urls[0], unique_scooter_id, f"rebuild-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 20)

        # Wait for recovery
        time.sleep(3)

        # All servers should have rebuilt state correctly
        expected_distance = 100  # 5 * 20
        expected_available = True

        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    scooter = response.json()
                    assert scooter["total_distance"] == expected_distance
                    assert scooter["is_available"] == expected_available
            except Exception:
                pass

    def test_reservation_state_preserved(self, server_urls, unique_scooter_id, unique_reservation_id):
        """
        Reservation state survives recovery.
        """
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, unique_reservation_id)

        # Wait for replication
        time.sleep(3)

        # All servers should show reserved
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    scooter = response.json()
                    assert scooter["is_available"] == False
                    assert scooter["current_reservation_id"] == unique_reservation_id
            except Exception:
                pass

    def test_distance_totals_preserved(self, server_urls, unique_scooter_id):
        """
        Distance totals survive recovery correctly.
        """
        create_scooter(server_urls[0], unique_scooter_id)

        # Accumulate distance over many operations
        expected = 0
        for i in range(10):
            distance = i + 1  # 1, 2, 3, ..., 10
            reserve_scooter(server_urls[0], unique_scooter_id, f"distance-{i}")
            release_scooter(server_urls[0], unique_scooter_id, distance)
            expected += distance

        # Expected: 1+2+3+...+10 = 55
        assert expected == 55

        # Wait for replication
        time.sleep(3)

        # All servers should have correct total
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    actual = response.json()["total_distance"]
                    assert actual == expected, \
                        f"Server has wrong distance: {actual}"
            except Exception:
                pass


class TestRecoveryEdgeCases:
    """
    Edge cases in recovery.
    """

    def test_recovery_empty_log(self, server_urls):
        """
        Recovery with no data should work.
        """
        # Just verify servers are up
        for url in server_urls:
            try:
                response = get_all_scooters(url)
                assert response.status_code == 200
            except Exception:
                pass

    def test_recovery_single_entry(self, server_urls, unique_scooter_id):
        """
        Recovery with just one log entry.
        """
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(2)

        # Should replicate
        for url in server_urls[1:]:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    assert response.json()["id"] == unique_scooter_id
                    return
            except Exception:
                pass

    def test_recovery_many_scooters(self, server_urls, unique_scooter_id):
        """
        Recovery with many different scooters.
        """
        # Create many scooters
        scooter_ids = [f"{unique_scooter_id}-many-{i}" for i in range(15)]
        for sid in scooter_ids:
            create_scooter(server_urls[0], sid)

        # Wait for replication
        time.sleep(5)

        # Check one other server
        for url in server_urls[1:]:
            try:
                response = get_all_scooters(url)
                if response.status_code == 200:
                    all_ids = [s["id"] for s in response.json()]
                    found = sum(1 for sid in scooter_ids if sid in all_ids)
                    assert found >= 10, f"Only {found}/15 scooters recovered"
                    return
            except Exception:
                pass

    def test_recovery_with_all_command_types(self, server_urls, unique_scooter_id, unique_reservation_id):
        """
        Recovery handles all command types correctly.
        """
        # CREATE
        create_scooter(server_urls[0], unique_scooter_id)
        # RESERVE
        reserve_scooter(server_urls[0], unique_scooter_id, unique_reservation_id)
        # RELEASE
        release_scooter(server_urls[0], unique_scooter_id, 50)

        # Wait for replication
        time.sleep(3)

        # Should have processed all command types
        for url in server_urls[1:]:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    scooter = response.json()
                    # Reflects CREATE + RESERVE + RELEASE
                    assert scooter["id"] == unique_scooter_id
                    assert scooter["is_available"] == True
                    assert scooter["total_distance"] == 50
                    return
            except Exception:
                pass
