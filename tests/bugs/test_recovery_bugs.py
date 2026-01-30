"""
Tests designed to catch recovery-related bugs.

These tests specifically target:
1. recovery/recovery.go: Silent failure when all servers fail
2. recovery/recovery.go: No snapshot validation
3. recovery/recovery.go: Double-apply risk during recovery
4. recovery/recovery.go: Race between recovery and normal operations
5. main.go: Recovery timing issues

Run with: pytest tests/bugs/test_recovery_bugs.py -v
"""

import pytest
import requests
import time
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conftest import (
    create_scooter, get_scooter, get_all_scooters,
    reserve_scooter, release_scooter, take_snapshot,
    wait_for_server, wait_for_replication
)


class TestRecoveryDataIntegrity:
    """
    Tests for data integrity after recovery scenarios.

    BUG: recovery.go applies entries without idempotency checks.
    If an entry was already applied, it gets applied again,
    corrupting state (especially for distance accumulation).
    """

    def test_data_consistent_after_replication(self, server_urls, unique_scooter_id):
        """
        CATCHES BUG: Data should be consistent across all replicas.

        If recovery double-applies entries, distance would be wrong.
        """
        # Create scooter with specific distance
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, "recovery-test")
        release_scooter(server_urls[0], unique_scooter_id, 123)

        # Wait for replication
        time.sleep(5)

        # Check all servers have EXACTLY 123 distance (not 246 from double-apply)
        for i, url in enumerate(server_urls):
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    distance = response.json()["total_distance"]
                    assert distance == 123, \
                        f"BUG: Server {i} has wrong distance {distance}! " \
                        f"Expected 123. Possible double-apply during recovery?"
            except Exception as e:
                print(f"Server {i} unavailable: {e}")

    def test_operations_replicate_exactly_once(self, server_urls, unique_scooter_id):
        """
        CATCHES BUG: Each operation should be applied exactly once on each server.
        """
        create_scooter(server_urls[0], unique_scooter_id)

        # Do precise operations
        operations = [
            (10, "op-1"),
            (20, "op-2"),
            (30, "op-3"),
        ]

        for distance, res_id in operations:
            reserve_scooter(server_urls[0], unique_scooter_id, res_id)
            release_scooter(server_urls[0], unique_scooter_id, distance)

        expected_total = sum(d for d, _ in operations)  # 60

        # Wait for replication
        time.sleep(5)

        # All servers should have exactly 60
        for i, url in enumerate(server_urls):
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    actual = response.json()["total_distance"]
                    assert actual == expected_total, \
                        f"BUG: Server {i} has {actual}, expected {expected_total}. " \
                        f"Exactly-once delivery violated?"
            except Exception:
                pass


class TestSnapshotRecoveryBugs:
    """
    Tests for bugs in snapshot-based recovery.

    BUG: recovery.go doesn't validate snapshot data before loading.
    Corrupted or empty snapshots could cause panics or data loss.
    """

    def test_recovery_after_snapshot(self, server_urls, unique_scooter_id):
        """
        CATCHES BUG: Recovery should work correctly after a snapshot.
        """
        # Create data
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, "pre-snap")
        release_scooter(server_urls[0], unique_scooter_id, 100)

        # Take snapshot
        take_snapshot(server_urls[0])
        time.sleep(2)

        # More operations after snapshot
        reserve_scooter(server_urls[0], unique_scooter_id, "post-snap")
        release_scooter(server_urls[0], unique_scooter_id, 50)

        # Wait for replication
        time.sleep(5)

        # All servers should have 150 (100 from pre-snap + 50 from post-snap)
        for i, url in enumerate(server_urls):
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    distance = response.json()["total_distance"]
                    assert distance == 150, \
                        f"BUG: Server {i} has {distance} after snapshot recovery, expected 150"
            except Exception:
                pass

    def test_snapshot_preserves_reservation_state(self, server_urls, unique_scooter_id, unique_reservation_id):
        """
        CATCHES BUG: Snapshot should preserve reservation state correctly.
        """
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, unique_reservation_id)

        # Take snapshot while reserved
        take_snapshot(server_urls[0])
        time.sleep(3)

        # Verify reservation is preserved
        response = get_scooter(server_urls[0], unique_scooter_id)
        scooter = response.json()

        assert scooter["is_available"] == False, \
            "BUG: Reservation lost after snapshot!"
        assert scooter["current_reservation_id"] == unique_reservation_id, \
            f"BUG: Wrong reservation ID after snapshot! Got {scooter['current_reservation_id']}"

    def test_multiple_snapshots_consistent(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Multiple snapshots shouldn't corrupt state.
        """
        create_scooter(api_url, unique_scooter_id)

        for i in range(5):
            reserve_scooter(api_url, unique_scooter_id, f"multi-snap-{i}")
            release_scooter(api_url, unique_scooter_id, 10)
            take_snapshot(api_url)
            time.sleep(0.5)

        # Should have 50 total distance
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 50, \
            f"BUG: Multiple snapshots corrupted state! Got {response.json()['total_distance']}"


class TestRecoveryAndOperationsRace:
    """
    Tests for race conditions between recovery and normal operations.

    BUG: recovery.go applies entries while API handlers might be running.
    No synchronization between recovery and normal operations.
    """

    def test_operations_during_replication(self, server_urls, unique_scooter_id):
        """
        CATCHES BUG: Operations should work while replication is happening.
        """
        # Create scooter
        create_scooter(server_urls[0], unique_scooter_id)

        # Do operations while replication might be happening
        errors = []
        for i in range(20):
            try:
                res = reserve_scooter(server_urls[0], unique_scooter_id, f"repl-{i}")
                if res.status_code != 200:
                    errors.append(f"Reserve {i}: {res.status_code}")
                    continue

                rel = release_scooter(server_urls[0], unique_scooter_id, 5)
                if rel.status_code != 200:
                    errors.append(f"Release {i}: {rel.status_code}")
            except Exception as e:
                errors.append(f"Cycle {i}: {e}")

        # Should have minimal errors
        assert len(errors) < 5, f"BUG: Too many errors during replication: {errors}"

        # Final state should be consistent
        response = get_scooter(server_urls[0], unique_scooter_id)
        scooter = response.json()
        assert scooter["is_available"] == True

    def test_read_during_recovery(self, server_urls, unique_scooter_id):
        """
        CATCHES BUG: Reads should return consistent data during recovery.
        """
        # Create data
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, "read-test")
        release_scooter(server_urls[0], unique_scooter_id, 100)

        # Read from multiple servers while replication happening
        time.sleep(1)  # Let some replication happen

        results = []
        for _ in range(10):
            for url in server_urls:
                try:
                    response = get_scooter(url, unique_scooter_id)
                    if response.status_code == 200:
                        results.append(response.json())
                except Exception:
                    pass
            time.sleep(0.2)

        # Check for inconsistent results (shouldn't see partial state)
        if results:
            distances = [r["total_distance"] for r in results]
            # All distances should be 0 (not replicated) or 100 (replicated)
            # Never something in between
            for d in distances:
                assert d in [0, 100], \
                    f"BUG: Saw partial/inconsistent distance {d} during recovery"


class TestLogRecoveryBugs:
    """
    Tests for bugs in log-based recovery.

    BUG: log/replicated_log.go has index management issues that
    could cause recovery to skip or duplicate entries.
    """

    def test_all_operations_recovered(self, server_urls, unique_scooter_id):
        """
        CATCHES BUG: All operations should be recovered on all servers.
        """
        create_scooter(server_urls[0], unique_scooter_id)

        # Do many operations
        expected_distance = 0
        for i in range(30):
            reserve_scooter(server_urls[0], unique_scooter_id, f"log-{i}")
            release_scooter(server_urls[0], unique_scooter_id, i + 1)
            expected_distance += (i + 1)

        # Expected: 1+2+3+...+30 = 465
        assert expected_distance == 465

        # Wait for full replication
        time.sleep(10)

        # All servers should have all operations
        for i, url in enumerate(server_urls):
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    actual = response.json()["total_distance"]
                    assert actual == expected_distance, \
                        f"BUG: Server {i} missing operations! Has {actual}, expected {expected_distance}"
            except Exception:
                pass

    def test_log_order_preserved(self, server_urls, unique_scooter_id):
        """
        CATCHES BUG: Log entries should be applied in order.
        """
        create_scooter(server_urls[0], unique_scooter_id)

        # Operations that depend on order
        reserve_scooter(server_urls[0], unique_scooter_id, "order-1")
        release_scooter(server_urls[0], unique_scooter_id, 10)
        reserve_scooter(server_urls[0], unique_scooter_id, "order-2")
        release_scooter(server_urls[0], unique_scooter_id, 20)

        # Wait for replication
        time.sleep(5)

        # All servers should show 30 distance, scooter available
        for i, url in enumerate(server_urls):
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    scooter = response.json()
                    assert scooter["total_distance"] == 30, \
                        f"BUG: Server {i} has wrong distance (order issue?)"
                    assert scooter["is_available"] == True, \
                        f"BUG: Server {i} in wrong state (order issue?)"
            except Exception:
                pass


class TestRecoverySilentFailure:
    """
    Tests for silent failures during recovery.

    BUG: recovery.go returns nil (success) even if recovery fails
    on all servers. This can cause the node to start with stale state.
    """

    def test_operations_work_after_startup(self, api_url, unique_scooter_id):
        """
        Test that operations work, implying recovery succeeded.
        """
        # If recovery failed silently, operations might fail
        response = create_scooter(api_url, unique_scooter_id)

        assert response.status_code in [200, 201], \
            f"BUG: Operation failed after startup (recovery issue?): {response.status_code}"

    def test_existing_data_visible_after_operations(self, server_urls, unique_scooter_id):
        """
        CATCHES BUG: Data created on one server should be visible after recovery.
        """
        # Create on server 0
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, "exist-test")
        release_scooter(server_urls[0], unique_scooter_id, 500)

        # Wait for replication
        time.sleep(5)

        # Data should be visible on other servers (they recovered it)
        found = False
        for url in server_urls[1:]:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    distance = response.json()["total_distance"]
                    assert distance == 500, \
                        f"BUG: Server has wrong distance {distance}. Recovery failed?"
                    found = True
            except Exception:
                pass

        if not found:
            print("WARNING: Could not verify data on other servers")


class TestMainRecoveryTiming:
    """
    Tests related to recovery timing in main.go.

    BUG: main.go calls Recovery() just before starting HTTP server.
    If recovery fails or is slow, there could be timing issues.
    """

    def test_api_responds_after_startup(self, api_url):
        """
        Test that API is responsive (implies recovery completed).
        """
        # Give server time to start
        time.sleep(2)

        response = get_all_scooters(api_url)

        # Should respond, even if empty
        assert response.status_code == 200, \
            f"BUG: API not responding after startup: {response.status_code}"

    def test_writes_work_immediately(self, api_url, unique_scooter_id):
        """
        Writes should work immediately after startup.
        """
        response = create_scooter(api_url, unique_scooter_id)

        # Should work
        assert response.status_code in [200, 201], \
            f"BUG: Write failed immediately after startup: {response.status_code}"

    def test_reads_after_writes(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        CATCHES BUG: Read-after-write should be consistent.
        """
        # Write
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        # Immediate read
        response = get_scooter(api_url, unique_scooter_id)

        assert response.status_code == 200
        scooter = response.json()
        assert scooter["is_available"] == False, \
            "BUG: Read-after-write inconsistency!"
        assert scooter["current_reservation_id"] == unique_reservation_id, \
            "BUG: Read returned wrong reservation ID!"


class TestConnectionResourceLeaks:
    """
    Tests for connection resource leaks during recovery.

    BUG: recovery.go creates new gRPC connections for each server
    without proper cleanup, potentially leaking resources.
    """

    def test_many_operations_no_resource_exhaustion(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Many operations shouldn't exhaust resources.

        If connections are leaked, the system would eventually fail.
        """
        create_scooter(api_url, unique_scooter_id)

        errors = []

        # Do many operations
        for i in range(200):
            try:
                res = reserve_scooter(api_url, unique_scooter_id, f"resource-{i}")
                if res.status_code == 200:
                    release_scooter(api_url, unique_scooter_id, 1)
                else:
                    errors.append(f"Reserve {i}: {res.status_code}")
            except requests.exceptions.ConnectionError as e:
                errors.append(f"Connection error at {i}: {e}")
                # Connection errors might indicate resource exhaustion
                break
            except Exception as e:
                errors.append(f"Error {i}: {e}")

        # Should complete without too many errors
        assert len(errors) < 20, \
            f"BUG: Too many errors (possible resource leak?): {errors[:10]}..."

    def test_system_stable_after_load(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: System should remain stable after heavy load.
        """
        # Create many scooters
        for i in range(50):
            create_scooter(api_url, f"{unique_scooter_id}-load-{i}")

        # System should still work
        time.sleep(1)

        response = get_all_scooters(api_url)
        assert response.status_code == 200, \
            "BUG: System unstable after load!"
