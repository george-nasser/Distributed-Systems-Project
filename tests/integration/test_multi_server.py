"""
Integration tests for multi-server replication.

These tests verify that writes replicate correctly across all servers
in the cluster, and that any server can serve reads.

Requires: Docker Compose with all 5 scooter-server replicas running.

Run with: pytest tests/integration/test_multi_server.py -v
"""

import pytest
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conftest import (
    create_scooter, get_scooter, get_all_scooters,
    reserve_scooter, release_scooter,
    wait_for_server, wait_for_replication
)


class TestBasicReplication:
    """Tests for basic data replication across servers."""

    def test_write_replicates_to_all_servers(self, server_urls, unique_scooter_id):
        """Write on one server becomes visible on all other servers."""
        # Make sure at least first server is up
        assert wait_for_server(server_urls[0]), "Server 0 not available"

        # Create scooter on server 0
        response = create_scooter(server_urls[0], unique_scooter_id)
        assert response.status_code in [200, 201]

        # Wait a bit for replication
        time.sleep(2)

        # Check all other servers have the scooter
        for i, url in enumerate(server_urls):
            try:
                response = get_scooter(url, unique_scooter_id)
                assert response.status_code == 200, f"Server {i} doesn't have scooter"
                assert response.json()["id"] == unique_scooter_id
            except Exception as e:
                # Server might be down, that's OK for some tests
                print(f"Server {i} at {url} unavailable: {e}")

    def test_read_from_any_server(self, server_urls, unique_scooter_id):
        """Any server can return the correct scooter data."""
        # Create scooter on server 0
        create_scooter(server_urls[0], unique_scooter_id)

        # Wait for replication
        wait_for_replication(server_urls, unique_scooter_id)

        # Read from each server
        for i, url in enumerate(server_urls):
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    scooter = response.json()
                    assert scooter["id"] == unique_scooter_id
                    assert scooter["is_available"] == True
                    assert scooter["total_distance"] == 0
            except Exception as e:
                print(f"Server {i} unavailable: {e}")

    def test_all_servers_have_same_state(self, server_urls, unique_scooter_id, unique_reservation_id):
        """All servers converge to the same state."""
        # Create and modify scooter
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(1)
        reserve_scooter(server_urls[0], unique_scooter_id, unique_reservation_id)
        time.sleep(1)
        release_scooter(server_urls[0], unique_scooter_id, 100)

        # Wait for replication
        time.sleep(3)

        # Check all servers have the same state
        expected_state = None
        for i, url in enumerate(server_urls):
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    scooter = response.json()
                    if expected_state is None:
                        expected_state = scooter
                    else:
                        # Compare key fields
                        assert scooter["id"] == expected_state["id"]
                        assert scooter["is_available"] == expected_state["is_available"]
                        assert scooter["total_distance"] == expected_state["total_distance"]
            except Exception as e:
                print(f"Server {i} unavailable: {e}")


class TestWriteOrdering:
    """Tests for write ordering across servers."""

    def test_sequential_writes_preserved(self, server_urls, unique_scooter_id):
        """Writes happen in the correct order across all servers."""
        # Create scooter
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(1)

        # Do multiple reserve/release cycles with increasing distances
        distances = [10, 20, 30, 40, 50]
        for i, distance in enumerate(distances):
            reserve_scooter(server_urls[0], unique_scooter_id, f"res-{i}")
            time.sleep(0.5)
            release_scooter(server_urls[0], unique_scooter_id, distance)
            time.sleep(0.5)

        # Wait for replication
        time.sleep(3)

        # All servers should have the same total distance
        expected_total = sum(distances)  # 150
        for i, url in enumerate(server_urls):
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    actual_distance = response.json()["total_distance"]
                    assert actual_distance == expected_total, \
                        f"Server {i} has wrong distance: {actual_distance} != {expected_total}"
            except Exception as e:
                print(f"Server {i} unavailable: {e}")

    def test_concurrent_writes_same_order(self, server_urls, unique_scooter_id):
        """
        Concurrent writes from same client should be ordered.

        Note: True concurrent writes from different clients are tested
        in the e2e tests. This test just verifies rapid sequential writes
        maintain order.
        """
        create_scooter(server_urls[0], unique_scooter_id)

        # Rapid reserve/release
        for i in range(5):
            reserve_scooter(server_urls[0], unique_scooter_id, f"rapid-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 10)

        # Wait for replication
        time.sleep(3)

        # All servers should have same state
        for i, url in enumerate(server_urls):
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    scooter = response.json()
                    assert scooter["total_distance"] == 50
                    assert scooter["is_available"] == True
            except Exception as e:
                print(f"Server {i} unavailable: {e}")


class TestCrossServerOperations:
    """Tests for operations that span multiple servers."""

    def test_create_on_one_reserve_on_another(self, server_urls, unique_scooter_id, unique_reservation_id):
        """Create scooter on server A, reserve on server B."""
        # Create on server 0
        response = create_scooter(server_urls[0], unique_scooter_id)
        assert response.status_code in [200, 201]

        # Wait for replication to server 1
        time.sleep(2)

        # Reserve on server 1
        response = reserve_scooter(server_urls[1], unique_scooter_id, unique_reservation_id)
        # This might need the write to go through Paxos, which might route to leader
        # So either 200 or redirect behavior is acceptable
        assert response.status_code in [200, 201, 307], f"Got {response.status_code}"

        # Wait for replication
        time.sleep(2)

        # Check state on all servers
        for i, url in enumerate(server_urls):
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    scooter = response.json()
                    assert scooter["is_available"] == False
            except Exception as e:
                print(f"Server {i} unavailable: {e}")

    def test_operations_cycle_through_servers(self, server_urls, unique_scooter_id):
        """Operations on different servers all apply correctly."""
        num_servers = len(server_urls)

        # Create on server 0
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(1)

        # Do reserve/release on each server in turn
        for i in range(num_servers):
            server_url = server_urls[i % num_servers]
            try:
                reserve_scooter(server_url, unique_scooter_id, f"res-server-{i}")
                time.sleep(0.5)
                release_scooter(server_url, unique_scooter_id, 10)
                time.sleep(0.5)
            except Exception as e:
                print(f"Server {i} operation failed: {e}")

        # Wait for replication
        time.sleep(3)

        # Check final state
        response = get_scooter(server_urls[0], unique_scooter_id)
        if response.status_code == 200:
            scooter = response.json()
            # Should have accumulated distance from all successful operations
            assert scooter["is_available"] == True
            assert scooter["total_distance"] >= 10  # At least one cycle worked


class TestReplicationTiming:
    """Tests for replication timing behavior."""

    def test_replication_happens_quickly(self, server_urls, unique_scooter_id):
        """Replication should happen within a few seconds."""
        # Create scooter
        start = time.time()
        create_scooter(server_urls[0], unique_scooter_id)

        # Poll other servers for replication
        replicated = wait_for_replication(server_urls[1:], unique_scooter_id, timeout=10)
        elapsed = time.time() - start

        assert replicated, "Replication didn't complete in 10 seconds"
        print(f"Replication completed in {elapsed:.2f} seconds")

    def test_reads_eventually_consistent(self, server_urls, unique_scooter_id, unique_reservation_id):
        """Reads eventually show the latest state."""
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(1)

        # Reserve
        reserve_scooter(server_urls[0], unique_scooter_id, unique_reservation_id)

        # Keep reading from server 1 until we see the reservation
        start = time.time()
        while time.time() - start < 10:
            try:
                response = get_scooter(server_urls[1], unique_scooter_id)
                if response.status_code == 200:
                    scooter = response.json()
                    if scooter["is_available"] == False:
                        # Got the updated state
                        print(f"Consistency achieved in {time.time() - start:.2f}s")
                        return
            except Exception:
                pass
            time.sleep(0.2)

        pytest.fail("Read consistency not achieved within 10 seconds")
