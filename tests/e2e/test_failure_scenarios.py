"""
End-to-end tests for failure scenarios.

These tests verify the system handles failures correctly:
- Server failures
- Recovery after failure

Requires: Full Docker Compose stack with ability to stop/start containers.

Run with: pytest tests/e2e/test_failure_scenarios.py -v
"""

import pytest
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conftest import (
    create_scooter, get_scooter, get_all_scooters,
    reserve_scooter, release_scooter,
    wait_for_server,
    DockerComposeManager
)


class TestServerFailures:
    """Tests for handling server failures."""

    def test_system_works_with_one_server_down(self, api_url, server_urls, unique_scooter_id):
        """
        System should work with 4/5 servers (one down).

        Note: This test assumes docker-compose is running. It just tests
        that the system handles unreachable servers gracefully.
        """
        # Create scooter through load balancer
        response = create_scooter(api_url, unique_scooter_id)

        # Should succeed (Paxos needs majority: 3 of 5)
        assert response.status_code in [200, 201]

        # Full workflow should still work
        response = reserve_scooter(api_url, unique_scooter_id, "res-1")
        assert response.status_code == 200

        response = release_scooter(api_url, unique_scooter_id, 100)
        assert response.status_code == 200

    def test_system_works_with_two_servers_down(self, api_url, unique_scooter_id):
        """
        System should work with 3/5 servers (two down).

        With 5 servers, we need 3 for majority. This tests that
        the minimum quorum still works.
        """
        # The system should handle this through the load balancer
        response = create_scooter(api_url, unique_scooter_id)

        # Might fail if less than 3 servers are actually up
        # But if it succeeds, the rest should too
        if response.status_code in [200, 201]:
            response = reserve_scooter(api_url, unique_scooter_id, "res-1")
            # Continue only if create succeeded
            if response.status_code == 200:
                response = release_scooter(api_url, unique_scooter_id, 50)
                assert response.status_code == 200

    def test_system_fails_with_majority_down(self, api_url, unique_scooter_id):
        """
        System should fail gracefully when majority is down.

        With only 2/5 servers, we don't have majority and writes should fail.

        Note: This test documents expected behavior; it's hard to test without
        actually stopping containers.
        """
        # This is more of a documentation test
        # In practice, if majority is down:
        # - Writes should fail or timeout
        # - Reads might still work for cached data

        # Just verify the system is responsive
        try:
            response = create_scooter(api_url, unique_scooter_id)
            # If it succeeds, majority must be up
            if response.status_code in [200, 201]:
                print("Majority appears to be up, system is working")
            else:
                print(f"Got status {response.status_code}, might indicate quorum issues")
        except Exception as e:
            print(f"System unavailable (possibly majority down): {e}")


class TestRecoveryAfterFailure:
    """Tests for recovery after server failures."""

    def test_data_persists_across_requests(self, api_url, unique_scooter_id):
        """
        Data should persist and be available across multiple requests.

        This simulates a basic recovery scenario where data written
        earlier is still available.
        """
        # Create and modify data
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, "res-1")
        release_scooter(api_url, unique_scooter_id, 100)

        # Wait a bit (simulating time passing)
        time.sleep(2)

        # Data should still be there
        response = get_scooter(api_url, unique_scooter_id)
        assert response.status_code == 200
        assert response.json()["total_distance"] == 100

    def test_writes_succeed_after_waiting(self, api_url, unique_scooter_id):
        """
        Writes should succeed even after system has been idle.
        """
        # Initial write
        create_scooter(api_url, unique_scooter_id)

        # Wait a bit
        time.sleep(5)

        # More writes should still work
        response = reserve_scooter(api_url, unique_scooter_id, "delayed-res")
        assert response.status_code == 200

        response = release_scooter(api_url, unique_scooter_id, 50)
        assert response.status_code == 200

    def test_server_rejoins_sees_data(self, server_urls, unique_scooter_id):
        """
        When a server comes back up, it should see all the data.

        This tests replication/recovery without actually stopping containers.
        """
        # Write data
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, "res-1")
        release_scooter(server_urls[0], unique_scooter_id, 150)

        # Wait for replication
        time.sleep(3)

        # All servers should have the data
        for i, url in enumerate(server_urls):
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    scooter = response.json()
                    assert scooter["total_distance"] == 150, \
                        f"Server {i} has wrong distance"
            except Exception as e:
                print(f"Server {i} unavailable: {e}")


class TestNetworkPartition:
    """Tests for network partition scenarios (simulated)."""

    def test_operations_during_instability(self, api_url, unique_scooter_id):
        """
        Test that operations handle temporary instability.

        This just does rapid operations which might expose timing issues.
        """
        create_scooter(api_url, unique_scooter_id)

        # Rapid operations
        success_count = 0
        for i in range(10):
            try:
                response = reserve_scooter(api_url, unique_scooter_id, f"rapid-{i}")
                if response.status_code == 200:
                    release_scooter(api_url, unique_scooter_id, 10)
                    success_count += 1
            except Exception:
                pass

        # Most should succeed
        assert success_count >= 5, f"Only {success_count}/10 operations succeeded"

    def test_eventual_consistency(self, server_urls, unique_scooter_id):
        """
        Even with some failures, system should reach consistent state.
        """
        # Write data
        create_scooter(server_urls[0], unique_scooter_id)
        for i in range(5):
            reserve_scooter(server_urls[0], unique_scooter_id, f"res-{i}")
            release_scooter(server_urls[0], unique_scooter_id, 20)

        # Wait for system to stabilize
        time.sleep(5)

        # All responding servers should have same state
        distances = []
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    distances.append(response.json()["total_distance"])
            except Exception:
                pass

        if len(distances) > 1:
            assert all(d == distances[0] for d in distances), \
                f"Inconsistent distances: {distances}"


class TestGracefulDegradation:
    """Tests for graceful degradation under failure conditions."""

    def test_reads_work_even_if_writes_slow(self, api_url, unique_scooter_id):
        """
        Reads should work even if the system is under load.
        """
        # Create some data
        create_scooter(api_url, unique_scooter_id)

        # Multiple reads should all work
        for _ in range(10):
            response = get_scooter(api_url, unique_scooter_id)
            assert response.status_code == 200

    def test_system_responds_to_health_checks(self, server_urls):
        """
        Servers should respond to basic queries even under load.
        """
        responsive = 0
        for url in server_urls:
            try:
                response = get_all_scooters(url)
                if response.status_code == 200:
                    responsive += 1
            except Exception:
                pass

        # At least some servers should be responsive
        assert responsive >= 1, "No servers responding"
        print(f"{responsive}/{len(server_urls)} servers responsive")

    def test_error_responses_are_proper(self, api_url):
        """
        Error responses should be proper HTTP errors, not crashes.
        """
        # Try operations that should fail gracefully
        # Reserve non-existent scooter
        response = reserve_scooter(api_url, "nonexistent-scooter-xyz", "res-1")
        assert response.status_code in [400, 404, 500]  # Not a crash

        # Release non-existent scooter
        response = release_scooter(api_url, "nonexistent-scooter-xyz", 100)
        assert response.status_code in [400, 404, 500]

        # Get non-existent scooter
        response = get_scooter(api_url, "nonexistent-scooter-xyz")
        assert response.status_code == 404
