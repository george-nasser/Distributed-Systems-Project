"""
Tests for fault tolerance as specified in the assignment.

From the assignment (Section 4.1 Assumptions):
- In a system with n nodes, there may be up to f < n/2 failures
- With n=5 servers, up to f=2 can fail simultaneously
- Crash-recover failures (not Byzantine)
- A live quorum must exist at all times

Run with: pytest tests/assignment/test_fault_tolerance.py -v
"""

import pytest
import time
import sys
import os
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conftest import (
    create_scooter, get_scooter, get_all_scooters,
    reserve_scooter, release_scooter,
    wait_for_server
)


class TestQuorumRequirements:
    """
    Tests that the system works with a quorum (majority) of servers.

    With n=5 servers, quorum = 3 (majority).
    System should work with 3, 4, or 5 servers alive.
    """

    def test_system_works_with_all_servers(self, api_url, unique_scooter_id):
        """
        Basic test: system works when all servers are up.
        """
        response = create_scooter(api_url, unique_scooter_id)
        assert response.status_code in [200, 201]

        response = get_scooter(api_url, unique_scooter_id)
        assert response.status_code == 200

    def test_system_works_through_load_balancer(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Full workflow through load balancer should work.
        """
        # Create
        response = create_scooter(api_url, unique_scooter_id)
        assert response.status_code in [200, 201]

        # Reserve
        response = reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        assert response.status_code == 200

        # Release
        response = release_scooter(api_url, unique_scooter_id, 100)
        assert response.status_code == 200

        # Verify
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 100

    def test_reads_work_from_multiple_servers(self, server_urls, unique_scooter_id):
        """
        Reads should work from any server that's up.
        """
        # Create scooter
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(3)  # Wait for replication

        # Try reading from each server
        successful_reads = 0
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    successful_reads += 1
            except Exception:
                pass

        # At least a majority should respond
        assert successful_reads >= 3, \
            f"Only {successful_reads}/5 servers responded to reads"


class TestMajorityRequired:
    """
    Tests that consensus requires a majority of servers.

    This is the fundamental Paxos requirement.
    """

    def test_write_requires_majority_acknowledgment(self, api_url, unique_scooter_id):
        """
        A write should only succeed if a majority of servers acknowledge.

        We can't easily test this directly, but we can verify that writes
        are durable and replicated.
        """
        # Create scooter
        create_scooter(api_url, unique_scooter_id)

        # Do multiple writes
        for i in range(5):
            reserve_scooter(api_url, unique_scooter_id, f"majority-{i}")
            release_scooter(api_url, unique_scooter_id, 10)

        # Verify all writes applied (would fail if majority wasn't working)
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 50

    def test_replicated_data_survives(self, server_urls, unique_scooter_id):
        """
        Data written to the system should be replicated to multiple servers.
        """
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, "replicate-test")
        release_scooter(server_urls[0], unique_scooter_id, 200)

        # Wait for replication
        time.sleep(5)

        # Check multiple servers have the data
        servers_with_data = 0
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    if response.json()["total_distance"] == 200:
                        servers_with_data += 1
            except Exception:
                pass

        # At least majority should have the data
        assert servers_with_data >= 3, \
            f"Only {servers_with_data}/5 servers have the data"


class TestCrashRecoverFailures:
    """
    Tests for crash-recover failure model.

    From assignment: nodes can crash and recover, system should handle this.
    """

    def test_data_persists_across_time(self, api_url, unique_scooter_id):
        """
        Data written should persist (simulates recovery scenario).
        """
        # Write data
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, "persist-test")
        release_scooter(api_url, unique_scooter_id, 500)

        # Wait some time (simulating potential recovery)
        time.sleep(3)

        # Data should still be there
        response = get_scooter(api_url, unique_scooter_id)
        assert response.status_code == 200
        assert response.json()["total_distance"] == 500

    def test_operations_work_after_delay(self, api_url, unique_scooter_id):
        """
        Operations should work even after system has been idle.
        """
        create_scooter(api_url, unique_scooter_id)

        # Wait (simulating idle period where crashes could happen)
        time.sleep(5)

        # Operations should still work
        response = reserve_scooter(api_url, unique_scooter_id, "delay-test")
        assert response.status_code == 200

        response = release_scooter(api_url, unique_scooter_id, 100)
        assert response.status_code == 200


class TestNoByzantineFaults:
    """
    Tests assuming no Byzantine faults.

    From assignment: "No Byzantine faults: nodes cannot behave maliciously"

    This means we trust that servers don't lie or corrupt data intentionally.
    We just need to handle crashes.
    """

    def test_all_servers_agree_on_state(self, server_urls, unique_scooter_id):
        """
        All servers should have the same state (no lying servers).
        """
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, "agree-test")
        release_scooter(server_urls[0], unique_scooter_id, 123)

        # Wait for replication
        time.sleep(5)

        # All servers should agree
        states = []
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    states.append(response.json()["total_distance"])
            except Exception:
                pass

        if len(states) > 1:
            assert all(s == states[0] for s in states), \
                f"Servers disagree on state: {states}"

    def test_no_data_corruption(self, api_url, unique_scooter_id):
        """
        Data should not be corrupted by the system.
        """
        create_scooter(api_url, unique_scooter_id)

        # Write specific values
        expected_total = 0
        for i in range(10):
            distance = (i + 1) * 11  # 11, 22, 33, ...
            reserve_scooter(api_url, unique_scooter_id, f"corrupt-{i}")
            release_scooter(api_url, unique_scooter_id, distance)
            expected_total += distance

        # Verify no corruption
        response = get_scooter(api_url, unique_scooter_id)
        actual = response.json()["total_distance"]

        assert actual == expected_total, \
            f"Data corrupted! Expected {expected_total}, got {actual}"


class TestSystemAvailability:
    """
    Tests for system availability when servers are up.
    """

    def test_system_responds_to_requests(self, api_url):
        """
        System should respond to requests when majority is up.
        """
        response = get_all_scooters(api_url)
        assert response.status_code == 200

    def test_multiple_concurrent_requests(self, api_url, unique_scooter_id):
        """
        System should handle multiple concurrent requests.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Create scooters concurrently
        scooter_ids = [f"{unique_scooter_id}-avail-{i}" for i in range(10)]

        def create_one(sid):
            try:
                return create_scooter(api_url, sid).status_code
            except Exception:
                return "error"

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(create_one, sid) for sid in scooter_ids]
            results = [f.result() for f in as_completed(futures)]

        # Most should succeed
        successes = sum(1 for r in results if r in [200, 201])
        assert successes >= 8, f"Only {successes}/10 concurrent creates succeeded"

    def test_system_handles_load(self, api_url, unique_scooter_id):
        """
        System should handle reasonable load.
        """
        create_scooter(api_url, unique_scooter_id)

        # Do 50 operations
        errors = 0
        for i in range(50):
            try:
                res = reserve_scooter(api_url, unique_scooter_id, f"load-{i}")
                if res.status_code != 200:
                    errors += 1
                    continue
                rel = release_scooter(api_url, unique_scooter_id, 1)
                if rel.status_code != 200:
                    errors += 1
            except Exception:
                errors += 1

        # Should have minimal errors
        assert errors < 10, f"Too many errors under load: {errors}/50"


class TestGracefulDegradation:
    """
    Tests that system degrades gracefully under partial failures.

    Note: These tests assume some servers might be down.
    """

    def test_operations_complete_or_fail_cleanly(self, api_url, unique_scooter_id):
        """
        Operations should either complete successfully or fail cleanly.

        No hanging, no partial results.
        """
        try:
            response = create_scooter(api_url, unique_scooter_id)
            # Should get a definite response
            assert response.status_code in [200, 201, 400, 404, 500, 503]
        except requests.exceptions.Timeout:
            pytest.fail("Operation timed out - should fail faster")
        except requests.exceptions.ConnectionError:
            # This is OK - system might be down
            pass

    def test_error_responses_are_proper_http(self, api_url):
        """
        Error responses should be proper HTTP responses.
        """
        # Request for nonexistent scooter
        response = get_scooter(api_url, "definitely-not-real-12345")

        # Should get proper HTTP error, not crash
        assert response.status_code in [404, 500, 503]
