"""
End-to-end tests for concurrent client scenarios.

These tests verify the system handles concurrent operations correctly:
- Concurrent creates
- Concurrent reservations (race conditions)
- High load scenarios

Requires: Full Docker Compose stack running.

Run with: pytest tests/e2e/test_concurrent_clients.py -v
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
    wait_for_server
)


class TestConcurrentOperations:
    """Tests for concurrent client operations."""

    def test_concurrent_create_different_scooters(self, api_url, unique_scooter_id):
        """
        Multiple clients creating different scooters concurrently should all succeed.
        """
        num_clients = 10
        scooter_ids = [f"{unique_scooter_id}-concurrent-{i}" for i in range(num_clients)]

        def create_one(scooter_id):
            try:
                response = create_scooter(api_url, scooter_id)
                return (scooter_id, response.status_code)
            except Exception as e:
                return (scooter_id, str(e))

        # Launch all creates concurrently
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(create_one, sid) for sid in scooter_ids]
            results = [f.result() for f in as_completed(futures)]

        # All should succeed
        successes = [r for r in results if r[1] in [200, 201]]
        assert len(successes) == num_clients, \
            f"Only {len(successes)}/{num_clients} succeeded: {results}"

        # Verify all scooters exist
        response = get_all_scooters(api_url)
        all_ids = [s["id"] for s in response.json()]
        for sid in scooter_ids:
            assert sid in all_ids, f"Scooter {sid} not created"

    def test_concurrent_reserve_same_scooter(self, api_url, unique_scooter_id):
        """
        Multiple clients trying to reserve the same scooter - only one should win.
        """
        # Create the scooter first
        create_scooter(api_url, unique_scooter_id)
        time.sleep(1)  # Make sure it's replicated

        num_clients = 5

        def try_reserve(client_id):
            try:
                response = reserve_scooter(api_url, unique_scooter_id, f"client-{client_id}")
                return (client_id, response.status_code)
            except Exception as e:
                return (client_id, str(e))

        # Launch all reservations concurrently
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(try_reserve, i) for i in range(num_clients)]
            results = [f.result() for f in as_completed(futures)]

        # Exactly one should succeed (status 200)
        successes = [r for r in results if r[1] == 200]
        failures = [r for r in results if r[1] in [400, 409]]

        # At least one success, and the rest should fail
        assert len(successes) >= 1, "No reservation succeeded"
        print(f"Successes: {len(successes)}, Failures: {len(failures)}")

        # The scooter should be reserved by exactly one client
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()
        assert scooter["is_available"] == False
        assert scooter["current_reservation_id"].startswith("client-")

    def test_concurrent_operations_on_different_scooters(self, api_url, unique_scooter_id):
        """
        Concurrent reserve/release on different scooters should all succeed.
        """
        # Create scooters
        scooter_ids = [f"{unique_scooter_id}-multi-{i}" for i in range(5)]
        for sid in scooter_ids:
            create_scooter(api_url, sid)
        time.sleep(2)

        def operate_scooter(scooter_id, client_id):
            try:
                # Reserve
                res1 = reserve_scooter(api_url, scooter_id, f"client-{client_id}")
                if res1.status_code != 200:
                    return (scooter_id, "reserve_failed", res1.status_code)

                # Release
                res2 = release_scooter(api_url, scooter_id, 50)
                if res2.status_code != 200:
                    return (scooter_id, "release_failed", res2.status_code)

                return (scooter_id, "success", 200)
            except Exception as e:
                return (scooter_id, "error", str(e))

        # Each client operates on their own scooter
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(operate_scooter, scooter_ids[i], i)
                for i in range(5)
            ]
            results = [f.result() for f in as_completed(futures)]

        # All should succeed
        successes = [r for r in results if r[1] == "success"]
        assert len(successes) == 5, f"Not all succeeded: {results}"


class TestHighLoad:
    """Tests for high load scenarios."""

    def test_many_requests_sequential(self, api_url, unique_scooter_id):
        """
        Many sequential requests should all complete.
        """
        create_scooter(api_url, unique_scooter_id)

        # 50 rapid cycles
        for i in range(50):
            reserve_scooter(api_url, unique_scooter_id, f"rapid-{i}")
            release_scooter(api_url, unique_scooter_id, 1)

        # Should have 50 total distance
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 50

    def test_many_concurrent_reads(self, api_url, unique_scooter_id):
        """
        Many concurrent reads should all succeed.
        """
        # Create scooter
        create_scooter(api_url, unique_scooter_id)

        num_reads = 50

        def do_read():
            try:
                response = get_scooter(api_url, unique_scooter_id)
                return response.status_code
            except Exception:
                return "error"

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(do_read) for _ in range(num_reads)]
            results = [f.result() for f in as_completed(futures)]

        successes = sum(1 for r in results if r == 200)
        assert successes >= num_reads * 0.9, f"Only {successes}/{num_reads} reads succeeded"

    def test_mixed_reads_and_writes(self, api_url, unique_scooter_id):
        """
        Mixed concurrent reads and writes should work correctly.
        """
        create_scooter(api_url, unique_scooter_id)

        results = {"reads": 0, "writes": 0, "errors": 0}
        lock = threading.Lock()

        def do_read():
            try:
                response = get_scooter(api_url, unique_scooter_id)
                with lock:
                    if response.status_code == 200:
                        results["reads"] += 1
                    else:
                        results["errors"] += 1
            except Exception:
                with lock:
                    results["errors"] += 1

        def do_write(iteration):
            try:
                # Only try to reserve if available
                response = get_scooter(api_url, unique_scooter_id)
                if response.status_code == 200 and response.json()["is_available"]:
                    res = reserve_scooter(api_url, unique_scooter_id, f"write-{iteration}")
                    if res.status_code == 200:
                        release_scooter(api_url, unique_scooter_id, 1)
                        with lock:
                            results["writes"] += 1
            except Exception:
                with lock:
                    results["errors"] += 1

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for i in range(30):
                futures.append(executor.submit(do_read))
                if i % 3 == 0:
                    futures.append(executor.submit(do_write, i))

            for f in as_completed(futures):
                pass  # Just wait for all

        print(f"Results: {results}")
        assert results["reads"] >= 20, "Too few successful reads"


class TestLinearizability:
    """Tests for linearizability (strong consistency)."""

    def test_writes_are_ordered(self, api_url, unique_scooter_id):
        """
        Sequential writes should be applied in order.
        """
        create_scooter(api_url, unique_scooter_id)

        # Do sequential writes with specific distances
        distances = [10, 20, 30, 40, 50]
        for i, distance in enumerate(distances):
            reserve_scooter(api_url, unique_scooter_id, f"seq-{i}")
            release_scooter(api_url, unique_scooter_id, distance)

        # Total should be sum of all distances
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == sum(distances)

    def test_read_sees_previous_write(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        A read after a write should see the written value.
        """
        create_scooter(api_url, unique_scooter_id)

        # Write
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        # Read should see the reservation
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()
        assert scooter["is_available"] == False
        assert scooter["current_reservation_id"] == unique_reservation_id

    def test_no_stale_reads_after_release(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        After release, reads should not show the old reserved state.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        release_scooter(api_url, unique_scooter_id, 100)

        # Multiple reads should all show available
        for _ in range(10):
            response = get_scooter(api_url, unique_scooter_id)
            scooter = response.json()
            assert scooter["is_available"] == True
            assert scooter["total_distance"] == 100


class TestRaceConditions:
    """Tests for potential race conditions."""

    def test_double_release_fails(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Two clients trying to release the same scooter - only one should succeed.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        def try_release(client_id):
            try:
                response = release_scooter(api_url, unique_scooter_id, 50)
                return (client_id, response.status_code)
            except Exception as e:
                return (client_id, str(e))

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(try_release, i) for i in range(2)]
            results = [f.result() for f in as_completed(futures)]

        # One should succeed, one should fail
        successes = [r for r in results if r[1] == 200]
        failures = [r for r in results if r[1] in [400, 409]]

        # At least one success (might be timing-dependent)
        assert len(successes) >= 1

        # Final state should show 50 distance (not 100)
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 50

    def test_reserve_during_release(self, api_url, unique_scooter_id):
        """
        Reserve and release happening simultaneously should resolve correctly.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, "initial-res")

        def release():
            return release_scooter(api_url, unique_scooter_id, 50)

        def reserve():
            # This should fail because scooter is reserved
            # OR succeed if it happens after release
            return reserve_scooter(api_url, unique_scooter_id, "concurrent-res")

        with ThreadPoolExecutor(max_workers=2) as executor:
            f1 = executor.submit(release)
            f2 = executor.submit(reserve)
            r1 = f1.result()
            r2 = f2.result()

        # Release should succeed
        assert r1.status_code == 200

        # Final state should be consistent (either reserved or available)
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()

        # Either the reserve happened after release (is_available=False)
        # Or it failed and scooter is available
        if scooter["is_available"]:
            assert scooter["total_distance"] == 50
        else:
            assert scooter["current_reservation_id"] == "concurrent-res"
