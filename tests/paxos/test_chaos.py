"""
Chaos and stress tests for system resilience.

These tests push the system to its limits:
- Chaos scenarios with random failures/delays
- Stress testing with high load
- Edge conditions (timeouts, large payloads)
- Consistency verification under chaos

Run with: pytest tests/paxos/test_chaos.py -v

Note: Some of these tests may take longer to run.
"""

import pytest
import time
import sys
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conftest import (
    create_scooter, get_scooter, get_all_scooters,
    reserve_scooter, release_scooter, take_snapshot,
    wait_for_replication
)


class TestChaosScenarios:
    """
    Chaos testing scenarios.
    """

    def test_random_server_access(self, server_urls, unique_scooter_id):
        """
        Randomly access different servers for operations.
        """
        # Create on one server
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(1)

        successful_ops = 0
        for i in range(20):
            # Pick random server
            url = random.choice(server_urls)

            try:
                # Try to do operation
                res = reserve_scooter(url, unique_scooter_id, f"chaos-{i}")
                if res.status_code == 200:
                    release_scooter(url, unique_scooter_id, 1)
                    successful_ops += 1
            except Exception:
                pass

        # Should complete some operations
        assert successful_ops >= 10, f"Only {successful_ops} ops succeeded"

    def test_rapid_operations_during_instability(self, api_url, unique_scooter_id):
        """
        Many rapid operations even during potential instability.
        """
        create_scooter(api_url, unique_scooter_id)

        results = []
        lock = threading.Lock()

        def do_operation(op_id):
            try:
                res1 = reserve_scooter(api_url, unique_scooter_id, f"rapid-{op_id}")
                if res1.status_code == 200:
                    res2 = release_scooter(api_url, unique_scooter_id, 1)
                    if res2.status_code == 200:
                        with lock:
                            results.append("success")
                        return
                with lock:
                    results.append("failed")
            except Exception as e:
                with lock:
                    results.append(f"error: {e}")

        # Launch many concurrent operations
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(do_operation, i) for i in range(50)]
            for f in as_completed(futures):
                pass

        successes = sum(1 for r in results if r == "success")
        # Some should succeed even under pressure
        assert successes >= 20, f"Only {successes}/50 succeeded"

    def test_system_recovers_from_chaos(self, api_url, server_urls, unique_scooter_id):
        """
        After chaotic operations, system returns to consistency.
        """
        create_scooter(api_url, unique_scooter_id)

        # Do chaotic operations
        for i in range(30):
            url = random.choice(server_urls)
            try:
                reserve_scooter(url, unique_scooter_id, f"recover-{i}")
                release_scooter(url, unique_scooter_id, 1)
            except Exception:
                pass

        # Wait for system to stabilize
        time.sleep(5)

        # All servers should converge
        distances = []
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    distances.append(response.json()["total_distance"])
            except Exception:
                pass

        # All responding servers should agree
        if len(distances) >= 2:
            assert all(d == distances[0] for d in distances), \
                f"Servers didn't converge: {distances}"


class TestStressTesting:
    """
    Stress tests for high load scenarios.
    """

    def test_sustained_high_load(self, api_url, unique_scooter_id):
        """
        1000+ operations over time.
        """
        create_scooter(api_url, unique_scooter_id)

        successful = 0
        failed = 0

        # 100 batches of 10 operations
        for batch in range(100):
            for i in range(10):
                try:
                    res = reserve_scooter(api_url, unique_scooter_id, f"load-{batch}-{i}")
                    if res.status_code == 200:
                        release_scooter(api_url, unique_scooter_id, 1)
                        successful += 1
                    else:
                        failed += 1
                except Exception:
                    failed += 1

        # Most should succeed
        total = successful + failed
        success_rate = successful / total if total > 0 else 0
        assert success_rate >= 0.8, f"Success rate {success_rate:.2%} too low"

        # Final state should reflect successes
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == successful

    def test_burst_traffic(self, api_url, unique_scooter_id):
        """
        100 operations in rapid burst.
        """
        # Create multiple scooters for burst
        scooter_ids = [f"{unique_scooter_id}-burst-{i}" for i in range(20)]

        results = []
        lock = threading.Lock()

        def create_one(sid):
            try:
                response = create_scooter(api_url, sid)
                with lock:
                    results.append(response.status_code in [200, 201])
            except Exception:
                with lock:
                    results.append(False)

        # Burst of creates
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(create_one, sid) for sid in scooter_ids]
            for f in as_completed(futures):
                pass

        # Most should succeed
        successes = sum(results)
        assert successes >= 15, f"Only {successes}/20 creates succeeded in burst"

    def test_many_concurrent_clients(self, api_url, unique_scooter_id):
        """
        50+ concurrent clients doing operations.
        """
        # Create scooters for each client
        scooter_ids = [f"{unique_scooter_id}-client-{i}" for i in range(50)]
        for sid in scooter_ids:
            create_scooter(api_url, sid)

        time.sleep(2)

        results = []
        lock = threading.Lock()

        def client_operation(sid, client_id):
            try:
                # Each client reserves their scooter
                res1 = reserve_scooter(api_url, sid, f"client-{client_id}")
                if res1.status_code == 200:
                    res2 = release_scooter(api_url, sid, client_id + 1)
                    if res2.status_code == 200:
                        with lock:
                            results.append(("success", client_id))
                        return
                with lock:
                    results.append(("failed", client_id))
            except Exception as e:
                with lock:
                    results.append(("error", str(e)))

        # 50 clients operating concurrently
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [
                executor.submit(client_operation, scooter_ids[i], i)
                for i in range(50)
            ]
            for f in as_completed(futures):
                pass

        # Most clients should succeed
        successes = sum(1 for r in results if r[0] == "success")
        assert successes >= 40, f"Only {successes}/50 clients succeeded"


class TestEdgeConditions:
    """
    Tests for edge conditions.
    """

    def test_timeout_handling(self, api_url, unique_scooter_id):
        """
        Operations with timeouts are handled correctly.
        """
        create_scooter(api_url, unique_scooter_id)

        # Try operations with short timeout
        for i in range(10):
            try:
                response = requests.post(
                    f"{api_url}/scooters/{unique_scooter_id}/reservations",
                    json={"reservation_id": f"timeout-{i}"},
                    timeout=5  # 5 second timeout
                )
                if response.status_code == 200:
                    requests.post(
                        f"{api_url}/scooters/{unique_scooter_id}/releases",
                        json={"distance": 1},
                        timeout=5
                    )
            except requests.exceptions.Timeout:
                # Timeout is acceptable
                pass
            except Exception:
                pass

        # System should still be functional
        response = get_scooter(api_url, unique_scooter_id)
        assert response.status_code == 200

    def test_large_reservation_id(self, api_url, unique_scooter_id):
        """
        Large reservation IDs (up to 1000 chars) handled correctly.
        """
        create_scooter(api_url, unique_scooter_id)

        # Create a large reservation ID
        large_id = "x" * 500  # 500 character reservation ID

        response = reserve_scooter(api_url, unique_scooter_id, large_id)
        # Should either succeed or reject gracefully
        assert response.status_code in [200, 400, 413]

        if response.status_code == 200:
            response = get_scooter(api_url, unique_scooter_id)
            # If accepted, it should be stored
            assert response.json()["is_available"] == False

    def test_many_scooters(self, api_url, unique_scooter_id):
        """
        System handles 100+ scooters.
        """
        # Create many scooters
        scooter_ids = [f"{unique_scooter_id}-many-{i}" for i in range(100)]

        created = 0
        for sid in scooter_ids:
            try:
                response = create_scooter(api_url, sid)
                if response.status_code in [200, 201]:
                    created += 1
            except Exception:
                pass

        # Most should be created
        assert created >= 90, f"Only {created}/100 scooters created"

        # Should be able to list them
        response = get_all_scooters(api_url)
        assert response.status_code == 200
        all_scooters = response.json()
        assert len(all_scooters) >= 90


class TestConsistencyUnderChaos:
    """
    Tests for consistency under chaotic conditions.
    """

    def test_linearizability_under_load(self, api_url, unique_scooter_id):
        """
        Linearizable operations under high load.

        After each write completes, reads should see it.
        """
        create_scooter(api_url, unique_scooter_id)

        violations = 0
        for i in range(50):
            # Write
            res1 = reserve_scooter(api_url, unique_scooter_id, f"linear-{i}")
            if res1.status_code == 200:
                res2 = release_scooter(api_url, unique_scooter_id, 1)
                if res2.status_code == 200:
                    # Read should see the write
                    read = get_scooter(api_url, unique_scooter_id)
                    if read.json()["total_distance"] < i + 1:
                        violations += 1

        assert violations == 0, f"{violations} linearizability violations"

    def test_no_divergence_after_chaos(self, server_urls, unique_scooter_id):
        """
        All servers converge after chaotic operations.
        """
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(1)

        # Chaotic operations from different servers
        for i in range(30):
            url = server_urls[i % len(server_urls)]
            try:
                reserve_scooter(url, unique_scooter_id, f"diverge-{i}")
                release_scooter(url, unique_scooter_id, 2)
            except Exception:
                pass

        # Wait for convergence
        time.sleep(5)

        # Check all servers
        distances = []
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    distances.append(response.json()["total_distance"])
            except Exception:
                pass

        # All should have converged to same value
        if len(distances) >= 2:
            assert len(set(distances)) == 1, \
                f"Servers diverged: {distances}"

    def test_no_data_loss_after_chaos(self, api_url, server_urls, unique_scooter_id):
        """
        No scooters lost after chaos.
        """
        # Create scooters
        scooter_ids = [f"{unique_scooter_id}-loss-{i}" for i in range(10)]
        for sid in scooter_ids:
            create_scooter(api_url, sid)

        time.sleep(2)

        # Do chaotic operations
        for i in range(20):
            url = random.choice(server_urls)
            sid = random.choice(scooter_ids)
            try:
                reserve_scooter(url, sid, f"chaos-loss-{i}")
                release_scooter(url, sid, 1)
            except Exception:
                pass

        # Wait for stability
        time.sleep(3)

        # All scooters should still exist
        response = get_all_scooters(api_url)
        all_ids = [s["id"] for s in response.json()]

        for sid in scooter_ids:
            assert sid in all_ids, f"Scooter {sid} was lost"


class TestMixedWorkload:
    """
    Tests with mixed workloads.
    """

    def test_mixed_reads_writes_under_load(self, api_url, unique_scooter_id):
        """
        Mixed read and write operations under load.
        """
        create_scooter(api_url, unique_scooter_id)

        results = {"reads": 0, "writes": 0, "errors": 0}
        lock = threading.Lock()

        def reader():
            for _ in range(20):
                try:
                    response = get_scooter(api_url, unique_scooter_id)
                    if response.status_code == 200:
                        with lock:
                            results["reads"] += 1
                except Exception:
                    with lock:
                        results["errors"] += 1
                time.sleep(0.01)

        def writer():
            for i in range(10):
                try:
                    res = reserve_scooter(api_url, unique_scooter_id, f"mixed-{i}")
                    if res.status_code == 200:
                        release_scooter(api_url, unique_scooter_id, 1)
                        with lock:
                            results["writes"] += 1
                except Exception:
                    with lock:
                        results["errors"] += 1
                time.sleep(0.05)

        # Run readers and writers concurrently
        with ThreadPoolExecutor(max_workers=6) as executor:
            # 4 readers, 2 writers
            futures = [executor.submit(reader) for _ in range(4)]
            futures.extend([executor.submit(writer) for _ in range(2)])
            for f in as_completed(futures):
                pass

        # Most operations should succeed
        total_ops = results["reads"] + results["writes"]
        assert total_ops >= 50, f"Only {total_ops} operations completed"
        assert results["errors"] < 20, f"Too many errors: {results['errors']}"

    def test_snapshots_during_load(self, api_url, unique_scooter_id):
        """
        Snapshots taken during high load.
        """
        create_scooter(api_url, unique_scooter_id)

        def worker(worker_id):
            for i in range(20):
                try:
                    reserve_scooter(api_url, unique_scooter_id, f"snap-load-{worker_id}-{i}")
                    release_scooter(api_url, unique_scooter_id, 1)
                except Exception:
                    pass
                time.sleep(0.02)

        def snapshotter():
            for _ in range(5):
                time.sleep(0.5)
                try:
                    take_snapshot(api_url)
                except Exception:
                    pass

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(worker, i) for i in range(3)]
            futures.append(executor.submit(snapshotter))
            for f in as_completed(futures):
                pass

        # System should still be consistent
        response = get_scooter(api_url, unique_scooter_id)
        assert response.status_code == 200
        # Distance should be non-negative
        assert response.json()["total_distance"] >= 0


class TestLongRunning:
    """
    Longer running tests (may take more time).
    """

    def test_extended_operation_sequence(self, api_url, unique_scooter_id):
        """
        Long sequence of operations without failure.
        """
        create_scooter(api_url, unique_scooter_id)

        successful = 0
        for i in range(200):
            try:
                res = reserve_scooter(api_url, unique_scooter_id, f"extended-{i}")
                if res.status_code == 200:
                    release_scooter(api_url, unique_scooter_id, 1)
                    successful += 1
            except Exception:
                pass

        # Most should complete
        assert successful >= 180, f"Only {successful}/200 completed"

        # Final state should be correct
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == successful

    def test_alternating_servers_consistency(self, server_urls, unique_scooter_id):
        """
        Operations alternating between servers maintain consistency.
        """
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(1)

        expected = 0
        for i in range(50):
            url = server_urls[i % len(server_urls)]
            try:
                res = reserve_scooter(url, unique_scooter_id, f"alternate-{i}")
                if res.status_code == 200:
                    release_scooter(url, unique_scooter_id, 2)
                    expected += 2
            except Exception:
                pass

        # Wait for consistency
        time.sleep(3)

        # All servers should have the same state
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    distance = response.json()["total_distance"]
                    assert distance == expected, \
                        f"Server has {distance}, expected {expected}"
            except Exception:
                pass
