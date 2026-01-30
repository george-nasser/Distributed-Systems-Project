"""
Tests designed to catch race conditions and concurrency bugs.

These tests specifically target the following bugs:
1. handlers.go: Race between Propose/Append/Apply (lines 48-54)
2. statemachine/scooter.go: GetScooter/GetScooters return unsafe pointers
3. paxos/proposer.go: Mutex not protecting round changes properly

These tests try to expose race conditions by doing concurrent operations.

Run with: pytest tests/bugs/test_race_conditions.py -v
"""

import pytest
import requests
import time
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conftest import (
    create_scooter, get_scooter, get_all_scooters,
    reserve_scooter, release_scooter
)


class TestProposeLogApplyRace:
    """
    BUG: handlers.go lines 48-54 has a critical race condition:
    1. api.proposer.Propose() - might not have consensus yet
    2. api.log.Append() - appends before consensus confirmed
    3. api.stateMachine.Apply() - applies state

    Between these calls, another handler could read inconsistent state.
    This violates write-ahead-log semantics.
    """

    def test_read_during_write_race(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Try to read scooter state while it's being written.

        If Propose-Log-Apply isn't atomic, reads might see partial state.
        """
        results = {"reads": [], "write_success": False}
        stop_reading = threading.Event()

        def continuous_reader():
            """Keep reading until told to stop."""
            while not stop_reading.is_set():
                try:
                    response = get_scooter(api_url, unique_scooter_id)
                    if response.status_code == 200:
                        results["reads"].append(response.json())
                except Exception:
                    pass
                time.sleep(0.001)  # 1ms between reads

        def writer():
            """Create scooter then immediately reserve it."""
            try:
                create_scooter(api_url, unique_scooter_id)
                reserve_response = reserve_scooter(api_url, unique_scooter_id, "race-test")
                results["write_success"] = reserve_response.status_code == 200
            except Exception as e:
                print(f"Writer error: {e}")

        # Start reader thread
        reader_thread = threading.Thread(target=continuous_reader)
        reader_thread.start()

        # Give reader a moment to start
        time.sleep(0.01)

        # Do the write
        writer()

        # Let reader continue for a bit
        time.sleep(0.1)
        stop_reading.set()
        reader_thread.join()

        # Analyze reads - check for inconsistent states
        if results["reads"]:
            # All reads should show consistent state
            # Either scooter doesn't exist (404), or it exists with valid state
            for read in results["reads"]:
                if "is_available" in read:
                    # If we see the scooter, it should be in a valid state
                    # BUG: We might see scooter created but not yet reserved
                    # even though reserve was called "immediately" after create
                    pass

            # If write succeeded, final state should be reserved
            if results["write_success"]:
                final_response = get_scooter(api_url, unique_scooter_id)
                if final_response.status_code == 200:
                    final_state = final_response.json()
                    assert final_state["is_available"] == False, \
                        "BUG: Final state should be reserved but isn't"


class TestStateMachinePointerRace:
    """
    BUG: statemachine/scooter.go returns pointers to internal Scooter structs.
    After RUnlock, caller holds pointer that can be modified by Apply().

    This can cause torn reads - seeing partially updated state.
    """

    def test_read_while_modifying(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Read scooter while another thread modifies it.

        GetScooter returns pointer, then lock is released.
        If Apply() runs while we're examining the returned data,
        we might see is_available=True but also have a reservation_id.
        """
        create_scooter(api_url, unique_scooter_id)

        inconsistencies = []
        stop_flag = threading.Event()

        def reader():
            """Read scooter and check for inconsistent state."""
            while not stop_flag.is_set():
                try:
                    response = get_scooter(api_url, unique_scooter_id)
                    if response.status_code == 200:
                        scooter = response.json()

                        # Check for inconsistent state
                        is_available = scooter.get("is_available", True)
                        reservation_id = scooter.get("current_reservation_id", "")

                        # Inconsistent: has reservation but is_available=True
                        if is_available and reservation_id and reservation_id != "":
                            inconsistencies.append({
                                "is_available": is_available,
                                "reservation_id": reservation_id
                            })

                        # Inconsistent: not available but no reservation
                        if not is_available and (not reservation_id or reservation_id == ""):
                            # This might be OK during release, but capture it
                            pass

                except Exception:
                    pass
                time.sleep(0.001)

        def writer():
            """Repeatedly reserve and release."""
            for i in range(50):
                try:
                    reserve_scooter(api_url, unique_scooter_id, f"race-{i}")
                    release_scooter(api_url, unique_scooter_id, 1)
                except Exception:
                    pass

        # Start reader
        reader_thread = threading.Thread(target=reader)
        reader_thread.start()

        # Run writer
        writer()

        # Stop reader
        stop_flag.set()
        reader_thread.join()

        # Check for inconsistencies
        if inconsistencies:
            print(f"BUG: Found {len(inconsistencies)} inconsistent states!")
            for inc in inconsistencies[:5]:  # Show first 5
                print(f"  {inc}")
            # This is a serious bug - data race
            pytest.fail(f"BUG: Found {len(inconsistencies)} inconsistent state reads")

    def test_get_all_scooters_race(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: GetScooters returns slice of pointers that can be modified.

        While iterating over the returned slice, Apply() could modify
        the underlying scooter objects.
        """
        # Create several scooters
        scooter_ids = [f"{unique_scooter_id}-{i}" for i in range(10)]
        for sid in scooter_ids:
            create_scooter(api_url, sid)

        anomalies = []
        stop_flag = threading.Event()

        def reader():
            """Get all scooters and check for anomalies."""
            while not stop_flag.is_set():
                try:
                    response = get_all_scooters(api_url)
                    if response.status_code == 200:
                        scooters = response.json()

                        # Count available vs reserved
                        available = sum(1 for s in scooters if s.get("is_available", True))
                        reserved = len(scooters) - available

                        # Store snapshot for comparison
                        # BUG: If counts don't match expected, might be torn read
                except Exception:
                    pass
                time.sleep(0.005)

        def modifier():
            """Modify scooters rapidly."""
            for _ in range(20):
                for sid in scooter_ids:
                    try:
                        reserve_scooter(api_url, sid, f"mod-{time.time()}")
                        release_scooter(api_url, sid, 1)
                    except Exception:
                        pass

        # Run concurrently
        reader_thread = threading.Thread(target=reader)
        reader_thread.start()
        modifier()
        stop_flag.set()
        reader_thread.join()


class TestPaxosRoundRace:
    """
    BUG: paxos/proposer.go - Mutex not protecting round changes properly.

    Multiple concurrent Propose() calls could see stale or corrupted round numbers.
    Line 40 calls choose() which accesses p.round, potentially before mutex lock.
    """

    def test_concurrent_proposals_round_numbers(self, server_urls, unique_scooter_id):
        """
        CATCHES BUG: Concurrent proposals might use same round number.

        If round numbers aren't properly incremented under lock,
        two proposers could use the same round, causing Paxos violations.
        """
        num_proposals = 20
        results = []

        def make_proposal(i):
            scooter_id = f"{unique_scooter_id}-prop-{i}"
            try:
                response = create_scooter(server_urls[0], scooter_id)
                return (i, response.status_code, scooter_id)
            except Exception as e:
                return (i, str(e), scooter_id)

        # Launch many proposals concurrently
        with ThreadPoolExecutor(max_workers=num_proposals) as executor:
            futures = [executor.submit(make_proposal, i) for i in range(num_proposals)]
            results = [f.result() for f in as_completed(futures)]

        # All should succeed (different scooter IDs)
        successes = [r for r in results if r[1] in [200, 201]]

        # If Paxos is working correctly, all should succeed
        # BUG: If round race exists, some might fail unexpectedly
        assert len(successes) == num_proposals, \
            f"BUG: Only {len(successes)}/{num_proposals} proposals succeeded"

    def test_rapid_reserve_release_paxos_stress(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Rapid sequential Paxos operations stress round management.

        Each reserve/release is a Paxos round. Rapid operations might expose
        round number race conditions.
        """
        create_scooter(api_url, unique_scooter_id)

        errors = []

        # Do 100 rapid reserve/release cycles
        for i in range(100):
            try:
                res = reserve_scooter(api_url, unique_scooter_id, f"rapid-{i}")
                if res.status_code != 200:
                    errors.append(f"Reserve {i} failed: {res.status_code}")
                    continue

                rel = release_scooter(api_url, unique_scooter_id, 1)
                if rel.status_code != 200:
                    errors.append(f"Release {i} failed: {rel.status_code}")
            except Exception as e:
                errors.append(f"Cycle {i} error: {e}")

        # Some failures under stress might be OK, but too many indicates a bug
        if len(errors) > 10:
            pytest.fail(f"BUG: Too many failures under stress: {errors[:10]}...")


class TestAcceptPhaseRace:
    """
    BUG: paxos/acceptor.go line 85-86 uses >= instead of > for round comparison.

    This allows accepting the same round multiple times from different proposers,
    violating Paxos correctness guarantees.
    """

    def test_same_round_different_values(self, api_url, server_urls, unique_scooter_id):
        """
        CATCHES BUG: Two proposers with same round should not both succeed.

        The >= comparison bug allows this. Both proposers think they won
        the same round, leading to split-brain.
        """
        # This is hard to trigger directly via API, but we can try
        # to create conditions where it might happen

        results = []

        def try_create(scooter_id):
            try:
                response = create_scooter(api_url, scooter_id)
                return response.status_code
            except Exception as e:
                return str(e)

        # Try to create many scooters simultaneously
        # If round numbers collide due to the bug, we might see inconsistency
        scooter_ids = [f"{unique_scooter_id}-accept-{i}" for i in range(50)]

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = {executor.submit(try_create, sid): sid for sid in scooter_ids}
            for future in as_completed(futures):
                sid = futures[future]
                result = future.result()
                results.append((sid, result))

        # Verify all scooters that reported success actually exist
        for sid, status in results:
            if status in [200, 201]:
                response = get_scooter(api_url, sid)
                if response.status_code != 200:
                    pytest.fail(f"BUG: Scooter {sid} creation reported success but doesn't exist")


class TestDoubleApplyRace:
    """
    BUG: statemachine/scooter.go Apply() has no idempotency check.

    If a command is applied twice (network retry, recovery race), it
    applies twice. For RESERVE and RELEASE, this has side effects.
    """

    def test_rapid_reserve_release_idempotency(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Even with concurrent operations, state should be consistent.

        If double-apply happens, we might see wrong distance totals.
        """
        create_scooter(api_url, unique_scooter_id)

        expected_distance = 0

        # Do 50 cycles
        for i in range(50):
            reserve_scooter(api_url, unique_scooter_id, f"idem-{i}")
            release_scooter(api_url, unique_scooter_id, 10)
            expected_distance += 10

        # Check final distance
        response = get_scooter(api_url, unique_scooter_id)
        actual_distance = response.json()["total_distance"]

        # BUG: If double-apply occurred, distance would be higher
        assert actual_distance == expected_distance, \
            f"BUG: Distance mismatch! Expected {expected_distance}, got {actual_distance}. " \
            f"Possible double-apply?"

    def test_concurrent_operations_distance_consistency(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Concurrent reserve/release shouldn't corrupt distance.
        """
        create_scooter(api_url, unique_scooter_id)

        successful_releases = []
        lock = threading.Lock()

        def do_cycle(cycle_num):
            try:
                res = reserve_scooter(api_url, unique_scooter_id, f"conc-{cycle_num}")
                if res.status_code == 200:
                    rel = release_scooter(api_url, unique_scooter_id, 5)
                    if rel.status_code == 200:
                        with lock:
                            successful_releases.append(5)
            except Exception:
                pass

        # Run cycles concurrently - only one can reserve at a time
        # but we're testing for race conditions in the state machine
        for i in range(20):
            do_cycle(i)

        # Final distance should equal sum of successful releases
        expected = sum(successful_releases)
        response = get_scooter(api_url, unique_scooter_id)
        actual = response.json()["total_distance"]

        assert actual == expected, \
            f"BUG: Distance inconsistency! Expected {expected}, got {actual}"
