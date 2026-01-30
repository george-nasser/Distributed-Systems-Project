"""
Tests for observable Paxos consensus behavior via REST API.

These tests verify consensus properties that we can observe through the API:
- Single value consensus works
- Consensus safety (decided values don't change)
- Dueling proposers resolve correctly
- Quorum behavior

We can't test internal Paxos mechanics (proposal numbers, acceptor state, etc.)
from the API, but we can observe the outcomes.

Run with: pytest tests/paxos/test_paxos_observable.py -v
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
    wait_for_replication, wait_for_server
)


class TestSingleValueConsensus:
    """
    Tests for basic single-value consensus.

    These verify that a single proposer can get consensus and
    the value is visible everywhere.
    """

    def test_consensus_single_proposer(self, api_url, server_urls, unique_scooter_id):
        """
        One write succeeds and all nodes eventually see it.

        This is the most basic consensus test - one client writes,
        and everyone agrees on the value.
        """
        # Single proposer creates a scooter
        response = create_scooter(api_url, unique_scooter_id)
        assert response.status_code in [200, 201], \
            f"Create failed: {response.status_code}"

        # Wait a bit for replication
        time.sleep(2)

        # All servers should see the scooter
        servers_with_scooter = 0
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    servers_with_scooter += 1
                    # Should have the same ID
                    assert response.json()["id"] == unique_scooter_id
            except Exception:
                pass

        # At least a majority should have it (quorum)
        assert servers_with_scooter >= 3, \
            f"Only {servers_with_scooter}/5 servers have the scooter"

    def test_consensus_value_persistence(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Once consensus is reached, the value survives.

        After a write completes, multiple reads should all see it.
        """
        # Create and modify the scooter
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        release_scooter(api_url, unique_scooter_id, 100)

        # Do multiple reads - all should see the same state
        for i in range(10):
            response = get_scooter(api_url, unique_scooter_id)
            assert response.status_code == 200
            scooter = response.json()
            assert scooter["is_available"] == True
            assert scooter["total_distance"] == 100
            time.sleep(0.1)

    def test_consensus_concurrent_proposers_one_wins(self, api_url, unique_scooter_id):
        """
        Two concurrent writes to the same key - one value must be decided.

        When two clients try to write at the same time, Paxos ensures
        exactly one value is chosen.
        """
        results = []

        def try_create():
            try:
                return create_scooter(api_url, unique_scooter_id)
            except Exception as e:
                return e

        # Launch two concurrent creates
        with ThreadPoolExecutor(max_workers=2) as executor:
            f1 = executor.submit(try_create)
            f2 = executor.submit(try_create)
            results.append(f1.result())
            results.append(f2.result())

        # At least one should succeed
        successes = [r for r in results if hasattr(r, 'status_code') and r.status_code in [200, 201]]
        assert len(successes) >= 1, "No create succeeded"

        # The scooter should exist with consistent state
        time.sleep(1)
        response = get_scooter(api_url, unique_scooter_id)
        assert response.status_code == 200
        assert response.json()["id"] == unique_scooter_id


class TestConsensusSafety:
    """
    Tests for consensus safety properties.

    Safety means: once a value is decided, it cannot change.
    """

    def test_decided_value_never_changes(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Once a value is set via consensus, it stays set.

        After writing is_available=False, we should never read True
        until we explicitly release.
        """
        create_scooter(api_url, unique_scooter_id)

        # Reserve the scooter (sets is_available=False)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        # Multiple reads should all see reserved state
        # This tests that the decided value doesn't flip
        for i in range(20):
            response = get_scooter(api_url, unique_scooter_id)
            scooter = response.json()
            assert scooter["is_available"] == False, \
                f"Read {i}: decided value changed unexpectedly"
            assert scooter["current_reservation_id"] == unique_reservation_id
            time.sleep(0.05)

    def test_all_nodes_agree_on_value(self, server_urls, unique_scooter_id, unique_reservation_id):
        """
        All servers return the same value after consensus.
        """
        # Create and reserve on one server
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(1)
        reserve_scooter(server_urls[0], unique_scooter_id, unique_reservation_id)
        release_scooter(server_urls[0], unique_scooter_id, 42)

        # Wait for replication
        time.sleep(3)

        # Query all servers
        values = []
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    values.append(response.json()["total_distance"])
            except Exception:
                pass

        # All responding servers should agree
        if len(values) >= 2:
            assert all(v == values[0] for v in values), \
                f"Servers disagree on value: {values}"

    def test_no_partial_state(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        We should never see incomplete/partial writes.

        A scooter should either:
        - Not exist (404)
        - Exist with complete, consistent state

        We should never see weird partial states.
        """
        # Create scooter
        create_scooter(api_url, unique_scooter_id)

        # Do some state changes
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        release_scooter(api_url, unique_scooter_id, 100)

        # Multiple reads - each should have complete state
        for i in range(20):
            response = get_scooter(api_url, unique_scooter_id)
            assert response.status_code == 200

            scooter = response.json()

            # All fields should be present
            assert "id" in scooter
            assert "is_available" in scooter
            assert "total_distance" in scooter

            # State should be consistent
            # If available, no reservation
            if scooter["is_available"]:
                # current_reservation_id should be empty or absent
                res_id = scooter.get("current_reservation_id", "")
                assert res_id == "" or res_id is None, \
                    f"Inconsistent: available but has reservation {res_id}"

            time.sleep(0.05)


class TestDuelingProposers:
    """
    Tests for when multiple proposers compete.

    In Paxos, competing proposers can cause delays but must
    eventually reach consensus on a single value.
    """

    def test_concurrent_conflicting_writes(self, server_urls, unique_scooter_id):
        """
        Two clients write the same key from different servers.

        One must win, and the system stays consistent.
        """
        results = []

        def try_create(url):
            try:
                return (url, create_scooter(url, unique_scooter_id))
            except Exception as e:
                return (url, e)

        # Create from two different servers simultaneously
        with ThreadPoolExecutor(max_workers=2) as executor:
            f1 = executor.submit(try_create, server_urls[0])
            f2 = executor.submit(try_create, server_urls[1])
            results.append(f1.result())
            results.append(f2.result())

        # At least one should succeed
        successes = [r for r in results if hasattr(r[1], 'status_code') and r[1].status_code in [200, 201]]
        assert len(successes) >= 1

        # Wait for consistency
        time.sleep(2)

        # All servers should agree on existence and state
        scooter_states = []
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    scooter_states.append(response.json())
            except Exception:
                pass

        # All should have the same state
        if len(scooter_states) >= 2:
            first = scooter_states[0]
            for state in scooter_states[1:]:
                assert state["id"] == first["id"]
                assert state["is_available"] == first["is_available"]

    def test_rapid_conflicting_proposals(self, api_url, unique_scooter_id):
        """
        Many rapid proposals - system should stay consistent.

        This simulates a scenario where multiple clients are
        hammering the system.
        """
        # Create the scooter
        create_scooter(api_url, unique_scooter_id)
        time.sleep(1)

        results = []
        lock = threading.Lock()

        def try_reserve(client_id):
            try:
                response = reserve_scooter(api_url, unique_scooter_id, f"rapid-{client_id}")
                with lock:
                    results.append((client_id, response.status_code))
            except Exception as e:
                with lock:
                    results.append((client_id, str(e)))

        # 10 concurrent reservation attempts
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(try_reserve, i) for i in range(10)]
            for f in as_completed(futures):
                pass

        # One should have won
        successes = [r for r in results if r[1] == 200]
        assert len(successes) >= 1, "No reservation succeeded"

        # The scooter should be reserved
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["is_available"] == False
        # Should have one of the rapid-X reservations
        res_id = response.json()["current_reservation_id"]
        assert res_id.startswith("rapid-")

    def test_dueling_proposers_eventually_succeed(self, api_url, unique_scooter_id):
        """
        Despite conflicts, operations eventually succeed.

        This tests liveness - the system makes progress even
        with concurrent proposers.
        """
        create_scooter(api_url, unique_scooter_id)
        time.sleep(0.5)

        # Do 5 rounds of reserve/release with some concurrent pressure
        successful_rounds = 0

        for round_num in range(5):
            # Try to reserve
            res_response = reserve_scooter(api_url, unique_scooter_id, f"duel-{round_num}")
            if res_response.status_code == 200:
                # Release
                rel_response = release_scooter(api_url, unique_scooter_id, 10)
                if rel_response.status_code == 200:
                    successful_rounds += 1
            time.sleep(0.2)

        # Should complete at least some rounds
        assert successful_rounds >= 3, \
            f"Only {successful_rounds}/5 rounds succeeded"

        # Final state should reflect successful operations
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == successful_rounds * 10


class TestQuorumBehavior:
    """
    Tests for quorum-based consensus.

    With 5 servers, quorum is 3. We can't directly control which
    servers respond, but we can observe quorum-based behavior.
    """

    def test_write_succeeds_with_all_servers(self, api_url, unique_scooter_id):
        """
        Write succeeds when all servers are up.

        This is the baseline - full cluster should work.
        """
        response = create_scooter(api_url, unique_scooter_id)
        assert response.status_code in [200, 201]

        response = get_scooter(api_url, unique_scooter_id)
        assert response.status_code == 200

    def test_read_after_quorum_write(self, server_urls, unique_scooter_id):
        """
        After a write succeeds (quorum acknowledged), reads should see it.
        """
        # Write
        response = create_scooter(server_urls[0], unique_scooter_id)
        assert response.status_code in [200, 201], "Write should succeed"

        # Wait for replication
        time.sleep(2)

        # Multiple reads from different servers
        successful_reads = 0
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    successful_reads += 1
            except Exception:
                pass

        # At least a quorum should have the value
        assert successful_reads >= 3, \
            f"Only {successful_reads} servers have the value"

    def test_majority_servers_agree(self, server_urls, unique_scooter_id, unique_reservation_id):
        """
        After operations complete, majority of servers agree.
        """
        # Do operations
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(1)
        reserve_scooter(server_urls[0], unique_scooter_id, unique_reservation_id)
        release_scooter(server_urls[0], unique_scooter_id, 55)

        # Wait for replication
        time.sleep(3)

        # Check all servers
        distances = []
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    distances.append(response.json()["total_distance"])
            except Exception:
                pass

        # At least 3 (majority) should have the same value
        if len(distances) >= 3:
            # Count how many have the expected value
            expected = 55
            matching = sum(1 for d in distances if d == expected)
            assert matching >= 3, \
                f"Only {matching} servers have correct distance 55: {distances}"


class TestConsensusWithStateChanges:
    """
    Tests for consensus on state-changing operations.
    """

    def test_reserve_requires_consensus(self, api_url, server_urls, unique_scooter_id, unique_reservation_id):
        """
        Reservation (state change) requires consensus.

        After successful reserve, all servers should see reserved state.
        """
        create_scooter(api_url, unique_scooter_id)
        time.sleep(1)

        # Reserve
        response = reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        assert response.status_code == 200

        # Wait for replication
        time.sleep(2)

        # Check all servers
        reserved_count = 0
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    if not response.json()["is_available"]:
                        reserved_count += 1
            except Exception:
                pass

        # At least majority should show reserved
        assert reserved_count >= 3, \
            f"Only {reserved_count} servers show reserved state"

    def test_release_requires_consensus(self, api_url, server_urls, unique_scooter_id, unique_reservation_id):
        """
        Release (state change with data) requires consensus.

        After successful release, all servers should see new distance.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        # Release with specific distance
        response = release_scooter(api_url, unique_scooter_id, 123)
        assert response.status_code == 200

        # Wait for replication
        time.sleep(2)

        # Check all servers
        correct_distance_count = 0
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    if response.json()["total_distance"] == 123:
                        correct_distance_count += 1
            except Exception:
                pass

        # At least majority should have correct distance
        assert correct_distance_count >= 3, \
            f"Only {correct_distance_count} servers have correct distance"

    def test_sequence_of_consensus_rounds(self, api_url, unique_scooter_id):
        """
        Multiple consensus rounds in sequence all work.

        Each operation is a separate Paxos round.
        """
        # Create (round 1)
        response = create_scooter(api_url, unique_scooter_id)
        assert response.status_code in [200, 201]

        total_distance = 0
        for i in range(5):
            # Reserve (round N)
            response = reserve_scooter(api_url, unique_scooter_id, f"seq-round-{i}")
            assert response.status_code == 200

            # Verify reserved
            response = get_scooter(api_url, unique_scooter_id)
            assert response.json()["is_available"] == False

            # Release (round N+1)
            distance = (i + 1) * 10
            response = release_scooter(api_url, unique_scooter_id, distance)
            assert response.status_code == 200
            total_distance += distance

            # Verify released with correct distance
            response = get_scooter(api_url, unique_scooter_id)
            assert response.json()["is_available"] == True
            assert response.json()["total_distance"] == total_distance
