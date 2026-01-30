"""
Integration tests for Paxos consensus.

These tests verify that the Paxos consensus protocol works correctly:
- Single proposals succeed
- Majority is required for commit
- Edge cases are handled

Requires: Docker Compose with all 5 scooter-server replicas running.

Run with: pytest tests/integration/test_consensus.py -v
"""

import pytest
import time
import sys
import os
import requests
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conftest import (
    create_scooter, get_scooter,
    reserve_scooter, release_scooter,
    wait_for_server
)


class TestBasicConsensus:
    """Tests for basic Paxos consensus."""

    def test_single_proposal_succeeds(self, api_url, unique_scooter_id):
        """A single proposal (create scooter) should succeed."""
        response = create_scooter(api_url, unique_scooter_id)

        # Should succeed
        assert response.status_code in [200, 201]

        # Verify it was committed
        response = get_scooter(api_url, unique_scooter_id)
        assert response.status_code == 200
        assert response.json()["id"] == unique_scooter_id

    def test_multiple_sequential_proposals(self, api_url, unique_scooter_id):
        """Multiple sequential proposals all succeed."""
        # Create scooter
        response = create_scooter(api_url, unique_scooter_id)
        assert response.status_code in [200, 201]

        # Reserve (another Paxos round)
        response = reserve_scooter(api_url, unique_scooter_id, "res-1")
        assert response.status_code == 200

        # Release (another Paxos round)
        response = release_scooter(api_url, unique_scooter_id, 100)
        assert response.status_code == 200

        # Verify final state
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()
        assert scooter["is_available"] == True
        assert scooter["total_distance"] == 100

    def test_majority_required(self, server_urls, unique_scooter_id):
        """
        Operations require a majority of servers to succeed.

        With 5 servers, we need 3 to agree (majority).
        This test just verifies the system works with all servers up.
        The failure scenarios are tested in e2e tests.
        """
        # Just verify basic operation works when all servers are up
        response = create_scooter(server_urls[0], unique_scooter_id)
        assert response.status_code in [200, 201]


class TestProposalConflicts:
    """Tests for conflicting proposals."""

    def test_conflicting_creates_one_wins(self, server_urls, unique_scooter_id):
        """
        If two clients try to create the same scooter, one succeeds.

        Note: This is a simplification. In reality with Paxos, both might
        see success but only one value is chosen. Here we test that
        at least one succeeds and the state is consistent.
        """
        # Try to create from two different servers "simultaneously"
        results = []

        def try_create(url):
            try:
                return create_scooter(url, unique_scooter_id)
            except Exception as e:
                return e

        with ThreadPoolExecutor(max_workers=2) as executor:
            future1 = executor.submit(try_create, server_urls[0])
            future2 = executor.submit(try_create, server_urls[1])
            results.append(future1.result())
            results.append(future2.result())

        # At least one should succeed (or both, depending on timing)
        successes = [r for r in results if hasattr(r, 'status_code') and r.status_code in [200, 201]]
        assert len(successes) >= 1

        # Wait for consistency
        time.sleep(2)

        # The scooter should exist with consistent state
        response = get_scooter(server_urls[0], unique_scooter_id)
        assert response.status_code == 200

    def test_reserve_conflict_one_wins(self, api_url, unique_scooter_id):
        """
        If two clients try to reserve the same scooter, only one wins.
        """
        # Create scooter
        create_scooter(api_url, unique_scooter_id)
        time.sleep(1)

        # Try concurrent reservations
        def try_reserve(reservation_id):
            try:
                return reserve_scooter(api_url, unique_scooter_id, reservation_id)
            except Exception as e:
                return e

        with ThreadPoolExecutor(max_workers=2) as executor:
            future1 = executor.submit(try_reserve, "res-client-1")
            future2 = executor.submit(try_reserve, "res-client-2")
            result1 = future1.result()
            result2 = future2.result()

        # At least one should succeed
        success_count = sum(1 for r in [result1, result2]
                          if hasattr(r, 'status_code') and r.status_code == 200)
        assert success_count >= 1

        # Exactly one reservation should be active
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()
        assert scooter["is_available"] == False
        assert scooter["current_reservation_id"] in ["res-client-1", "res-client-2"]


class TestConsensusEdgeCases:
    """Tests for edge cases in consensus."""

    def test_empty_value_proposal(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Proposals with minimal data should still work.
        """
        # Create scooter with just ID (minimal data)
        response = create_scooter(api_url, unique_scooter_id)
        assert response.status_code in [200, 201]

        # Release with 0 distance
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        response = release_scooter(api_url, unique_scooter_id, 0)
        assert response.status_code == 200

    def test_rapid_proposals(self, api_url, unique_scooter_id):
        """
        Rapid sequential proposals should all be ordered correctly.
        """
        create_scooter(api_url, unique_scooter_id)

        # Send rapid reserve/release without waiting
        for i in range(10):
            reserve_scooter(api_url, unique_scooter_id, f"rapid-{i}")
            release_scooter(api_url, unique_scooter_id, 1)

        # Verify final state is consistent
        time.sleep(2)
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()

        # Should have accumulated all distances
        assert scooter["total_distance"] == 10
        assert scooter["is_available"] == True


