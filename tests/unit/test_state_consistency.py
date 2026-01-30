"""
Unit tests for single server state consistency.

These tests verify that a single server maintains consistent state:
- Read-after-write consistency
- Sequential operations apply in order
- Full lifecycle consistency

Run with: pytest tests/unit/test_state_consistency.py -v
"""

import pytest
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conftest import (
    create_scooter, get_scooter, get_all_scooters,
    reserve_scooter, release_scooter
)


class TestReadAfterWrite:
    """Tests for read-after-write consistency."""

    def test_read_after_create(self, api_url, unique_scooter_id):
        """Read immediately after write returns the written value."""
        # Write
        create_scooter(api_url, unique_scooter_id)

        # Read immediately
        response = get_scooter(api_url, unique_scooter_id)

        assert response.status_code == 200
        assert response.json()["id"] == unique_scooter_id

    def test_read_after_reserve(self, api_url, unique_scooter_id, unique_reservation_id):
        """Read immediately after reserve shows reserved state."""
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        # Read immediately
        response = get_scooter(api_url, unique_scooter_id)

        assert response.json()["is_available"] == False
        assert response.json()["current_reservation_id"] == unique_reservation_id

    def test_read_after_release(self, api_url, unique_scooter_id, unique_reservation_id):
        """Read immediately after release shows available state."""
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        release_scooter(api_url, unique_scooter_id, 100)

        # Read immediately
        response = get_scooter(api_url, unique_scooter_id)

        assert response.json()["is_available"] == True
        assert response.json()["total_distance"] == 100


class TestSequentialOperations:
    """Tests that operations apply in order."""

    def test_sequential_creates(self, api_url, unique_scooter_id):
        """Creating scooters in sequence are all visible."""
        scooter_ids = [f"{unique_scooter_id}-{i}" for i in range(5)]

        # Create all scooters in sequence
        for sid in scooter_ids:
            create_scooter(api_url, sid)

        # All should be visible
        response = get_all_scooters(api_url)
        returned_ids = [s["id"] for s in response.json()]

        for sid in scooter_ids:
            assert sid in returned_ids

    def test_sequential_operations_on_same_scooter(self, api_url, unique_scooter_id):
        """Operations on same scooter happen in order."""
        create_scooter(api_url, unique_scooter_id)

        # Sequence: reserve -> release -> reserve -> release
        reserve_scooter(api_url, unique_scooter_id, "res-1")
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["is_available"] == False

        release_scooter(api_url, unique_scooter_id, 10)
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["is_available"] == True
        assert response.json()["total_distance"] == 10

        reserve_scooter(api_url, unique_scooter_id, "res-2")
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["is_available"] == False

        release_scooter(api_url, unique_scooter_id, 20)
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["is_available"] == True
        assert response.json()["total_distance"] == 30  # 10 + 20

    def test_distance_accumulates_correctly(self, api_url, unique_scooter_id):
        """Distance values accumulate in correct order."""
        create_scooter(api_url, unique_scooter_id)

        # Distances: 100, 50, 25, 10, 5
        distances = [100, 50, 25, 10, 5]
        total = 0

        for i, distance in enumerate(distances):
            reserve_scooter(api_url, unique_scooter_id, f"res-{i}")
            release_scooter(api_url, unique_scooter_id, distance)
            total += distance

            # Verify after each release
            response = get_scooter(api_url, unique_scooter_id)
            assert response.json()["total_distance"] == total


class TestFullLifecycleConsistency:
    """Tests for full lifecycle state consistency."""

    def test_create_reserve_release_cycle_consistency(self, api_url, unique_scooter_id):
        """Full lifecycle maintains consistent state."""
        # Create
        create_scooter(api_url, unique_scooter_id)

        for cycle in range(3):
            reservation_id = f"cycle-{cycle}"

            # Before reserve: should be available
            response = get_scooter(api_url, unique_scooter_id)
            scooter = response.json()
            assert scooter["is_available"] == True, f"Cycle {cycle}: should be available before reserve"

            # Reserve
            reserve_scooter(api_url, unique_scooter_id, reservation_id)

            # After reserve: should not be available
            response = get_scooter(api_url, unique_scooter_id)
            scooter = response.json()
            assert scooter["is_available"] == False, f"Cycle {cycle}: should not be available after reserve"
            assert scooter["current_reservation_id"] == reservation_id

            # Release
            release_scooter(api_url, unique_scooter_id, 50)

            # After release: should be available
            response = get_scooter(api_url, unique_scooter_id)
            scooter = response.json()
            assert scooter["is_available"] == True, f"Cycle {cycle}: should be available after release"

        # Final distance check: 3 cycles * 50 = 150
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 150

    def test_multiple_scooters_independent(self, api_url, unique_scooter_id):
        """Operations on one scooter don't affect another."""
        scooter1 = f"{unique_scooter_id}-1"
        scooter2 = f"{unique_scooter_id}-2"

        # Create both
        create_scooter(api_url, scooter1)
        create_scooter(api_url, scooter2)

        # Reserve scooter1 only
        reserve_scooter(api_url, scooter1, "res-1")

        # Scooter1 should be reserved
        response = get_scooter(api_url, scooter1)
        assert response.json()["is_available"] == False

        # Scooter2 should still be available
        response = get_scooter(api_url, scooter2)
        assert response.json()["is_available"] == True

        # Release scooter1 with distance
        release_scooter(api_url, scooter1, 100)

        # Scooter1 should have distance
        response = get_scooter(api_url, scooter1)
        assert response.json()["total_distance"] == 100

        # Scooter2 should still have 0 distance
        response = get_scooter(api_url, scooter2)
        assert response.json()["total_distance"] == 0


class TestRapidOperations:
    """Tests for rapid sequential operations."""

    def test_rapid_create_operations(self, api_url, unique_scooter_id):
        """Rapidly creating scooters all succeed."""
        scooter_ids = []

        # Create 10 scooters as fast as possible
        for i in range(10):
            sid = f"{unique_scooter_id}-rapid-{i}"
            response = create_scooter(api_url, sid)
            assert response.status_code in [200, 201]
            scooter_ids.append(sid)

        # All should exist
        response = get_all_scooters(api_url)
        returned_ids = [s["id"] for s in response.json()]

        for sid in scooter_ids:
            assert sid in returned_ids

    def test_rapid_reserve_release(self, api_url, unique_scooter_id):
        """Rapid reserve/release cycles work correctly."""
        create_scooter(api_url, unique_scooter_id)

        # Do 10 rapid cycles
        for i in range(10):
            reserve_scooter(api_url, unique_scooter_id, f"rapid-{i}")
            release_scooter(api_url, unique_scooter_id, 1)

        # Should have 10 total distance
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 10
        assert response.json()["is_available"] == True
