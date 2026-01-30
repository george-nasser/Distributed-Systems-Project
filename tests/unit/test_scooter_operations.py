"""
Unit tests for scooter state machine operations.

These tests verify the business logic of scooter operations:
- Initial state of new scooters
- State changes on reserve/release
- Distance accumulation
- Reservation ID tracking

Run with: pytest tests/unit/test_scooter_operations.py -v
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conftest import (
    create_scooter, get_scooter,
    reserve_scooter, release_scooter
)


class TestScooterInitialState:
    """Tests for initial scooter state."""

    def test_scooter_initial_state(self, api_url, unique_scooter_id):
        """New scooter is available with distance=0."""
        create_scooter(api_url, unique_scooter_id)

        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()

        # Check initial state
        assert scooter["is_available"] == True
        assert scooter["total_distance"] == 0
        # Reservation ID should be empty or None
        assert scooter.get("current_reservation_id", "") in ["", None]


class TestReserveChangesState:
    """Tests for state changes on reservation."""

    def test_reserve_changes_availability(self, api_url, unique_scooter_id, unique_reservation_id):
        """Reserve sets is_available=false."""
        create_scooter(api_url, unique_scooter_id)

        # Check it's available initially
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["is_available"] == True

        # Reserve it
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        # Check it's no longer available
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()
        assert scooter["is_available"] == False


class TestReleaseChangesState:
    """Tests for state changes on release."""

    def test_release_changes_availability(self, api_url, unique_scooter_id, unique_reservation_id):
        """Release sets is_available=true."""
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        # Verify it's reserved
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["is_available"] == False

        # Release it
        release_scooter(api_url, unique_scooter_id, 50)

        # Check it's available again
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()
        assert scooter["is_available"] == True


class TestDistanceAccumulation:
    """Tests for distance tracking."""

    def test_distance_accumulation(self, api_url, unique_scooter_id, unique_reservation_id):
        """Multiple releases add up distance correctly."""
        create_scooter(api_url, unique_scooter_id)

        # First rental: 100 distance
        reserve_scooter(api_url, unique_scooter_id, f"{unique_reservation_id}-1")
        release_scooter(api_url, unique_scooter_id, 100)

        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 100

        # Second rental: 50 distance
        reserve_scooter(api_url, unique_scooter_id, f"{unique_reservation_id}-2")
        release_scooter(api_url, unique_scooter_id, 50)

        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 150

        # Third rental: 75 distance
        reserve_scooter(api_url, unique_scooter_id, f"{unique_reservation_id}-3")
        release_scooter(api_url, unique_scooter_id, 75)

        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 225

    def test_distance_starts_at_zero(self, api_url, unique_scooter_id):
        """New scooter has zero distance."""
        create_scooter(api_url, unique_scooter_id)

        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 0


class TestReservationIdTracking:
    """Tests for reservation ID management."""

    def test_reservation_id_set_on_reserve(self, api_url, unique_scooter_id, unique_reservation_id):
        """Reservation ID is stored when scooter is reserved."""
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()

        assert scooter["current_reservation_id"] == unique_reservation_id

    def test_reservation_id_cleared_on_release(self, api_url, unique_scooter_id, unique_reservation_id):
        """Reservation ID is cleared when scooter is released."""
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        # Verify it's set
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["current_reservation_id"] == unique_reservation_id

        # Release
        release_scooter(api_url, unique_scooter_id, 50)

        # Verify it's cleared
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()
        assert scooter.get("current_reservation_id", "") in ["", None]

    def test_different_reservation_ids_per_rental(self, api_url, unique_scooter_id):
        """Each rental can have a different reservation ID."""
        create_scooter(api_url, unique_scooter_id)

        # First rental with ID "rental-1"
        reserve_scooter(api_url, unique_scooter_id, "rental-1")
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["current_reservation_id"] == "rental-1"
        release_scooter(api_url, unique_scooter_id, 10)

        # Second rental with ID "rental-2"
        reserve_scooter(api_url, unique_scooter_id, "rental-2")
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["current_reservation_id"] == "rental-2"
        release_scooter(api_url, unique_scooter_id, 20)


class TestFullLifecycle:
    """Tests for complete scooter lifecycle."""

    def test_create_reserve_release_cycle(self, api_url, unique_scooter_id, unique_reservation_id):
        """Complete lifecycle: create -> reserve -> release."""
        # 1. Create
        response = create_scooter(api_url, unique_scooter_id)
        assert response.status_code in [200, 201]

        # Verify created state
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()
        assert scooter["is_available"] == True
        assert scooter["total_distance"] == 0

        # 2. Reserve
        response = reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        assert response.status_code == 200

        # Verify reserved state
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()
        assert scooter["is_available"] == False
        assert scooter["current_reservation_id"] == unique_reservation_id

        # 3. Release
        response = release_scooter(api_url, unique_scooter_id, 100)
        assert response.status_code == 200

        # Verify final state
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()
        assert scooter["is_available"] == True
        assert scooter["total_distance"] == 100
        assert scooter.get("current_reservation_id", "") in ["", None]

    def test_multiple_cycles(self, api_url, unique_scooter_id):
        """Scooter can go through multiple reserve/release cycles."""
        create_scooter(api_url, unique_scooter_id)

        # Do 5 rental cycles
        for i in range(5):
            reservation_id = f"rental-cycle-{i}"
            distance = (i + 1) * 10  # 10, 20, 30, 40, 50

            # Reserve
            reserve_scooter(api_url, unique_scooter_id, reservation_id)

            response = get_scooter(api_url, unique_scooter_id)
            assert response.json()["is_available"] == False

            # Release
            release_scooter(api_url, unique_scooter_id, distance)

            response = get_scooter(api_url, unique_scooter_id)
            assert response.json()["is_available"] == True

        # Check final distance: 10+20+30+40+50 = 150
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 150
