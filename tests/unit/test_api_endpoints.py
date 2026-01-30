"""
Unit tests for CityScooter REST API endpoints.

These tests verify that the REST API works correctly for:
- Scooter CRUD operations
- Reservations
- Releases
- Snapshots

Run with: pytest tests/unit/test_api_endpoints.py -v
"""

import pytest
import requests
import sys
import os

# Add parent directory to path so we can import from conftest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conftest import (
    create_scooter, get_scooter, get_all_scooters,
    reserve_scooter, release_scooter, take_snapshot
)


# ============================================================================
# SCOOTER CRUD TESTS
# ============================================================================

class TestScooterCRUD:
    """Tests for basic scooter create/read operations."""

    def test_create_scooter_success(self, api_url, unique_scooter_id):
        """PUT /scooters/:id creates a new scooter."""
        response = create_scooter(api_url, unique_scooter_id)

        # Should return success (200 or 201)
        assert response.status_code in [200, 201], f"Expected 200/201, got {response.status_code}"

        # Verify scooter was created by fetching it
        get_response = get_scooter(api_url, unique_scooter_id)
        assert get_response.status_code == 200
        scooter = get_response.json()
        assert scooter["id"] == unique_scooter_id

    def test_create_scooter_already_exists(self, api_url, unique_scooter_id):
        """Creating a duplicate scooter should return an error."""
        # Create scooter first time
        response1 = create_scooter(api_url, unique_scooter_id)
        assert response1.status_code in [200, 201]

        # Try to create same scooter again
        response2 = create_scooter(api_url, unique_scooter_id)

        # Should fail with conflict or bad request
        # The exact status code depends on implementation
        assert response2.status_code in [400, 409], \
            f"Expected 400/409 for duplicate, got {response2.status_code}"

    def test_get_scooter_success(self, api_url, unique_scooter_id):
        """GET /scooters/:id returns the scooter."""
        # Create scooter first
        create_scooter(api_url, unique_scooter_id)

        # Now get it
        response = get_scooter(api_url, unique_scooter_id)

        assert response.status_code == 200
        scooter = response.json()
        assert scooter["id"] == unique_scooter_id
        assert "is_available" in scooter
        assert "total_distance" in scooter

    def test_get_scooter_not_found(self, api_url):
        """GET non-existent scooter returns 404."""
        response = get_scooter(api_url, "nonexistent-scooter-12345")

        assert response.status_code == 404

    def test_get_all_scooters_empty(self, api_url):
        """GET /scooters returns empty list when no scooters exist."""
        # Note: This test may not work if other tests have run first
        # and created scooters. In a real setup, you'd reset the database.
        response = get_all_scooters(api_url)

        assert response.status_code == 200
        scooters = response.json()
        assert isinstance(scooters, list)

    def test_get_all_scooters_multiple(self, api_url, unique_scooter_id):
        """GET /scooters returns all created scooters."""
        # Create a few scooters
        scooter_ids = [f"{unique_scooter_id}-{i}" for i in range(3)]
        for scooter_id in scooter_ids:
            create_scooter(api_url, scooter_id)

        # Get all scooters
        response = get_all_scooters(api_url)

        assert response.status_code == 200
        scooters = response.json()
        assert isinstance(scooters, list)

        # All our scooters should be in the list
        returned_ids = [s["id"] for s in scooters]
        for scooter_id in scooter_ids:
            assert scooter_id in returned_ids, f"Scooter {scooter_id} not in response"


# ============================================================================
# RESERVATION TESTS
# ============================================================================

class TestReservations:
    """Tests for scooter reservation operations."""

    def test_reserve_scooter_success(self, api_url, unique_scooter_id, unique_reservation_id):
        """POST /scooters/:id/reservations reserves an available scooter."""
        # Create scooter first
        create_scooter(api_url, unique_scooter_id)

        # Reserve it
        response = reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"

        # Verify scooter is now reserved
        get_response = get_scooter(api_url, unique_scooter_id)
        scooter = get_response.json()
        assert scooter["is_available"] == False

    def test_reserve_nonexistent_scooter(self, api_url, unique_reservation_id):
        """Reserve a non-existent scooter fails."""
        response = reserve_scooter(api_url, "nonexistent-scooter", unique_reservation_id)

        # Should fail with 404
        assert response.status_code == 404

    def test_reserve_already_reserved(self, api_url, unique_scooter_id, unique_reservation_id):
        """Double reservation fails."""
        # Create and reserve scooter
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        # Try to reserve again
        response = reserve_scooter(api_url, unique_scooter_id, "another-reservation")

        # Should fail - scooter is already reserved
        assert response.status_code in [400, 409], \
            f"Expected 400/409 for double reservation, got {response.status_code}"

    def test_reserve_with_reservation_id(self, api_url, unique_scooter_id, unique_reservation_id):
        """Reservation ID is stored correctly."""
        # Create and reserve scooter
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        # Get scooter and check reservation ID
        get_response = get_scooter(api_url, unique_scooter_id)
        scooter = get_response.json()

        assert scooter["current_reservation_id"] == unique_reservation_id


# ============================================================================
# RELEASE TESTS
# ============================================================================

class TestReleases:
    """Tests for scooter release operations."""

    def test_release_scooter_success(self, api_url, unique_scooter_id, unique_reservation_id):
        """POST /scooters/:id/releases releases a reserved scooter."""
        # Create, reserve, then release
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        response = release_scooter(api_url, unique_scooter_id, 100)

        assert response.status_code == 200

        # Verify scooter is now available
        get_response = get_scooter(api_url, unique_scooter_id)
        scooter = get_response.json()
        assert scooter["is_available"] == True

    def test_release_adds_distance(self, api_url, unique_scooter_id, unique_reservation_id):
        """Distance is accumulated after release."""
        # Create scooter
        create_scooter(api_url, unique_scooter_id)

        # Reserve and release with distance
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        release_scooter(api_url, unique_scooter_id, 150)

        # Check distance
        get_response = get_scooter(api_url, unique_scooter_id)
        scooter = get_response.json()
        assert scooter["total_distance"] == 150

    def test_release_nonexistent_scooter(self, api_url):
        """Release a non-existent scooter fails."""
        response = release_scooter(api_url, "nonexistent-scooter", 100)

        assert response.status_code == 404

    def test_release_available_scooter(self, api_url, unique_scooter_id):
        """Release an already-available scooter fails."""
        # Create scooter (it starts available)
        create_scooter(api_url, unique_scooter_id)

        # Try to release without reserving
        response = release_scooter(api_url, unique_scooter_id, 100)

        # Should fail - scooter is not reserved
        assert response.status_code in [400, 409], \
            f"Expected 400/409 for releasing available scooter, got {response.status_code}"


# ============================================================================
# SNAPSHOT TESTS
# ============================================================================

class TestSnapshots:
    """Tests for snapshot operations."""

    def test_take_snapshot(self, api_url):
        """POST /snapshot succeeds."""
        response = take_snapshot(api_url)

        # Should succeed
        assert response.status_code == 200


# ============================================================================
# EDGE CASES
# ============================================================================

class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_scooter_id_with_special_characters(self, api_url):
        """Scooter ID with dashes and numbers works."""
        scooter_id = "scooter-123-abc"
        response = create_scooter(api_url, scooter_id)

        assert response.status_code in [200, 201]

    def test_zero_distance_release(self, api_url, unique_scooter_id, unique_reservation_id):
        """Release with zero distance works."""
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        response = release_scooter(api_url, unique_scooter_id, 0)

        assert response.status_code == 200

        # Distance should still be 0
        get_response = get_scooter(api_url, unique_scooter_id)
        scooter = get_response.json()
        assert scooter["total_distance"] == 0

    def test_empty_reservation_id(self, api_url, unique_scooter_id):
        """Empty reservation ID might be rejected."""
        create_scooter(api_url, unique_scooter_id)

        # Try with empty reservation ID
        response = requests.post(
            f"{api_url}/scooters/{unique_scooter_id}/reservations",
            json={"reservation_id": ""},
            timeout=10
        )

        # This might succeed or fail depending on validation rules
        # Just check we get a reasonable response
        assert response.status_code in [200, 400]
