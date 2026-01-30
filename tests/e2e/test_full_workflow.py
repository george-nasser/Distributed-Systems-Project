"""
End-to-end tests for complete user workflows.

These tests verify complete user scenarios:
- Full rental lifecycle
- Multiple rentals
- Fleet management

Requires: Full Docker Compose stack running.

Run with: pytest tests/e2e/test_full_workflow.py -v
"""

import pytest
import time
import sys
import os
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conftest import (
    create_scooter, get_scooter, get_all_scooters,
    reserve_scooter, release_scooter,
    wait_for_server, wait_for_replication
)


class TestScooterRentalFlow:
    """Tests for complete scooter rental workflows."""

    def test_complete_rental_cycle(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Test the full rental cycle: Create -> Reserve -> Release

        This is the basic happy path for renting a scooter.
        """
        # Step 1: Create a new scooter
        response = create_scooter(api_url, unique_scooter_id)
        assert response.status_code in [200, 201], "Failed to create scooter"

        # Verify scooter is available
        response = get_scooter(api_url, unique_scooter_id)
        assert response.status_code == 200
        scooter = response.json()
        assert scooter["is_available"] == True
        assert scooter["total_distance"] == 0

        # Step 2: Reserve the scooter
        response = reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        assert response.status_code == 200, "Failed to reserve scooter"

        # Verify scooter is now reserved
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()
        assert scooter["is_available"] == False
        assert scooter["current_reservation_id"] == unique_reservation_id

        # Step 3: Use the scooter (simulated by releasing with distance)
        distance_traveled = 250
        response = release_scooter(api_url, unique_scooter_id, distance_traveled)
        assert response.status_code == 200, "Failed to release scooter"

        # Verify final state
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()
        assert scooter["is_available"] == True
        assert scooter["total_distance"] == distance_traveled
        # Reservation ID should be cleared
        assert scooter.get("current_reservation_id", "") in ["", None]

    def test_multiple_rentals_same_scooter(self, api_url, unique_scooter_id):
        """
        Test multiple rental cycles on the same scooter.

        Simulates a scooter being used by different customers.
        """
        create_scooter(api_url, unique_scooter_id)

        total_distance = 0

        # Simulate 5 different customers renting
        customers = ["alice", "bob", "charlie", "diana", "eve"]
        distances = [100, 50, 200, 75, 125]

        for customer, distance in zip(customers, distances):
            reservation_id = f"rental-{customer}"

            # Customer reserves
            response = reserve_scooter(api_url, unique_scooter_id, reservation_id)
            assert response.status_code == 200, f"{customer} failed to reserve"

            # Verify reserved
            response = get_scooter(api_url, unique_scooter_id)
            assert response.json()["is_available"] == False

            # Customer returns scooter
            response = release_scooter(api_url, unique_scooter_id, distance)
            assert response.status_code == 200, f"{customer} failed to release"

            total_distance += distance

            # Verify available for next customer
            response = get_scooter(api_url, unique_scooter_id)
            scooter = response.json()
            assert scooter["is_available"] == True
            assert scooter["total_distance"] == total_distance

        # Final check
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()
        expected_total = sum(distances)  # 550
        assert scooter["total_distance"] == expected_total

    def test_multiple_scooters_multiple_users(self, api_url, unique_scooter_id):
        """
        Test parallel usage of multiple scooters by multiple users.
        """
        # Create a small fleet
        scooter_ids = [f"{unique_scooter_id}-{i}" for i in range(5)]
        for sid in scooter_ids:
            create_scooter(api_url, sid)

        # All should be available
        for sid in scooter_ids:
            response = get_scooter(api_url, sid)
            assert response.json()["is_available"] == True

        # Reserve all scooters (different users)
        for i, sid in enumerate(scooter_ids):
            reserve_scooter(api_url, sid, f"user-{i}")

        # All should be reserved
        for sid in scooter_ids:
            response = get_scooter(api_url, sid)
            assert response.json()["is_available"] == False

        # Release all with different distances
        for i, sid in enumerate(scooter_ids):
            release_scooter(api_url, sid, (i + 1) * 100)

        # Verify final states
        for i, sid in enumerate(scooter_ids):
            response = get_scooter(api_url, sid)
            scooter = response.json()
            assert scooter["is_available"] == True
            assert scooter["total_distance"] == (i + 1) * 100


class TestFleetManagement:
    """Tests for fleet management scenarios."""

    def test_create_fleet_of_scooters(self, api_url, unique_scooter_id):
        """
        Test creating a fleet of scooters.
        """
        fleet_size = 20
        scooter_ids = [f"{unique_scooter_id}-fleet-{i}" for i in range(fleet_size)]

        # Create all scooters
        for sid in scooter_ids:
            response = create_scooter(api_url, sid)
            assert response.status_code in [200, 201]

        # Verify all exist
        response = get_all_scooters(api_url)
        all_scooters = response.json()
        all_ids = [s["id"] for s in all_scooters]

        for sid in scooter_ids:
            assert sid in all_ids, f"Scooter {sid} not in fleet"

    def test_fleet_availability_tracking(self, api_url, unique_scooter_id):
        """
        Test tracking which scooters in a fleet are available.
        """
        # Create small fleet
        scooter_ids = [f"{unique_scooter_id}-track-{i}" for i in range(10)]
        for sid in scooter_ids:
            create_scooter(api_url, sid)

        # Reserve some (odd-numbered)
        reserved_ids = scooter_ids[1::2]  # indices 1, 3, 5, 7, 9
        for i, sid in enumerate(reserved_ids):
            reserve_scooter(api_url, sid, f"res-{i}")

        # Check availability
        response = get_all_scooters(api_url)
        all_scooters = response.json()

        available_count = 0
        reserved_count = 0

        for scooter in all_scooters:
            if scooter["id"] in scooter_ids:
                if scooter["is_available"]:
                    available_count += 1
                else:
                    reserved_count += 1

        # Should have 5 available, 5 reserved
        assert available_count == 5
        assert reserved_count == 5

    def test_fleet_total_distance(self, api_url, unique_scooter_id):
        """
        Test calculating total distance across the fleet.
        """
        # Create fleet
        scooter_ids = [f"{unique_scooter_id}-dist-{i}" for i in range(5)]
        for sid in scooter_ids:
            create_scooter(api_url, sid)

        # Use each scooter
        for i, sid in enumerate(scooter_ids):
            reserve_scooter(api_url, sid, f"use-{i}")
            release_scooter(api_url, sid, (i + 1) * 50)

        # Calculate total fleet distance
        response = get_all_scooters(api_url)
        all_scooters = response.json()

        total_fleet_distance = sum(
            s["total_distance"] for s in all_scooters
            if s["id"] in scooter_ids
        )

        # Expected: 50 + 100 + 150 + 200 + 250 = 750
        assert total_fleet_distance == 750


class TestUserJourneys:
    """Tests for realistic user journey scenarios."""

    def test_user_finds_and_rents_scooter(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Simulate a user finding and renting a scooter.
        """
        # Setup: Create some scooters, some already in use
        scooter_ids = [f"{unique_scooter_id}-avail-{i}" for i in range(5)]
        for sid in scooter_ids:
            create_scooter(api_url, sid)

        # Reserve first 3 scooters (in use)
        for sid in scooter_ids[:3]:
            reserve_scooter(api_url, sid, f"other-user-{sid}")

        # User scenario: Find an available scooter
        response = get_all_scooters(api_url)
        all_scooters = response.json()

        # Find first available scooter
        available_scooter = None
        for scooter in all_scooters:
            if scooter["id"] in scooter_ids and scooter["is_available"]:
                available_scooter = scooter["id"]
                break

        assert available_scooter is not None, "No available scooters found"

        # User rents the scooter
        response = reserve_scooter(api_url, available_scooter, unique_reservation_id)
        assert response.status_code == 200

        # User rides and returns
        response = release_scooter(api_url, available_scooter, 150)
        assert response.status_code == 200

    def test_user_tries_unavailable_scooter(self, api_url, unique_scooter_id):
        """
        Simulate a user trying to rent an already-reserved scooter.
        """
        # Create scooter and have someone else reserve it
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, "first-user")

        # Another user tries to reserve
        response = reserve_scooter(api_url, unique_scooter_id, "second-user")

        # Should fail
        assert response.status_code in [400, 409]

    def test_user_completes_trip_adds_distance(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Verify that completing a trip properly adds to total distance.
        """
        create_scooter(api_url, unique_scooter_id)

        # Initial distance check
        response = get_scooter(api_url, unique_scooter_id)
        initial_distance = response.json()["total_distance"]
        assert initial_distance == 0

        # Complete a trip
        trip_distance = 327
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        release_scooter(api_url, unique_scooter_id, trip_distance)

        # Verify distance added
        response = get_scooter(api_url, unique_scooter_id)
        final_distance = response.json()["total_distance"]
        assert final_distance == trip_distance


class TestMultiServerWorkflow:
    """Tests for workflows spanning multiple servers."""

    def test_workflow_through_load_balancer(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Full workflow going through the load balancer (Traefik).

        Each request might hit a different server.
        """
        # All these go through the load balancer
        response = create_scooter(api_url, unique_scooter_id)
        assert response.status_code in [200, 201]

        response = reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        assert response.status_code == 200

        response = release_scooter(api_url, unique_scooter_id, 100)
        assert response.status_code == 200

        # Final read should return consistent state
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()
        assert scooter["total_distance"] == 100
        assert scooter["is_available"] == True

    def test_reads_consistent_across_servers(self, server_urls, unique_scooter_id, unique_reservation_id):
        """
        Reads from different servers should show consistent state.
        """
        # Create and modify
        create_scooter(server_urls[0], unique_scooter_id)
        reserve_scooter(server_urls[0], unique_scooter_id, unique_reservation_id)
        release_scooter(server_urls[0], unique_scooter_id, 200)

        # Wait for replication
        time.sleep(3)

        # Read from multiple servers
        states = []
        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    states.append(response.json()["total_distance"])
            except Exception:
                pass

        # All should show the same distance
        if len(states) > 1:
            assert all(s == states[0] for s in states), f"Inconsistent states: {states}"
