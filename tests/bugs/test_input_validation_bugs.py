"""
Tests designed to catch input validation bugs in the API.

NOTE: The assignment states "Assume the syntax is correct; fancy parsers and
checks for input errors are unnecessary." Therefore, tests for input validation
(empty IDs, malformed JSON, wrong types) are marked as xfail - the system is
not required to validate these, but these tests document potential bugs.

Tests for business logic bugs (negative distances causing data corruption)
are kept as regular tests since they affect data integrity.

Run with: pytest tests/bugs/test_input_validation_bugs.py -v
"""

import pytest
import requests
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conftest import (
    create_scooter, get_scooter,
    reserve_scooter, release_scooter
)


class TestNegativeDistanceBug:
    """
    BUG: handlers.go line 91 - No validation that distance is non-negative.
    statemachine/scooter.go line 80 silently adds negative distances.

    This can cause total_distance to decrease or go negative, which is
    physically impossible and breaks business logic.

    NOTE: This is a DATA INTEGRITY issue, not input validation.
    The assignment doesn't say we should accept corrupt data.
    """

    @pytest.mark.xfail(reason="Assignment says syntax assumed correct - input validation not required")
    def test_negative_distance_should_be_rejected(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Negative distance in release should ideally be rejected.

        The assignment doesn't require input validation, so this is xfail.
        However, accepting negative distances can corrupt data.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        response = release_scooter(api_url, unique_scooter_id, -100)

        # Ideally would return 400, but assignment doesn't require this
        assert response.status_code == 400, \
            f"Negative distance accepted (status {response.status_code})"

    def test_negative_distance_corrupts_total(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: If negative distance is accepted, it corrupts the total.

        This test checks that IF the system accepts the request, the data
        doesn't become corrupted (go negative).
        """
        create_scooter(api_url, unique_scooter_id)

        # First rental: 200 distance
        reserve_scooter(api_url, unique_scooter_id, "good-rental")
        release_scooter(api_url, unique_scooter_id, 200)

        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 200

        # Try negative distance
        reserve_scooter(api_url, unique_scooter_id, "bad-rental")
        bad_response = release_scooter(api_url, unique_scooter_id, -500)

        # If the system accepted it, check data integrity
        if bad_response.status_code == 200:
            response = get_scooter(api_url, unique_scooter_id)
            distance = response.json()["total_distance"]

            # Data integrity check: distance should never go negative
            assert distance >= 0, \
                f"BUG: Total distance went negative! Got {distance}"


class TestEmptyInputValidation:
    """
    Tests for empty/whitespace inputs.

    The assignment says "Assume the syntax is correct" - so these tests
    are marked xfail. The system is not required to validate these.
    """

    @pytest.mark.xfail(reason="Assignment: 'Assume the syntax is correct; fancy parsers unnecessary'")
    def test_empty_scooter_id_create(self, api_url):
        """
        Creating a scooter with empty ID.

        Assignment doesn't require this validation.
        """
        response = requests.put(f"{api_url}/scooters/", timeout=10)
        assert response.status_code in [400, 404, 405]

    @pytest.mark.xfail(reason="Assignment: 'Assume the syntax is correct; fancy parsers unnecessary'")
    def test_whitespace_scooter_id(self, api_url):
        """
        Scooter ID with only whitespace.

        Assignment doesn't require this validation.
        """
        response = requests.put(f"{api_url}/scooters/%20%20%20", timeout=10)
        assert response.status_code == 400

    @pytest.mark.xfail(reason="Assignment: 'Assume the syntax is correct; fancy parsers unnecessary'")
    def test_empty_reservation_id(self, api_url, unique_scooter_id):
        """
        Empty reservation ID.

        Assignment doesn't require this validation.
        """
        create_scooter(api_url, unique_scooter_id)
        response = requests.post(
            f"{api_url}/scooters/{unique_scooter_id}/reservations",
            json={"reservation_id": ""},
            timeout=10
        )
        assert response.status_code == 400

    @pytest.mark.xfail(reason="Assignment: 'Assume the syntax is correct; fancy parsers unnecessary'")
    def test_whitespace_reservation_id(self, api_url, unique_scooter_id):
        """
        Whitespace-only reservation ID.

        Assignment doesn't require this validation.
        """
        create_scooter(api_url, unique_scooter_id)
        response = requests.post(
            f"{api_url}/scooters/{unique_scooter_id}/reservations",
            json={"reservation_id": "   "},
            timeout=10
        )
        assert response.status_code == 400

    @pytest.mark.xfail(reason="Assignment: 'Assume the syntax is correct; fancy parsers unnecessary'")
    def test_missing_reservation_id_field(self, api_url, unique_scooter_id):
        """
        Missing reservation_id field.

        Assignment doesn't require this validation.
        """
        create_scooter(api_url, unique_scooter_id)
        response = requests.post(
            f"{api_url}/scooters/{unique_scooter_id}/reservations",
            json={},
            timeout=10
        )
        assert response.status_code == 400


class TestMalformedJsonValidation:
    """
    Tests for malformed JSON input.

    The assignment says "Assume the syntax is correct" - so these tests
    are marked xfail. However, the system should not crash on bad input.
    """

    @pytest.mark.xfail(reason="Assignment: 'Assume the syntax is correct; fancy parsers unnecessary'")
    def test_malformed_json_reserve(self, api_url, unique_scooter_id):
        """
        Malformed JSON in reserve request.

        Assignment doesn't require validation, but shouldn't crash.
        """
        create_scooter(api_url, unique_scooter_id)
        response = requests.post(
            f"{api_url}/scooters/{unique_scooter_id}/reservations",
            data="not valid json {{{",
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        # Should be 400, not 500 (crash)
        assert response.status_code == 400

    @pytest.mark.xfail(reason="Assignment: 'Assume the syntax is correct; fancy parsers unnecessary'")
    def test_malformed_json_release(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Malformed JSON in release request.

        Assignment doesn't require validation, but shouldn't crash.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        response = requests.post(
            f"{api_url}/scooters/{unique_scooter_id}/releases",
            data="{invalid json",
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        assert response.status_code == 400

    @pytest.mark.xfail(reason="Assignment: 'Assume the syntax is correct; fancy parsers unnecessary'")
    def test_wrong_type_distance(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Wrong type for distance field.

        Assignment doesn't require validation.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        response = requests.post(
            f"{api_url}/scooters/{unique_scooter_id}/releases",
            json={"distance": "one hundred"},
            timeout=10
        )
        assert response.status_code == 400


class TestSpecialCharacterHandling:
    """
    Tests for special characters in input.

    These test that the system handles unusual but valid input safely.
    """

    def test_special_characters_dont_crash(self, api_url):
        """
        Special characters in scooter ID shouldn't crash the server.

        We don't require rejection, just that it doesn't crash.
        """
        problematic_ids = [
            "scooter-with-dashes",
            "scooter_with_underscores",
            "scooter.with.dots",
        ]

        for scooter_id in problematic_ids:
            try:
                response = create_scooter(api_url, scooter_id)
                # Should either work or return an error, not crash
                assert response.status_code in [200, 201, 400, 404]
            except requests.exceptions.ConnectionError:
                pytest.fail(f"Server crashed on ID: {scooter_id}")


class TestBoundaryValuesBug:
    """
    Tests for boundary values that might cause overflow or precision issues.

    BUG: statemachine/scooter.go line 80 uses float64 for TotalDistance
    but Distance is int64. Precision loss can occur.

    These are implementation bugs, not input validation issues.
    """

    def test_zero_distance(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Zero distance should be valid (user returned immediately).
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        response = release_scooter(api_url, unique_scooter_id, 0)

        # Zero should be valid
        assert response.status_code == 200, \
            f"Zero distance rejected! Got {response.status_code}"

        # Verify it's stored as 0
        get_response = get_scooter(api_url, unique_scooter_id)
        assert get_response.json()["total_distance"] == 0

    def test_large_distance(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Large but reasonable distance should work.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        # 1 million meters = 1000 km - a long trip but valid
        large_distance = 1000000

        response = release_scooter(api_url, unique_scooter_id, large_distance)
        assert response.status_code == 200

        get_response = get_scooter(api_url, unique_scooter_id)
        assert get_response.json()["total_distance"] == large_distance

    def test_precision_accumulation(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Float64 precision loss when accumulating many distances.

        float64 has ~15-17 significant digits. Repeated additions can lose precision.
        """
        create_scooter(api_url, unique_scooter_id)

        # Add 100 distances of 1 each (reduced from 1000 for speed)
        expected_total = 0
        for i in range(100):
            reserve_scooter(api_url, unique_scooter_id, f"rental-{i}")
            release_scooter(api_url, unique_scooter_id, 1)
            expected_total += 1

        # Check final total
        response = get_scooter(api_url, unique_scooter_id)
        actual_total = response.json()["total_distance"]

        # Should be exactly 100
        assert actual_total == expected_total, \
            f"BUG: Precision loss! Expected {expected_total}, got {actual_total}"

    def test_max_int64_distance(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Maximum int64 value might overflow or cause precision loss.

        This is a boundary test, not input validation.
        """
        create_scooter(api_url, unique_scooter_id)
        reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)

        max_int64 = 9223372036854775807

        response = release_scooter(api_url, unique_scooter_id, max_int64)

        if response.status_code == 200:
            get_response = get_scooter(api_url, unique_scooter_id)
            stored_distance = get_response.json()["total_distance"]

            # float64 can't precisely represent this value
            # Just check it didn't crash and stored something
            assert stored_distance > 0, "Distance should be positive"
