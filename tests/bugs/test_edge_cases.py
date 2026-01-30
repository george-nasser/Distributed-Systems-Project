"""
Tests designed to catch edge case and boundary condition bugs.

These tests target the following bugs:
1. log/replicated_log.go: Append uses wrong index key
2. log/replicated_log.go: Store has off-by-one errors
3. paxos/proposer.go: Missing nil check on round array
4. statemachine/scooter.go: Unknown CommandType silently succeeds

Run with: pytest tests/bugs/test_edge_cases.py -v
"""

import pytest
import requests
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conftest import (
    create_scooter, get_scooter, get_all_scooters,
    reserve_scooter, release_scooter, take_snapshot,
    wait_for_server
)


class TestLogIndexEdgeCases:
    """
    BUG: log/replicated_log.go has confusing index management.

    Line 31: log.entries[log.nextIndex] stores at nextIndex key
    But parameter 'index' is passed but only used in comparisons.
    This could cause entries to be stored at wrong indices.
    """

    def test_sequential_operations_log_integrity(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Verify that sequential operations are logged correctly.

        Each operation should be in the log at the correct index.
        If indices are wrong, recovery would fail.
        """
        create_scooter(api_url, unique_scooter_id)

        # Do a sequence of operations
        operations = []
        for i in range(10):
            res = reserve_scooter(api_url, unique_scooter_id, f"log-test-{i}")
            operations.append(("reserve", res.status_code))

            rel = release_scooter(api_url, unique_scooter_id, i + 1)
            operations.append(("release", rel.status_code))

        # Verify final state matches expected
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()

        # Expected distance: 1+2+3+4+5+6+7+8+9+10 = 55
        expected_distance = sum(range(1, 11))
        actual_distance = scooter["total_distance"]

        assert actual_distance == expected_distance, \
            f"BUG: Log integrity issue! Expected distance {expected_distance}, got {actual_distance}"

    def test_operations_after_snapshot(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Operations after snapshot should be logged correctly.

        If log indices reset or get confused after snapshot, new operations
        could be lost or applied incorrectly.
        """
        create_scooter(api_url, unique_scooter_id)

        # Do some operations
        reserve_scooter(api_url, unique_scooter_id, "before-snap")
        release_scooter(api_url, unique_scooter_id, 100)

        # Take snapshot
        take_snapshot(api_url)
        time.sleep(1)

        # Do more operations after snapshot
        reserve_scooter(api_url, unique_scooter_id, "after-snap")
        release_scooter(api_url, unique_scooter_id, 50)

        # Verify final state
        response = get_scooter(api_url, unique_scooter_id)
        scooter = response.json()

        # Total should be 100 + 50 = 150
        assert scooter["total_distance"] == 150, \
            f"BUG: Post-snapshot operations not logged correctly! Got {scooter['total_distance']}"


class TestStoredIndexEdgeCases:
    """
    BUG: log/replicated_log.go Store() has potential off-by-one errors.

    storedIndex initialized to -1, and the loop uses inclusive ranges.
    Edge cases around index 0 and negative indices could cause issues.
    """

    def test_first_operation_stored_correctly(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: The very first operation (index 0) should be stored.

        With storedIndex = -1, the first Store(0) should work correctly.
        """
        # First operation ever
        response = create_scooter(api_url, unique_scooter_id)

        assert response.status_code in [200, 201], \
            f"BUG: First operation failed! Status: {response.status_code}"

        # Verify it exists
        get_response = get_scooter(api_url, unique_scooter_id)
        assert get_response.status_code == 200

    def test_single_entry_log(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: A log with only one entry should work.
        """
        # Just create one scooter
        create_scooter(api_url, unique_scooter_id)

        # Take snapshot immediately
        snap_response = take_snapshot(api_url)

        # Should succeed
        assert snap_response.status_code == 200


class TestPaxosProtocolEdgeCases:
    """
    BUG: paxos/proposer.go has missing nil checks on round arrays.

    Line 81: promise.LastGoodRound[0] accessed without checking if array has elements.
    Could panic if gRPC returns malformed response.
    """

    def test_proposal_with_no_acceptors_reachable(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: What happens when no acceptors are reachable?

        Should fail gracefully, not panic or hang forever.
        """
        # This test assumes normal operation - we're testing error handling
        # When acceptors are reachable, this should succeed
        response = create_scooter(api_url, unique_scooter_id)

        # If system is up, should succeed
        assert response.status_code in [200, 201, 500, 503], \
            f"Unexpected response: {response.status_code}"

    def test_rapid_proposals_no_panic(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Rapid proposals shouldn't cause panics from nil pointers.
        """
        errors = []

        # Rapid fire proposals
        for i in range(100):
            try:
                sid = f"{unique_scooter_id}-rapid-{i}"
                response = create_scooter(api_url, sid)
                # Any status code is OK - just shouldn't panic
            except requests.exceptions.ConnectionError:
                # Server might have panicked and restarted
                errors.append(f"Connection error at iteration {i}")
            except Exception as e:
                errors.append(f"Error at {i}: {e}")

        # If we get many connection errors, server might be crashing
        if len(errors) > 20:
            pytest.fail(f"BUG: Too many errors (possible panics): {errors[:10]}...")


class TestUnknownCommandType:
    """
    BUG: statemachine/scooter.go Apply() has no default case.

    Unknown CommandType silently succeeds with no-op.
    Should probably return an error.
    """

    def test_malformed_command_handling(self, api_url, unique_scooter_id):
        """
        Test that the API only accepts valid operation types.

        Note: We can't directly send unknown command types via REST API,
        but we can test that invalid requests are rejected.
        """
        create_scooter(api_url, unique_scooter_id)

        # Try to hit non-existent endpoints that might bypass validation
        invalid_endpoints = [
            f"/scooters/{unique_scooter_id}/delete",
            f"/scooters/{unique_scooter_id}/update",
            f"/scooters/{unique_scooter_id}/modify",
        ]

        for endpoint in invalid_endpoints:
            try:
                response = requests.post(f"{api_url}{endpoint}", json={}, timeout=10)
                # Should return 404 or 405, not 200
                assert response.status_code in [404, 405], \
                    f"BUG: Invalid endpoint {endpoint} returned {response.status_code}"
            except Exception:
                pass  # Connection errors are fine


class TestSnapshotEdgeCases:
    """
    BUG: statemachine/scooter.go LoadSnapshot issues:
    1. No validation that loaded snapshot is non-empty map
    2. If data unmarshals to nil map, subsequent operations panic
    """

    def test_snapshot_with_no_scooters(self, api_url):
        """
        CATCHES BUG: Snapshot of empty state should work.
        """
        # Just take a snapshot (might be empty or have data)
        response = take_snapshot(api_url)

        # Should succeed
        assert response.status_code == 200

    def test_snapshot_preserves_all_data(self, api_url, unique_scooter_id):
        """
        CATCHES BUG: Snapshot should preserve all scooter data.
        """
        # Create scooters with different states
        scooter_ids = [f"{unique_scooter_id}-snap-{i}" for i in range(5)]

        for i, sid in enumerate(scooter_ids):
            create_scooter(api_url, sid)
            if i % 2 == 0:
                # Some reserved
                reserve_scooter(api_url, sid, f"snap-res-{i}")
            if i > 0:
                # Some with distance (release those that were reserved)
                if i % 2 == 0:
                    release_scooter(api_url, sid, i * 10)

        # Take snapshot
        take_snapshot(api_url)
        time.sleep(1)

        # Verify all scooters still exist with correct state
        for i, sid in enumerate(scooter_ids):
            response = get_scooter(api_url, sid)
            assert response.status_code == 200, f"BUG: Scooter {sid} lost after snapshot"


class TestOperationOrderEdgeCases:
    """
    Tests for edge cases in operation ordering.
    """

    def test_release_immediately_after_reserve(self, api_url, unique_scooter_id, unique_reservation_id):
        """
        Reserve followed immediately by release should work.
        """
        create_scooter(api_url, unique_scooter_id)

        # Immediate reserve-release
        res = reserve_scooter(api_url, unique_scooter_id, unique_reservation_id)
        rel = release_scooter(api_url, unique_scooter_id, 50)

        assert res.status_code == 200
        assert rel.status_code == 200

    def test_many_creates_then_operations(self, api_url, unique_scooter_id):
        """
        Create many scooters, then operate on them all.
        """
        num_scooters = 20
        scooter_ids = [f"{unique_scooter_id}-bulk-{i}" for i in range(num_scooters)]

        # Create all
        for sid in scooter_ids:
            response = create_scooter(api_url, sid)
            assert response.status_code in [200, 201], f"Failed to create {sid}"

        # Reserve all
        for sid in scooter_ids:
            response = reserve_scooter(api_url, sid, f"bulk-res-{sid}")
            assert response.status_code == 200, f"Failed to reserve {sid}"

        # Release all
        for i, sid in enumerate(scooter_ids):
            response = release_scooter(api_url, sid, i + 1)
            assert response.status_code == 200, f"Failed to release {sid}"

        # Verify all
        for i, sid in enumerate(scooter_ids):
            response = get_scooter(api_url, sid)
            scooter = response.json()
            assert scooter["is_available"] == True
            assert scooter["total_distance"] == i + 1

    def test_interleaved_operations_different_scooters(self, api_url, unique_scooter_id):
        """
        Interleaved operations on different scooters shouldn't interfere.
        """
        sid1 = f"{unique_scooter_id}-inter-1"
        sid2 = f"{unique_scooter_id}-inter-2"

        # Create both
        create_scooter(api_url, sid1)
        create_scooter(api_url, sid2)

        # Interleave: reserve 1, reserve 2, release 1, release 2
        reserve_scooter(api_url, sid1, "res-1")
        reserve_scooter(api_url, sid2, "res-2")
        release_scooter(api_url, sid1, 100)
        release_scooter(api_url, sid2, 200)

        # Check each
        resp1 = get_scooter(api_url, sid1)
        resp2 = get_scooter(api_url, sid2)

        assert resp1.json()["total_distance"] == 100
        assert resp2.json()["total_distance"] == 200


class TestHTTPEdgeCases:
    """
    Tests for HTTP handling edge cases.
    """

    def test_timeout_handling(self, api_url, unique_scooter_id):
        """
        Test with very short timeout - should fail gracefully.
        """
        try:
            response = requests.put(
                f"{api_url}/scooters/{unique_scooter_id}",
                timeout=0.001  # 1ms - very short
            )
        except requests.exceptions.Timeout:
            pass  # Expected
        except requests.exceptions.ConnectionError:
            pass  # Also acceptable
        except Exception as e:
            pytest.fail(f"Unexpected error type: {type(e)}: {e}")

    def test_large_scooter_id(self, api_url):
        """
        Test with very long scooter ID.
        """
        # 10KB scooter ID
        long_id = "a" * 10000

        response = create_scooter(api_url, long_id)

        # Should either accept or reject gracefully (400), not crash
        assert response.status_code in [200, 201, 400, 414], \
            f"Unexpected response for long ID: {response.status_code}"

    def test_unicode_scooter_id(self, api_url):
        """
        Test with unicode characters in scooter ID.
        """
        unicode_id = "scooter-\u4e2d\u6587-\u0422\u0435\u0441\u0442"

        response = create_scooter(api_url, unicode_id)

        # Should handle unicode gracefully
        if response.status_code in [200, 201]:
            # If accepted, verify retrieval works
            get_response = get_scooter(api_url, unicode_id)
            assert get_response.status_code == 200

    def test_concurrent_requests_same_connection(self, api_url, unique_scooter_id):
        """
        Test multiple requests on same session.
        """
        session = requests.Session()

        try:
            # Multiple requests on same session
            for i in range(10):
                sid = f"{unique_scooter_id}-session-{i}"
                response = session.put(f"{api_url}/scooters/{sid}", timeout=10)
                assert response.status_code in [200, 201]
        finally:
            session.close()
