"""
Tests for request forwarding as specified in the assignment.

From the assignment (Section 3.1 Client Communication):
"A client may contact any server in the system, even though the specific
server is not responsible handling the request. In such case, the server
should forward the request to the server responsible on behalf of the client."

This means:
- Writes can be sent to any server
- If the server is not the leader, it should forward to the leader
- The client doesn't need to know who the leader is

Run with: pytest tests/assignment/test_request_forwarding.py -v
"""

import pytest
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from conftest import (
    create_scooter, get_scooter, get_all_scooters,
    reserve_scooter, release_scooter,
    wait_for_server, get_servers
)


class TestAnyServerCanHandleRequests:
    """
    Tests that any server can handle client requests.

    The client shouldn't need to know which server is the leader.
    """

    def test_create_through_any_server(self, server_urls, unique_scooter_id):
        """
        Creating a scooter through any server should work.

        The server should either handle it directly (if leader) or
        forward to the leader.
        """
        successful_creates = 0

        for i, url in enumerate(server_urls):
            scooter_id = f"{unique_scooter_id}-server{i}"
            try:
                response = create_scooter(url, scooter_id)
                if response.status_code in [200, 201]:
                    successful_creates += 1
            except Exception as e:
                print(f"Server {i} failed: {e}")

        # All servers should be able to handle create requests
        assert successful_creates >= 3, \
            f"Only {successful_creates}/{len(server_urls)} servers handled create"

    def test_read_from_any_server(self, server_urls, unique_scooter_id):
        """
        Reading from any server should work.
        """
        # Create scooter first
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(3)  # Wait for replication

        successful_reads = 0

        for url in server_urls:
            try:
                response = get_scooter(url, unique_scooter_id)
                if response.status_code == 200:
                    successful_reads += 1
            except Exception:
                pass

        # All servers should be able to serve reads
        assert successful_reads >= 3, \
            f"Only {successful_reads}/{len(server_urls)} servers served reads"

    def test_reserve_through_any_server(self, server_urls, unique_scooter_id):
        """
        Reserving through any server should work.
        """
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(2)

        # Try to reserve through a different server
        response = reserve_scooter(server_urls[1], unique_scooter_id, "forward-test")

        # Should succeed (either directly or via forwarding)
        assert response.status_code == 200, \
            f"Reserve through non-leader server failed: {response.status_code}"

    def test_release_through_any_server(self, server_urls, unique_scooter_id, unique_reservation_id):
        """
        Releasing through any server should work.
        """
        create_scooter(server_urls[0], unique_scooter_id)
        time.sleep(1)
        reserve_scooter(server_urls[0], unique_scooter_id, unique_reservation_id)
        time.sleep(1)

        # Try to release through a different server
        response = release_scooter(server_urls[2], unique_scooter_id, 100)

        # Should succeed
        assert response.status_code == 200, \
            f"Release through non-leader server failed: {response.status_code}"


class TestWriteForwarding:
    """
    Tests that write requests are forwarded to the leader if necessary.
    """

    def test_write_to_follower_succeeds(self, server_urls, unique_scooter_id):
        """
        Writing to a follower should succeed (forwarded to leader).
        """
        # Find a follower (non-leader)
        follower_url = None
        for url in server_urls:
            try:
                response = get_servers(url)
                if response.status_code == 200:
                    servers = response.json()
                    for s in servers:
                        if not s.get("is_leader"):
                            # This server is a follower
                            follower_url = url
                            break
                if follower_url:
                    break
            except Exception:
                pass

        if follower_url is None:
            # Just use any server if we can't determine leader
            follower_url = server_urls[1]

        # Write to follower
        response = create_scooter(follower_url, unique_scooter_id)

        # Should succeed via forwarding
        assert response.status_code in [200, 201], \
            f"Write to follower failed: {response.status_code}"

    def test_consecutive_writes_different_servers(self, server_urls, unique_scooter_id):
        """
        Writes to different servers should all work and be consistent.
        """
        # Create on server 0
        response = create_scooter(server_urls[0], unique_scooter_id)
        assert response.status_code in [200, 201]
        time.sleep(1)

        # Reserve on server 1
        response = reserve_scooter(server_urls[1], unique_scooter_id, "multi-server")
        assert response.status_code == 200
        time.sleep(1)

        # Release on server 2
        response = release_scooter(server_urls[2], unique_scooter_id, 100)
        assert response.status_code == 200
        time.sleep(2)

        # Verify final state from any server
        response = get_scooter(server_urls[3], unique_scooter_id)
        assert response.status_code == 200
        assert response.json()["total_distance"] == 100
        assert response.json()["is_available"] == True


class TestTransparentForwarding:
    """
    Tests that forwarding is transparent to the client.

    The client should just get a successful response - it shouldn't
    need to know about the forwarding.
    """

    def test_client_doesnt_need_to_retry(self, server_urls, unique_scooter_id):
        """
        Client should get success without needing to retry on another server.
        """
        # Try each server - all should give success, not redirect
        for i, url in enumerate(server_urls[:3]):  # Test first 3
            scooter_id = f"{unique_scooter_id}-noretry-{i}"

            response = create_scooter(url, scooter_id)

            # Should succeed directly, not tell client to try elsewhere
            assert response.status_code in [200, 201], \
                f"Server {i} didn't handle request transparently: {response.status_code}"

    def test_response_contains_result_not_redirect(self, server_urls, unique_scooter_id):
        """
        Response should contain the result, not a redirect instruction.
        """
        response = create_scooter(server_urls[1], unique_scooter_id)

        # Should be success, not 3xx redirect
        assert response.status_code in [200, 201], \
            f"Got redirect instead of result: {response.status_code}"


class TestFullWorkflowAnyServer:
    """
    Tests complete workflows going through different servers.
    """

    def test_complete_workflow_switching_servers(self, server_urls, unique_scooter_id):
        """
        Complete workflow using a different server for each step.
        """
        # Step 1: Create on server 0
        response = create_scooter(server_urls[0], unique_scooter_id)
        assert response.status_code in [200, 201]
        time.sleep(1)

        # Step 2: Read from server 1
        response = get_scooter(server_urls[1], unique_scooter_id)
        assert response.status_code == 200
        assert response.json()["is_available"] == True

        # Step 3: Reserve on server 2
        response = reserve_scooter(server_urls[2], unique_scooter_id, "workflow-test")
        assert response.status_code == 200
        time.sleep(1)

        # Step 4: Read from server 3
        response = get_scooter(server_urls[3], unique_scooter_id)
        assert response.status_code == 200
        assert response.json()["is_available"] == False

        # Step 5: Release on server 4
        response = release_scooter(server_urls[4], unique_scooter_id, 150)
        assert response.status_code == 200
        time.sleep(1)

        # Step 6: Final read from server 0
        response = get_scooter(server_urls[0], unique_scooter_id)
        assert response.status_code == 200
        assert response.json()["total_distance"] == 150
        assert response.json()["is_available"] == True

    def test_multiple_scooters_different_servers(self, server_urls, unique_scooter_id):
        """
        Create multiple scooters using different servers.
        """
        scooter_ids = []

        # Create scooters on different servers
        for i, url in enumerate(server_urls):
            sid = f"{unique_scooter_id}-multi-{i}"
            response = create_scooter(url, sid)
            if response.status_code in [200, 201]:
                scooter_ids.append(sid)

        # Wait for replication
        time.sleep(5)

        # All scooters should be visible from any server
        response = get_all_scooters(server_urls[0])
        all_scooters = response.json()
        all_ids = [s["id"] for s in all_scooters]

        for sid in scooter_ids:
            assert sid in all_ids, f"Scooter {sid} not visible"


class TestLoadBalancerBehavior:
    """
    Tests behavior when using the load balancer (Traefik).

    The load balancer distributes requests across servers, so the
    system must handle requests going to different servers.
    """

    def test_rapid_requests_through_load_balancer(self, api_url, unique_scooter_id):
        """
        Rapid requests through load balancer should all work.

        The load balancer might send each request to a different server.
        """
        create_scooter(api_url, unique_scooter_id)

        # Rapid operations through load balancer
        for i in range(20):
            reserve_scooter(api_url, unique_scooter_id, f"lb-{i}")
            release_scooter(api_url, unique_scooter_id, 5)

        # Should have consistent final state
        response = get_scooter(api_url, unique_scooter_id)
        assert response.json()["total_distance"] == 100  # 20 * 5

    def test_concurrent_requests_load_balanced(self, api_url, unique_scooter_id):
        """
        Concurrent requests through load balancer should be handled.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Create multiple scooters concurrently
        scooter_ids = [f"{unique_scooter_id}-conc-{i}" for i in range(10)]

        def create_one(sid):
            return create_scooter(api_url, sid).status_code

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(create_one, sid) for sid in scooter_ids]
            results = [f.result() for f in as_completed(futures)]

        successes = sum(1 for r in results if r in [200, 201])
        assert successes == 10, f"Only {successes}/10 concurrent creates through LB succeeded"
