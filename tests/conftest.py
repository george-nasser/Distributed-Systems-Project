"""
Shared fixtures and helper functions for CityScooter tests.

This file contains simple fixtures and helper functions used across all tests.
Nothing fancy here - just straightforward helper code.
"""

import pytest
import requests
import time
import subprocess
import os


# ============================================================================
# FIXTURES - Simple configuration fixtures
# ============================================================================

@pytest.fixture
def api_url():
    """Base URL for the API (server 1 directly, since Traefik is disabled)."""
    return os.environ.get("API_URL", "http://localhost:8081")


@pytest.fixture
def server_urls():
    """Direct URLs to each of the 5 scooter-server replicas."""
    base_port = int(os.environ.get("SERVER_BASE_PORT", "8081"))
    return [f"http://localhost:{base_port + i}" for i in range(5)]


@pytest.fixture
def etcd_url():
    """URL for the etcd server."""
    return os.environ.get("ETCD_URL", "http://localhost:2379")


@pytest.fixture
def unique_scooter_id():
    """Generate a unique scooter ID for each test to avoid conflicts."""
    import uuid
    return f"scooter-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def unique_reservation_id():
    """Generate a unique reservation ID for each test."""
    import uuid
    return f"res-{uuid.uuid4().hex[:8]}"


# ============================================================================
# HELPER FUNCTIONS - Simple wrappers around API calls
# ============================================================================

def create_scooter(url, scooter_id):
    """
    Create a new scooter.

    Args:
        url: Base API URL
        scooter_id: ID for the new scooter

    Returns:
        requests.Response object
    """
    return requests.put(f"{url}/scooters/{scooter_id}", timeout=60)


def get_scooter(url, scooter_id):
    """
    Get a scooter by ID.

    Args:
        url: Base API URL
        scooter_id: ID of scooter to fetch

    Returns:
        requests.Response object
    """
    return requests.get(f"{url}/scooters/{scooter_id}", timeout=60)


def get_all_scooters(url):
    """
    Get all scooters.

    Args:
        url: Base API URL

    Returns:
        requests.Response object
    """
    return requests.get(f"{url}/scooters", timeout=60)


def reserve_scooter(url, scooter_id, reservation_id):
    """
    Reserve a scooter.

    Args:
        url: Base API URL
        scooter_id: ID of scooter to reserve
        reservation_id: Reservation identifier

    Returns:
        requests.Response object
    """
    return requests.post(
        f"{url}/scooters/{scooter_id}/reservations",
        json={"reservation_id": reservation_id},
        timeout=60
    )


def release_scooter(url, scooter_id, distance):
    """
    Release a scooter and record distance traveled.

    Args:
        url: Base API URL
        scooter_id: ID of scooter to release
        distance: Distance traveled during rental

    Returns:
        requests.Response object
    """
    return requests.post(
        f"{url}/scooters/{scooter_id}/releases",
        json={"distance": distance},
        timeout=60
    )


def take_snapshot(url):
    """
    Trigger a state snapshot.

    Args:
        url: Base API URL

    Returns:
        requests.Response object
    """
    return requests.post(f"{url}/snapshot", timeout=60)


def get_servers(url):
    """
    Get list of registered servers.

    Args:
        url: Base API URL

    Returns:
        requests.Response object
    """
    return requests.get(f"{url}/servers", timeout=60)


# ============================================================================
# WAIT HELPERS - For waiting on async operations
# ============================================================================

def wait_for_server(url, timeout=30):
    """
    Wait for a server to become available.

    Args:
        url: Server URL to check
        timeout: Max seconds to wait

    Returns:
        True if server is up, False if timeout
    """
    start = time.time()
    while time.time() - start < timeout:
        try:
            response = requests.get(f"{url}/scooters", timeout=2)
            if response.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(0.5)
    return False


def wait_for_replication(server_urls, scooter_id, timeout=10):
    """
    Wait for a scooter to be visible on all servers.

    Args:
        server_urls: List of server URLs
        scooter_id: Scooter ID to check
        timeout: Max seconds to wait

    Returns:
        True if replicated to all, False if timeout
    """
    start = time.time()
    while time.time() - start < timeout:
        all_have_it = True
        for url in server_urls:
            try:
                response = get_scooter(url, scooter_id)
                if response.status_code != 200:
                    all_have_it = False
                    break
            except requests.exceptions.RequestException:
                all_have_it = False
                break
        if all_have_it:
            return True
        time.sleep(0.5)
    return False


def wait_for_leader(server_urls, timeout=30):
    """
    Wait for a leader to be elected.

    Args:
        server_urls: List of server URLs
        timeout: Max seconds to wait

    Returns:
        URL of the leader server, or None if timeout
    """
    start = time.time()
    while time.time() - start < timeout:
        for url in server_urls:
            try:
                response = get_servers(url)
                if response.status_code == 200:
                    servers = response.json()
                    for server in servers:
                        if server.get("is_leader"):
                            return url
            except requests.exceptions.RequestException:
                pass
        time.sleep(0.5)
    return None


# ============================================================================
# DOCKER HELPERS - For e2e tests that need docker-compose
# ============================================================================

class DockerComposeManager:
    """Simple manager for docker-compose operations."""

    def __init__(self, compose_dir):
        self.compose_dir = compose_dir

    def up(self):
        """Start all services."""
        subprocess.run(
            ["docker-compose", "up", "-d"],
            cwd=self.compose_dir,
            check=True,
            capture_output=True
        )

    def down(self):
        """Stop all services."""
        subprocess.run(
            ["docker-compose", "down"],
            cwd=self.compose_dir,
            check=True,
            capture_output=True
        )

    def stop_service(self, service_name):
        """Stop a specific service."""
        subprocess.run(
            ["docker-compose", "stop", service_name],
            cwd=self.compose_dir,
            check=True,
            capture_output=True
        )

    def start_service(self, service_name):
        """Start a specific service."""
        subprocess.run(
            ["docker-compose", "start", service_name],
            cwd=self.compose_dir,
            check=True,
            capture_output=True
        )

    def restart_service(self, service_name):
        """Restart a specific service."""
        subprocess.run(
            ["docker-compose", "restart", service_name],
            cwd=self.compose_dir,
            check=True,
            capture_output=True
        )


@pytest.fixture
def docker_compose():
    """
    Fixture that manages docker-compose for e2e tests.

    Usage:
        def test_something(docker_compose):
            docker_compose.up()
            # ... run tests ...
            docker_compose.down()
    """
    compose_dir = os.environ.get(
        "COMPOSE_DIR",
        os.path.join(os.path.dirname(__file__), "..", "src", "docker")
    )
    return DockerComposeManager(compose_dir)


# ============================================================================
# CLEANUP FIXTURE - Clean up test scooters after tests
# ============================================================================

@pytest.fixture
def cleanup_scooters(api_url):
    """
    Fixture that tracks created scooters and could clean them up.

    Note: Since we use unique IDs per test and the system doesn't have
    a delete endpoint, we just track what was created for debugging.
    """
    created = []

    def track(scooter_id):
        created.append(scooter_id)
        return scooter_id

    yield track

    # In a real system with delete, we'd clean up here
    # For now, just log what was created
    if created:
        print(f"\nTest created scooters: {created}")
