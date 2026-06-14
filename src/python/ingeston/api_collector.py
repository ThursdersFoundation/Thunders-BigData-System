"""
API Collector Module for Thunders BigData System.

Provides a configurable REST API data collector with authentication,
pagination, rate limiting, and retry support.
"""

import logging
import time
from typing import Any, Callable, Dict, Generator, List, Optional, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class APICollector:
    """Collects data from REST APIs with robust error handling and pagination.

    Supports various authentication schemes (Bearer, API key, Basic),
    automatic pagination, rate limiting, and configurable retries.

    Attributes:
        base_url: Root URL for the API endpoint.
        session: Requests session with retry configuration.
        headers: Default HTTP headers included with every request.
    """

    def __init__(
        self,
        base_url: str,
        headers: Optional[Dict[str, str]] = None,
        auth_token: Optional[str] = None,
        api_key: Optional[str] = None,
        api_key_header: str = "X-API-Key",
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        retry_backoff: float = 0.5,
        rate_limit: Optional[float] = None,
        verify_ssl: bool = True,
    ) -> None:
        """Initialize the APICollector.

        Args:
            base_url: Root URL for API requests (e.g., 'https://api.example.com/v1').
            headers: Default headers to include with every request.
            auth_token: Bearer token for authentication.
            api_key: API key for authentication.
            api_key_header: Header name for the API key.
            username: Username for Basic authentication.
            password: Password for Basic authentication.
            timeout: Request timeout in seconds.
            max_retries: Maximum number of retry attempts.
            retry_backoff: Backoff factor for retries.
            rate_limit: Minimum seconds between requests (None for no limit).
            verify_ssl: Whether to verify SSL certificates.
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.rate_limit = rate_limit
        self.verify_ssl = verify_ssl
        self._last_request_time: float = 0.0

        # Build default headers
        self.headers: Dict[str, str] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if headers:
            self.headers.update(headers)

        # Configure authentication
        self._auth = None
        if auth_token:
            self.headers["Authorization"] = f"Bearer {auth_token}"
        if api_key:
            self.headers[api_key_header] = api_key
        if username and password:
            self._auth = (username, password)

        # Configure session with retries
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=retry_backoff,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT", "DELETE"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _enforce_rate_limit(self) -> None:
        """Enforce the configured rate limit between requests."""
        if self.rate_limit is not None:
            elapsed = time.time() - self._last_request_time
            if elapsed < self.rate_limit:
                time.sleep(self.rate_limit - elapsed)
        self._last_request_time = time.time()

    def _build_url(self, endpoint: str) -> str:
        """Build the full URL for a given endpoint.

        Args:
            endpoint: API endpoint path (e.g., '/users').

        Returns:
            Full URL string.
        """
        endpoint = endpoint.lstrip("/")
        return f"{self.base_url}/{endpoint}"

    def collect(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET",
        body: Optional[Dict[str, Any]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Collect data from a single API endpoint.

        Args:
            endpoint: API endpoint path.
            params: Query parameters for the request.
            method: HTTP method (GET, POST, PUT, DELETE).
            body: Request body for POST/PUT requests.
            extra_headers: Additional headers for this request only.

        Returns:
            Dictionary containing:
                - status_code: HTTP status code
                - data: Parsed JSON response (or raw text)
                - headers: Response headers
                - elapsed: Request duration in seconds

        Raises:
            requests.RequestException: If the request fails after retries.
        """
        self._enforce_rate_limit()

        url = self._build_url(endpoint)
        request_headers = {**self.headers, **(extra_headers or {})}

        logger.debug("%s %s params=%s", method, url, params)

        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=body,
                headers=request_headers,
                auth=self._auth,
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
            response.raise_for_status()

            try:
                data = response.json()
            except ValueError:
                data = response.text

            result = {
                "status_code": response.status_code,
                "data": data,
                "headers": dict(response.headers),
                "elapsed": response.elapsed.total_seconds(),
            }
            logger.info(
                "API request successful: %s %s -> %d (%.2fs)",
                method,
                url,
                response.status_code,
                result["elapsed"],
            )
            return result

        except requests.RequestException as exc:
            logger.error("API request failed: %s %s -> %s", method, url, exc)
            raise

    def collect_paginated(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_param: str = "page",
        page_size_param: str = "per_page",
        page_size: int = 100,
        max_pages: Optional[int] = None,
        next_page_extractor: Optional[Callable[[Dict], Optional[str]]] = None,
        data_extractor: Optional[Callable[[Dict], List]] = None,
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """Collect paginated data from an API endpoint.

        Automatically handles pagination by incrementing page numbers or
        following 'next' links in the response.

        Args:
            endpoint: API endpoint path.
            params: Base query parameters.
            page_param: Query parameter name for the page number.
            page_size_param: Query parameter name for the page size.
            page_size: Number of items per page.
            max_pages: Maximum number of pages to fetch (None for unlimited).
            next_page_extractor: Function to extract the next page URL/token from the response.
            data_extractor: Function to extract the data list from the response.

        Yields:
            List of items from each page.
        """
        params = dict(params or {})
        params[page_size_param] = page_size

        current_page = 1
        next_url: Optional[str] = None
        pages_fetched = 0

        while True:
            if max_pages is not None and pages_fetched >= max_pages:
                logger.info("Reached max page limit (%d)", max_pages)
                break

            if next_url is None:
                params[page_param] = current_page
                result = self.collect(endpoint, params=params)
            else:
                self._enforce_rate_limit()
                response = self.session.get(
                    next_url,
                    headers=self.headers,
                    auth=self._auth,
                    timeout=self.timeout,
                    verify=self.verify_ssl,
                )
                response.raise_for_status()
                try:
                    result = {"data": response.json()}
                except ValueError:
                    break

            response_data = result.get("data", {})
            items = data_extractor(response_data) if data_extractor else response_data if isinstance(response_data, list) else response_data.get("data", [])

            if not items:
                logger.info("No more items on page %d", current_page)
                break

            yield items if isinstance(items, list) else [items]

            pages_fetched += 1
            current_page += 1

            if next_page_extractor:
                next_url = next_page_extractor(response_data)
                if not next_url:
                    break

    def health_check(self) -> bool:
        """Verify connectivity to the API.

        Returns:
            True if the API is reachable, False otherwise.
        """
        try:
            result = self.collect("", method="GET")
            return result["status_code"] < 500
        except requests.RequestException:
            return False

    def close(self) -> None:
        """Close the underlying requests session."""
        self.session.close()
        logger.info("API collector session closed")

    def __enter__(self) -> "APICollector":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit: close the session."""
        self.close()

    def __repr__(self) -> str:
        return f"APICollector(base_url='{self.base_url}')"
