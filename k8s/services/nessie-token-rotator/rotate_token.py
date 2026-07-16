#!/usr/bin/env python3
"""
Nessie Catalog Token Rotator for Dremio

Periodically fetches a new OAuth2 bearer token from AWS Cognito and updates
the Dremio Nessie source configuration via the REST API so that the Nessie
catalog never falls into an unauthenticated state.

Uses client_credentials grant type for service-to-service authentication.
"""

import base64
import json
import logging
import os
import sys
import time
import traceback

import requests

# ---------------------------------------------------------------------------
# Configuration (sourced from environment variables / K8s ConfigMap + Secret)
# ---------------------------------------------------------------------------
DREMIO_URL = os.environ.get("DREMIO_URL", "http://dremio-coordinator.dremio.svc.cluster.local:9047")
DREMIO_USERNAME = os.environ.get("DREMIO_USERNAME", "")
DREMIO_PASSWORD = os.environ.get("DREMIO_PASSWORD", "")

NESSIE_SOURCE_NAME = os.environ.get("NESSIE_SOURCE_NAME", "nessiecatalog")
# Nessie API endpoint for token validation (e.g., https://nessie.tierravivaai.net/api/v2)
NESSIE_ENDPOINT = os.environ.get("NESSIE_ENDPOINT", "")

OAUTH2_TOKEN_ENDPOINT = os.environ.get("OAUTH2_TOKEN_ENDPOINT", "")
OAUTH2_CLIENT_ID = os.environ.get("OAUTH2_CLIENT_ID", "")
OAUTH2_CLIENT_SECRET = os.environ.get("OAUTH2_CLIENT_SECRET", "")
OAUTH2_SCOPE = os.environ.get("OAUTH2_SCOPE", "")

TOKEN_REFRESH_MARGIN = int(os.environ.get("TOKEN_REFRESH_MARGIN", "300"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.environ.get("RETRY_DELAY", "5"))

# Path to cache the last-known source config so we can update the token even
# when the source is unhealthy (deadlock recovery).
SOURCE_CACHE_PATH = os.environ.get("SOURCE_CACHE_PATH", "/tmp/nessie_source_cache.json")
# When true, skip rotation if the current token is still valid and has more
# than TOKEN_REFRESH_MARGIN seconds remaining. Set to "false" to force rotation.
SKIP_IF_VALID = os.environ.get("SKIP_IF_VALID", "true").lower() == "true"

# Storage configuration for constructing a minimal source payload when no
# cache is available (first-deployment bootstrap scenario).
NESSIE_SOURCE_ENDPOINT = os.environ.get("NESSIE_SOURCE_ENDPOINT", NESSIE_ENDPOINT.rstrip("/api/v2").rstrip("/"))
STORAGE_PROVIDER_TYPE = os.environ.get("STORAGE_PROVIDER_TYPE", "AWS")
AWS_ROOT_PATH = os.environ.get("AWS_ROOT_PATH", "/")
CREDENTIAL_TYPE = os.environ.get("CREDENTIAL_TYPE", "ACCESS_KEY")
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY", "")
AWS_ACCESS_SECRET = os.environ.get("AWS_ACCESS_SECRET", "")
AWS_SECURE_CONNECTION = os.environ.get("AWS_SECURE_CONNECTION", "true")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("nessie-token-rotator")


# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------
def retry(max_attempts=3, delay=5, exceptions=(Exception,)):
    """Retry decorator for transient failures."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        raise
                    logger.warning(
                        "Attempt %d/%d failed: %s, retrying in %ds",
                        attempt, max_attempts, e, delay,
                    )
                    time.sleep(delay)
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Dremio REST API helpers
# ---------------------------------------------------------------------------
class DremioClient:
    """Minimal client for Dremio REST API."""

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.session = requests.Session()

    def login(self) -> None:
        """Authenticate to Dremio and store the auth token for API calls."""
        url = f"{self.base_url}/apiv2/login"
        resp = self.session.post(
            url,
            json={"userName": self.username, "password": self.password},
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"Dremio login failed: status={resp.status_code} "
                f"body={resp.text[:200]}"
            )
        login_data = resp.json()
        token = login_data.get("token")
        if not token:
            raise RuntimeError(
                f"Dremio login succeeded but no token in response: "
                f"{list(login_data.keys())}"
            )
        self.session.headers.update({"Authorization": f"_dremio {token}"})
        logger.info("Logged in to Dremio as %s", self.username)

    def get_source(self, source_name: str) -> dict | None:
        """Fetch a source configuration by name.

        First tries the by-path endpoint; falls back to catalog listing
        + GET by ID if the source is unavailable (returns 400).

        Returns the source dict on success, or None if the source cannot
        be fetched (e.g. it is currently unhealthy). Returning None
        signals the caller to skip the update and keep the existing token.
        """
        encoded = source_name
        url = f"{self.base_url}/api/v3/catalog/by-path/{encoded}"
        resp = self.session.get(url)
        if resp.status_code == 200:
            source = resp.json()
            logger.info(
                "Got source '%s' via by-path (id=%s, tag=%s)",
                source["name"], source["id"], source.get("tag"),
            )
            return source
        if resp.status_code != 400:
            # Unexpected error — raise
            resp.raise_for_status()

        # Fallback: list catalog to find source ID, then GET by ID
        logger.warning(
            "by-path returned 400 (source unavailable) — "
            "falling back to catalog listing"
        )
        try:
            resp = self.session.get(f"{self.base_url}/api/v3/catalog")
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("Catalog listing failed: %s", e)
            return None

        catalog = resp.json()
        matches = [
            e for e in catalog.get("data", [])
            if e.get("containerType") == "SOURCE"
            and e.get("path", [""])[0] == source_name
        ]
        if not matches:
            logger.error("Source '%s' not found in catalog listing", source_name)
            return None
        src_id = matches[0]["id"]
        logger.info("Found source id=%s via catalog listing", src_id)

        try:
            resp = self.session.get(f"{self.base_url}/api/v3/catalog/{src_id}")
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(
                "GET /catalog/%s failed: %s — source may be unhealthy, "
                "skipping update to preserve existing token",
                src_id, e,
            )
            return None

        source = resp.json()
        logger.info(
            "Got source '%s' via id (id=%s, tag=%s)",
            source["name"], source["id"], source.get("tag"),
        )
        return source

    @staticmethod
    def _build_source_payload(source: dict, config: dict) -> dict:
        """Build a complete PUT payload from the GET source response.

        Dremio's Source.toSourceConfig() requires several fields to be present
        (metadataPolicy, accelerationGracePeriodMs, etc.) or it throws NPE.
        We must forward ALL top-level fields from the GET response, not just
        a minimal subset, to avoid 500 errors on PUT.
        """
        # Copy all top-level fields from the GET response to preserve
        # required fields like metadataPolicy, accelerationGracePeriodMs, etc.
        payload = dict(source)
        # Override config with our modified version
        payload["config"] = config
        # Remove read-only / response-only fields that Dremio ignores on PUT
        payload.pop("children", None)
        payload.pop("nextPageToken", None)
        payload.pop("state", None)
        payload.pop("sourceChangeState", None)
        return payload

    def update_source_token(
        self, source: dict, new_token: str
    ) -> dict:
        """Update the nessieAccessToken in the source config.

        Sends the FULL source object back (not a minimal payload) because
        Dremio's Source.toSourceConfig() calls getMetadataPolicy().toMetadataPolicy()
        which throws NullPointerException if metadataPolicy is missing.

        Handles HTTP 409 (ConcurrentModificationException) when the tag in the
        payload is stale. On 409: re-fetches the source config to get a fresh
        tag and retries the PUT once. If re-fetch also fails, retries with
        tag=None (forces the create path, which replaces the source config).

        If the PUT ultimately fails, attempts to roll back to the previous
        config to avoid leaving the source in a broken state.
        """
        config = dict(source.get("config", {}))  # shallow copy
        old_config = dict(config)  # snapshot for rollback
        old_tag = source.get("tag")
        config["nessieAccessToken"] = new_token

        # Remove read-only fields that Dremio doesn't accept in PUT
        config.pop("propertyList", None)

        payload = self._build_source_payload(source, config)

        source_id = source["id"]
        url = f"{self.base_url}/api/v3/catalog/{source_id}"
        resp = self.session.put(url, json=payload)

        # Handle 409: stale tag from cached config
        if resp.status_code == 409:
            logger.warning(
                "PUT returned 409 (stale tag) for source '%s' — "
                "attempting re-fetch and retry",
                source.get("name", source_id),
            )
            fresh_source = self.get_source(source.get("name", NESSIE_SOURCE_NAME))
            if fresh_source is not None:
                fresh_config = dict(fresh_source.get("config", {}))
                fresh_config["nessieAccessToken"] = new_token
                fresh_config.pop("propertyList", None)
                retry_payload = self._build_source_payload(fresh_source, fresh_config)
                retry_url = f"{self.base_url}/api/v3/catalog/{fresh_source['id']}"
                resp = self.session.put(retry_url, json=retry_payload)
                if resp.status_code == 200:
                    result = resp.json()
                    logger.info(
                        "Updated source '%s' with fresh token (recovered from 409 via re-fetch)",
                        result["name"],
                    )
                    return result
                logger.error(
                    "Retry PUT after re-fetch failed: status=%d body=%s",
                    resp.status_code, resp.text[:500],
                )
            else:
                logger.warning(
                    "Re-fetch failed for source '%s' — retrying PUT with tag=None",
                    source.get("name", source_id),
                )
                retry_payload = dict(payload)
                retry_payload["tag"] = None
                resp = self.session.put(url, json=retry_payload)
                if resp.status_code == 200:
                    result = resp.json()
                    logger.info(
                        "Updated source '%s' with fresh token (recovered from 409 via tag=None)",
                        result["name"],
                    )
                    return result
                logger.error(
                    "Retry PUT with tag=None failed: status=%d body=%s",
                    resp.status_code, resp.text[:500],
                )

        if resp.status_code != 200:
            logger.error(
                "Update source failed: status=%d body=%s",
                resp.status_code, resp.text[:500],
            )
            # Attempt rollback to preserve the previous working config
            logger.warning("Attempting rollback to previous config for source '%s'...", source.get("name", source_id))
            old_config.pop("propertyList", None)
            rollback_payload = self._build_source_payload(source, old_config)
            rollback_payload["tag"] = old_tag
            try:
                rb_resp = self.session.put(url, json=rollback_payload)
                if rb_resp.status_code == 200:
                    logger.info("Rollback succeeded — source '%s' restored to previous config", source.get("name", source_id))
                else:
                    logger.error(
                        "Rollback FAILED for source '%s': status=%d body=%s",
                        source.get("name", source_id), rb_resp.status_code, rb_resp.text[:200],
                    )
            except Exception as rb_exc:
                logger.error("Rollback raised exception for source '%s': %s", source.get("name", source_id), rb_exc)
            resp.raise_for_status()
        result = resp.json()
        logger.info("Updated source '%s' with fresh token", result["name"])
        return result

    def create_source_token(self, source: dict) -> dict:
        """Create a new source via POST (bootstrap with no existing source ID).

        Used when no cache exists and the source must be recreated from
        scratch. Sends the minimal source payload via POST /api/v3/catalog.
        """
        url = f"{self.base_url}/api/v3/catalog"
        # Remove read-only fields that Dremio doesn't accept on POST
        payload = dict(source)
        payload.pop("propertyList", None)
        resp = self.session.post(url, json=payload)
        if resp.status_code != 200:
            logger.error(
                "Create source (bootstrap) failed: status=%d body=%s",
                resp.status_code, resp.text[:500],
            )
            resp.raise_for_status()
        result = resp.json()
        logger.info("Created source '%s' via bootstrap POST", result.get("name"))
        return result

    def verify_source_token(self, source_id: str, new_token: str, nessie_endpoint: str) -> bool:
        """Verify the token rotation succeeded by validating the new token
        against the Nessie API after the PUT.

        Dremio always masks secrets in GET responses as '$DREMIO_EXISTING_VALUE$',
        so re-reading the source via GET cannot confirm the token was actually
        stored. Instead we validate that the token we just pushed can
        authenticate with Nessie.
        """
        if not nessie_endpoint:
            logger.warning(
                "NESSIE_ENDPOINT not set — skipping post-rotation Nessie token validation. "
                "Cannot confirm token was stored correctly."
            )
            return True

        try:
            validate_token_against_nessie(new_token, nessie_endpoint)
            logger.info("Post-rotation token verification OK via Nessie API")
            return True
        except RuntimeError as e:
            logger.error("Post-rotation Nessie token validation FAILED: %s", e)
            return False


# ---------------------------------------------------------------------------
# OAuth2 token fetch — client_credentials grant
# ---------------------------------------------------------------------------
def fetch_oauth2_token(
    token_endpoint: str,
    client_id: str,
    client_secret: str,
    scope: str = "",
) -> tuple[str, int]:
    """
    Fetch a new bearer token using OAuth2 client_credentials grant.

    Returns (access_token, expires_in_seconds).
    """
    data: dict[str, str] = {"grant_type": "client_credentials"}
    if scope:
        data["scope"] = scope

    resp = requests.post(
        token_endpoint,
        auth=(client_id, client_secret),
        data=data,
        timeout=30,
    )

    if resp.status_code != 200:
        raise RuntimeError(
            f"OAuth2 client_credentials grant failed: "
            f"status={resp.status_code} body={resp.text[:500]}"
        )

    token_resp = resp.json()
    access_token = token_resp["access_token"]
    expires_in = int(token_resp.get("expires_in", 3600))
    return access_token, expires_in


# ---------------------------------------------------------------------------
# Token validation — test the token against the Nessie API before pushing
# ---------------------------------------------------------------------------
def validate_token_against_nessie(token: str, nessie_endpoint: str) -> None:
    """Validate that the fetched token works with the Nessie API.
    Makes a GET request to /trees and checks for a successful response.
    Raises RuntimeError if validation fails.
    """
    if not nessie_endpoint:
        logger.warning("NESSIE_ENDPOINT not set — skipping Nessie token validation")
        return

    # Ensure the endpoint ends with /trees for the validation request
    url = nessie_endpoint.rstrip("/")
    if not url.endswith("/trees"):
        url = url + "/trees"

    logger.info("Validating token against Nessie API: %s", url)
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )

    if resp.status_code == 200:
        data = resp.json()
        refs = data.get("references", [])
        logger.info(
            "Nessie token validation OK — %d references returned",
            len(refs),
        )
    elif resp.status_code == 401:
        raise RuntimeError(
            f"Nessie token validation FAILED: token rejected (HTTP 401). "
            f"The token does not authenticate with the Nessie server at {url}. "
            f"Check that the OAuth2 client ID and scope are correct."
        )
    else:
        raise RuntimeError(
            f"Nessie token validation FAILED: HTTP {resp.status_code} "
            f"from {url} — body: {resp.text[:200]}"
        )


# ---------------------------------------------------------------------------
# Source config cache — for deadlock recovery
# ---------------------------------------------------------------------------
def save_source_cache(source: dict) -> None:
    """Persist a sanitized copy of the source config to a local file.

    The nessieAccessToken is stripped because we don't need the old token
    in the cache; we'll set the new one when we use the cache for recovery.
    """
    cache = dict(source)
    cache_config = dict(cache.get("config", {}))
    cache_config.pop("nessieAccessToken", None)
    cache["config"] = cache_config
    try:
        with open(SOURCE_CACHE_PATH, "w") as f:
            json.dump(cache, f)
        logger.info("Saved source config cache to %s", SOURCE_CACHE_PATH)
    except OSError as e:
        logger.warning("Failed to save source config cache: %s", e)


def load_source_cache() -> dict | None:
    """Load the cached source config, or None if the cache doesn't exist."""
    try:
        with open(SOURCE_CACHE_PATH, "r") as f:
            cache = json.load(f)
        logger.info("Loaded source config cache from %s", SOURCE_CACHE_PATH)
        return cache
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Failed to load source config cache: %s", e)
        return None


# ---------------------------------------------------------------------------
# Minimal payload — for first-deployment bootstrap (no cache available)
# ---------------------------------------------------------------------------
def build_minimal_source_payload(source_name: str, new_token: str) -> dict:
    """Construct a minimal source config when no cache exists.

    This is used when the source is unhealthy AND no cached config is
    available (first deployment or cache wiped). The payload includes the
    required fields for Dremio's Source.toSourceConfig() to avoid NPEs.

    The tag is set to None so Dremio treats this as a create/replace,
    bypassing optimistic concurrency checks.
    """
    config = {
        "nessieEndpoint": NESSIE_SOURCE_ENDPOINT,
        "nessieAuthType": "BEARER",
        "nessieAccessToken": new_token,
        "storageProviderType": STORAGE_PROVIDER_TYPE,
        "awsRootPath": AWS_ROOT_PATH,
        "credentialType": CREDENTIAL_TYPE,
        "secure": AWS_SECURE_CONNECTION.lower() == "true",
    }

    if STORAGE_PROVIDER_TYPE == "AWS" and CREDENTIAL_TYPE == "ACCESS_KEY":
        config["awsAccessKey"] = AWS_ACCESS_KEY
        config["awsAccessSecret"] = AWS_ACCESS_SECRET

    return {
        "id": None,
        "tag": None,
        "type": "NESSIE",
        "name": source_name,
        "config": config,
        "metadataPolicy": {
            "datasetRefreshMode": "PERIODIC",
            "datasetRefreshPeriodMs": 3600000,
            "datasetExpireAfterMs": 604800000,
            "datasetAuthTTLMs": 60000,
            "deleteUnavailableDatasets": True,
        },
        "accelerationGracePeriodMs": 9600000,
        "accelerationRefreshPeriodMs": 9600000,
        "accelerationActivePolicyType": "PERIODIC",
        "accelerationNeverExpire": False,
        "accelerationNeverRefresh": False,
        "allowCrossSourceSelection": False,
        "disableMetadataValidityCheck": False,
    }


# ---------------------------------------------------------------------------
# JWT decoding — for idempotent rotation pre-check
# ---------------------------------------------------------------------------
def decode_jwt_expiry(token: str) -> int | None:
    """Decode the 'exp' claim from a JWT token.

    Returns the expiry as a Unix timestamp, or None if the token is not
    a valid JWT.
    """
    parts = token.split(".")
    if len(parts) < 2:
        return None
    try:
        # JWT base64url payload is parts[1]; pad to 4-byte boundary
        payload_b64 = parts[1] + "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        return int(payload.get("exp", 0)) or None
    except Exception:
        return None


def should_rotate() -> bool:
    """Check whether rotation is needed by testing the current token.

    If SKIP_IF_VALID is disabled, always returns True.
    If NESSIE_ENDPOINT is not set, always returns True (can't verify).
    Otherwise, tests the current token (from cache or by querying Nessie)
    and returns False if it's still valid with sufficient remaining time.
    """
    if not SKIP_IF_VALID:
        logger.info("SKIP_IF_VALID is false — forcing rotation")
        return True

    if not NESSIE_ENDPOINT:
        logger.info("NESSIE_ENDPOINT not set — cannot pre-check, proceeding with rotation")
        return True

    # Try to get the current token from the source cache
    cache = load_source_cache()
    if cache is None:
        logger.info("No source cache available — proceeding with rotation")
        return True

    current_token = cache.get("config", {}).get("nessieAccessToken")
    if not current_token:
        logger.info("No current token in cache — proceeding with rotation")
        return True

    # First try JWT expiry check
    exp = decode_jwt_expiry(current_token)
    if exp is not None:
        now = int(time.time())
        remaining = exp - now
        if remaining > TOKEN_REFRESH_MARGIN:
            logger.info(
                "Current token still valid (expires in %ds, margin=%ds) — skipping rotation",
                remaining, TOKEN_REFRESH_MARGIN,
            )
            return False
        logger.info("Current token expires in %ds (within margin) — rotation needed", remaining)
        return True

    # JWT decode failed; validate against Nessie API as fallback
    url = NESSIE_ENDPOINT.rstrip("/")
    if not url.endswith("/trees"):
        url = url + "/trees"
    try:
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {current_token}"},
            timeout=15,
        )
        if resp.status_code == 200:
            logger.info("Current token is still valid (Nessie API returned 200) — skipping rotation")
            return False
        logger.info("Current token rejected by Nessie (HTTP %d) — rotation needed", resp.status_code)
        return True
    except requests.exceptions.RequestException as e:
        logger.warning("Could not verify current token against Nessie (%s) — proceeding with rotation", e)
        return True


# ---------------------------------------------------------------------------
# Main rotation logic
# ---------------------------------------------------------------------------
@retry(max_attempts=MAX_RETRIES, delay=RETRY_DELAY, exceptions=(requests.exceptions.RequestException, RuntimeError))
def rotate_token() -> None:
    """Fetch a fresh OAuth2 token and update the Dremio Nessie source."""
    logger.info("Starting token rotation for source '%s'", NESSIE_SOURCE_NAME)

    # 0. Idempotent pre-check: skip rotation if token is still valid
    if not should_rotate():
        return

    # 1. Fetch a new bearer token from Cognito
    new_token, expires_in = fetch_oauth2_token(
        OAUTH2_TOKEN_ENDPOINT,
        OAUTH2_CLIENT_ID,
        OAUTH2_CLIENT_SECRET,
        OAUTH2_SCOPE,
    )
    logger.info("Obtained new OAuth2 token (expires_in=%ds)", expires_in)

    # 2. Validate the token against the Nessie API before pushing to Dremio
    validate_token_against_nessie(new_token, NESSIE_ENDPOINT)

    # 3. Authenticate to Dremio
    client = DremioClient(DREMIO_URL, DREMIO_USERNAME, DREMIO_PASSWORD)
    client.login()

    # 4. Get current source config (with by-path fallback)
    source = client.get_source(NESSIE_SOURCE_NAME)

    if source is not None:
        # Source is healthy — save config for future deadlock recovery
        save_source_cache(source)
    else:
        # Source is unhealthy — try to recover using cached config
        logger.warning(
            "Could not fetch source '%s' — it may be unhealthy. "
            "Attempting recovery from cached source config.",
            NESSIE_SOURCE_NAME,
        )
        source = load_source_cache()
        if source is not None:
            logger.info("Recovering using cached source config")
        else:
            # No cache available — construct a minimal payload for bootstrap
            logger.warning(
                "No cached source config available for source '%s'. "
                "Constructing minimal payload for bootstrap.",
                NESSIE_SOURCE_NAME,
            )
            source = build_minimal_source_payload(NESSIE_SOURCE_NAME, new_token)

    # 5. Update or create the nessieAccessToken
    if source.get("id") is None:
        # Bootstrap: no existing source ID, create via POST
        source = client.create_source_token(source)
    else:
        client.update_source_token(source, new_token)

    # 6. Verify the token was stored by validating against the Nessie API
    source_id = source["id"]
    if not client.verify_source_token(source_id, new_token, NESSIE_ENDPOINT):
        raise RuntimeError("Token verification failed after update")

    # 7. Update the cache with the new token for future pre-checks
    source["config"]["nessieAccessToken"] = new_token
    save_source_cache(source)

    # 8. Log when the next rotation should happen
    next_rotation = expires_in - TOKEN_REFRESH_MARGIN
    if next_rotation < 60:
        next_rotation = 60
    logger.info(
        "Token rotation complete. Expires in %ds, next rotation in %ds",
        expires_in, next_rotation,
    )


def main() -> None:
    """Entry point: run a single token rotation."""
    try:
        rotate_token()
    except Exception:
        logger.error("Token rotation failed:\n%s", traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
