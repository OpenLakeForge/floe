"""Create the spike catalog, principal, grants and namespace in the local Polaris. Idempotent.

Stdlib only. Follows the setup in Apache Polaris's getting-started/quickstart compose (1.8.0),
except that the principal gets fixed credentials (via the reset API) so every spike can hard-code
them. Values must match docker-compose.yml and smoke.py.
"""

import json
import urllib.error
import urllib.parse
import urllib.request

POLARIS = "http://localhost:8181/api"
ROOT_ID, ROOT_SECRET = "root", "s3cr3t"
CATALOG = "spike"
PRINCIPAL, PRINCIPAL_ROLE = "spike", "spike_role"
CLIENT_ID, CLIENT_SECRET = "spike-client", "spike-secret"
NAMESPACE = "spike"
BUCKET = "s3://warehouse"


def call(method, path, token=None, body=None, form=None):
    """Send a request; treat 409 (already exists) as success."""
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    data = None
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
    elif body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(f"{POLARIS}{path}", data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as e:
        if e.code != 409:
            raise SystemExit(f"{method} {path} -> {e.code}: {e.read().decode()}")
        print(f"  exists: {path}")
        return None
    print(f"  ok:     {method} {path}")
    return json.loads(raw) if raw else None


def token(client_id, client_secret):
    form = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "PRINCIPAL_ROLE:ALL",
    }
    return call("POST", "/catalog/v1/oauth/tokens", form=form)["access_token"]


def main():
    root = token(ROOT_ID, ROOT_SECRET)
    call("POST", "/management/v1/catalogs", root, {"catalog": {
        "name": CATALOG,
        "type": "INTERNAL",
        "properties": {"default-base-location": f"{BUCKET}/{CATALOG}"},
        "storageConfigInfo": {
            "storageType": "S3",
            "allowedLocations": [BUCKET],
            "endpoint": "http://localhost:8333",
            "endpointInternal": "http://seaweedfs:8333",
            "pathStyleAccess": True,
            "region": "us-east-1",
            # SeaweedFS has no STS here: Polaris vends no credentials, clients use the S3 key.
            "stsUnavailable": True,
        },
    }})
    call("POST", "/management/v1/principals", root, {"principal": {"name": PRINCIPAL}})
    # Pin the credentials so reruns (and every spike) use the same ones.
    call("POST", f"/management/v1/principals/{PRINCIPAL}/reset", root,
         {"clientId": CLIENT_ID, "clientSecret": CLIENT_SECRET})
    call("POST", "/management/v1/principal-roles", root,
         {"principalRole": {"name": PRINCIPAL_ROLE}})
    call("PUT", f"/management/v1/principals/{PRINCIPAL}/principal-roles", root,
         {"principalRole": {"name": PRINCIPAL_ROLE}})
    # catalog_admin is created with the catalog; give it to the principal role, plus table access.
    call("PUT", f"/management/v1/principal-roles/{PRINCIPAL_ROLE}/catalog-roles/{CATALOG}", root,
         {"catalogRole": {"name": "catalog_admin"}})
    call("PUT", f"/management/v1/catalogs/{CATALOG}/catalog-roles/catalog_admin/grants", root,
         {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}})
    # Created as the spike principal, which also checks its credentials and grants.
    call("POST", f"/catalog/v1/{CATALOG}/namespaces", token(CLIENT_ID, CLIENT_SECRET),
         {"namespace": [NAMESPACE]})
    print(f"ready: catalog={CATALOG} namespace={NAMESPACE} client={CLIENT_ID}")


if __name__ == "__main__":
    main()
