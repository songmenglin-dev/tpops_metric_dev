"""TPOPS (GaussDB v3 monitoring platform) API client.

Credentials come from the environment -- never hardcode them:

    export TPOPS_USER=sml
    export TPOPS_PASSWORD='...'
    export TPOPS_BASE_URL=https://172.16.103.193:8002   # optional
"""

import os
import sys
import time

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DEFAULT_BASE_URL = "https://172.16.103.193:8002"

METRIC_GROUPS = [
    "CPUMEMORY", "CONNECTION", "SYNCSTAT", "PROCESSRESOURCE", "NETWORK",
    "TRANSACTION", "IOSTORAGE", "LOCK", "INSTATUS", "FORDR",
    "AGENTSTATUS", "COREMETRICS",
]


class TpopsError(RuntimeError):
    """Raised when the platform rejects a request or returns an unexpected shape."""


class TpopsClient:
    def __init__(self, base_url=None, verify_tls=False, timeout=60):
        self.base_url = (base_url or os.environ.get("TPOPS_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        self.verify_tls = verify_tls
        self.timeout = timeout
        self._token = None
        self.session = requests.Session()
        self.session.verify = verify_tls

    # --- auth -------------------------------------------------------------

    @property
    def username(self):
        user = os.environ.get("TPOPS_USER")
        if not user:
            raise TpopsError("TPOPS_USER is not set")
        return user

    @property
    def password(self):
        pwd = os.environ.get("TPOPS_PASSWORD")
        if not pwd:
            raise TpopsError("TPOPS_PASSWORD is not set")
        return pwd

    def token(self, refresh=False):
        """Fetch an X-Mini-Token. Tokens expire after the server's session_timeout."""
        if self._token and not refresh:
            return self._token
        resp = self.session.post(
            f"{self.base_url}/rds/v3/auth/mini-token",
            json={"username": self.username, "password": self.password},
            timeout=self.timeout,
        )
        self._raise_for_status(resp, "mini-token")
        token = resp.headers.get("X-Mini-Token")
        if not token:
            raise TpopsError(f"no X-Mini-Token header in response: {dict(resp.headers)}")
        self._token = token
        return token

    def _headers(self):
        return {"X-Mini-Token": self.token(), "X-Language": "zh-cn"}

    # --- resources --------------------------------------------------------

    def instances(self, limit=100, offset=0):
        """GET /rds/v3.2/instances -- list database instances."""
        resp = self.session.get(
            f"{self.base_url}/rds/v3.2/instances",
            params={"limit": limit, "offset": offset},
            headers=self._headers(),
            timeout=self.timeout,
        )
        self._raise_for_status(resp, "instances")
        return resp.json()

    def metric_catalog(self, groups=None, instance_id=None):
        """GET /rds/v3/monitor/metrics/series -- {group: [{name, unit, alarm_id, ...}]}.

        `name` accepts repeated values, so every group is fetched in one request.
        Pass `instance_id` to also resolve each metric's alarm_id (the platform
        leaves alarm_threshold null either way).

        The platform has no endpoint that enumerates the available groups, so the
        group codes must be supplied up front (METRIC_GROUPS). A group name that
        does not exist returns HTTP 200 with an empty series rather than an error,
        so empty groups are reported on stderr -- they usually mean a stale group
        list, not a group that legitimately has no metrics.
        """
        groups = list(groups or METRIC_GROUPS)
        params = [("name", g) for g in groups]
        if instance_id:
            params.append(("instance_id", instance_id))

        resp = self.session.get(
            f"{self.base_url}/rds/v3/monitor/metrics/series",
            params=params,
            headers=self._headers(),
            timeout=self.timeout,
        )
        self._raise_for_status(resp, "metric series")

        catalog = {group: [] for group in groups}
        for series in resp.json().get("series", []):
            group = series.get("name")
            if group is None:
                continue
            catalog.setdefault(group, []).extend(
                m for m in series.get("metrics", []) if m.get("name")
            )

        empty = [g for g in groups if not catalog.get(g)]
        if empty:
            print(
                f"WARNING: these metric groups returned no metrics: {', '.join(empty)}\n"
                f"         The platform returns HTTP 200 for unknown group names, so this\n"
                f"         usually means METRIC_GROUPS is stale or misspelled.",
                file=sys.stderr,
            )
        return catalog

    def metrics(self, instance_ids, metric_names, start_ms, end_ms, latest_only=False):
        """POST /rds/v3/monitor/metrics -- metric data for the given instances."""
        payload = {
            "start_time": start_ms,
            "end_time": end_ms,
            "is_latest_point_only": "true" if latest_only else "false",
            "instances": [{"instance_id": iid} for iid in instance_ids],
            "metric_names": list(metric_names),
        }
        resp = self.session.post(
            f"{self.base_url}/rds/v3/monitor/metrics",
            json=payload,
            headers=self._headers(),
            timeout=max(self.timeout, 300),
        )
        self._raise_for_status(resp, "metrics")
        return resp.json()

    def metrics_latest(self, instance_ids, metric_names, window_ms=24 * 60 * 60 * 1000):
        now_ms = int(time.time() * 1000)
        return self.metrics(instance_ids, metric_names, now_ms - window_ms, now_ms, latest_only=True)

    # --- alarms -----------------------------------------------------------

    def alarm_records(self, is_active=True, is_convergence=True, limit=100, offset=0,
                      alarm_id=None, instance_id=None):
        """POST /rds/v3/alarm/records -- list alarm records.

        is_active     True -> currently active alarms; False -> recovered/cleared history.
        is_convergence True -> grouped list (one row per instance+alarm_id);
                      False -> detail rows (one per node, with causes and fix steps).
        alarm_id / instance_id only apply when is_convergence is False.
        """
        payload = {
            "is_active": is_active,
            "is_convergence": is_convergence,
            "limit": limit,
            "offset": offset,
        }
        if alarm_id:
            payload["alarm_id"] = alarm_id
        if instance_id:
            payload["instance_id"] = instance_id
        resp = self.session.post(
            f"{self.base_url}/rds/v3/alarm/records",
            json=payload,
            headers=self._headers(),
            timeout=self.timeout,
        )
        self._raise_for_status(resp, "alarm records")
        return resp.json()

    def alarm_statistics(self):
        """GET /rds/v3/alarm/records/statistic -- alarm counts per severity.

        Returns system_severity_statistics and instance_severity_statistics,
        each with four entries (notice / minor / major / critical).
        """
        resp = self.session.get(
            f"{self.base_url}/rds/v3/alarm/records/statistic",
            headers=self._headers(),
            timeout=self.timeout,
        )
        self._raise_for_status(resp, "alarm statistic")
        return resp.json()

    # --- alarm rule configuration (the /gaussdb/v3 namespace) --------------

    def console_login(self):
        """POST /gaussdb/v3/login -- log in to the GaussDB **console** namespace.

        The console namespace (/gaussdb/v3/...) is a *different* auth domain from
        /rds/v3/...: it needs a CONSOLE-type session, not the OPENAPI mini-token.

        Returns (jwt, csrf). Both come back as response headers:
          X-Mini-Token -- the console JWT (claims carry login_type=CONSOLE)
          Csrf         -- the CSRF token to echo back in `x-mini-csrf`
        """
        resp = self.session.post(
            f"{self.base_url}/gaussdb/v3/login",
            json={"username": self.username, "password": self.password},
            headers={"Content-Type": "application/json",
                     "service-tag": "luban", "X-Language": "zh-cn"},
            timeout=self.timeout,
        )
        self._raise_for_status(resp, "console login")
        jwt = resp.headers.get("X-Mini-Token")
        csrf = resp.headers.get("Csrf")
        if not jwt or not csrf:
            raise TpopsError("console login did not return X-Mini-Token / Csrf")
        return jwt, csrf

    def alarm_rule_configs(self, limit=100, offset=0, jwt=None, csrf=None):
        """GET /gaussdb/v3/alarm/configs -- the alarm **rule configuration** catalogue.

        This is the authoritative alarm->metric mapping: every rule carries
        `metric_code` (the platform metric id) plus the thresholds, observation
        policy, effective window and the Chinese alarm_name/description.

        NOTE the namespace: this endpoint lives under /gaussdb/v3/, NOT
        /rds/v3/ or /rds/gaussdb/v3/. It is routed by two headers:
          service-tag: luban   -- without it the route 404s
          x-mini-token: gaussdbtpops -- a literal service tag, NOT the JWT
                                     (putting the JWT here also 404s)
        Plus the console session: the CONSOLE JWT in the cookie and the Csrf
        echoed in `x-mini-csrf`. Missing/!matching -> 403.

        `x-language: zh-cn` switches alarm_name/description to Chinese
        (the default is English).
        """
        if jwt is None or csrf is None:
            jwt, csrf = self.console_login()
        cookie = (f"session_timeout=30; X-Mini-Token={jwt}; isLogin=logged; "
                  f"userName={self.username}; ts={int(time.time() * 1000)}")
        headers = {
            "service-tag": "luban",
            "x-mini-token": "gaussdbtpops",
            "x-mini-csrf": csrf,
            "cookie": cookie,
            "X-Language": "zh-cn",
        }
        resp = self.session.get(
            f"{self.base_url}/gaussdb/v3/alarm/configs",
            params={"limit": limit, "offset": offset},
            headers=headers,
            timeout=self.timeout,
        )
        self._raise_for_status(resp, "alarm rule configs")
        return resp.json()

    def alarm_rule_catalog(self, jwt=None, csrf=None):
        """Every alarm rule config, paging until exhausted.

        Returns (rules, total_count). Each rule is a dict as returned by the
        platform -- see data/alarm/alarm_configs.json for a full capture.
        """
        if jwt is None or csrf is None:
            jwt, csrf = self.console_login()
        rules = []
        offset = 0
        total = None
        while True:
            body = self.alarm_rule_configs(limit=100, offset=offset, jwt=jwt, csrf=csrf)
            page = body.get("alarm_rule_config") or []
            total = body.get("total_count", total)
            rules.extend(page)
            offset += len(page)
            if not page or (total is not None and offset >= total):
                break
        return rules, total

    # --- helpers ----------------------------------------------------------

    @staticmethod
    def _raise_for_status(resp, what):
        if not resp.ok:
            raise TpopsError(f"{what} failed: HTTP {resp.status_code} {resp.text[:300]}")
