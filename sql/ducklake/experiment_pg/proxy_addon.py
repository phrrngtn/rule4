"""
mitmproxy addon: observability, rate/bandwidth throttling, and response caching.

Features:
  - Per-host request/response/byte/timing stats with management API
  - Token-bucket request rate limiting
  - Bandwidth throttling
  - Local response cache with size-weighted LRU eviction
    (prefers keeping larger responses over smaller ones)
  - Connect timeout enforcement

Management API (via proxy, no TLS):
  GET http://proxy.stats/           -> full stats JSON
  GET http://proxy.stats/summary    -> text summary
  GET http://proxy.stats/reset      -> reset counters
  GET http://proxy.stats/cache      -> cache stats

Usage:
    mitmdump -p 8890 -s proxy_addon.py \\
        --set stream_large_bodies=1m \\
        --set rate_limit=5 \\
        --set bandwidth_mbps=2 \\
        --set cache_max_mb=500 \\
        --set connect_timeout=10
"""

import hashlib
import json
import os
import time
from collections import defaultdict, OrderedDict
from dataclasses import dataclass, field

from mitmproxy import ctx, http, connection
from mitmproxy.net.server_spec import ServerSpec


@dataclass
class HostStats:
    requests: int = 0
    responses: int = 0
    errors: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    status_codes: dict = field(default_factory=lambda: defaultdict(int))
    first_request: float = 0.0
    last_request: float = 0.0
    total_response_time_ms: float = 0.0
    min_response_time_ms: float = float("inf")
    max_response_time_ms: float = 0.0
    cache_hits: int = 0
    cache_misses: int = 0


@dataclass
class CacheEntry:
    """A cached response with metadata for eviction scoring."""
    url: str
    status_code: int
    headers: list  # list of (name, value) tuples
    content: bytes
    size: int
    created: float
    last_accessed: float
    access_count: int = 1

    @property
    def eviction_score(self):
        """Lower score = more likely to evict.
        Larger files get a bonus; recently accessed get a bonus."""
        age = time.time() - self.last_accessed
        # size_weight: prefer keeping larger files (log scale)
        import math
        size_weight = math.log2(max(self.size, 1024)) / 20.0  # normalized ~0.5-1.0
        # recency_weight: 1.0 for just accessed, decays over hours
        recency_weight = 1.0 / (1.0 + age / 3600.0)
        # access frequency bonus
        freq_weight = min(self.access_count / 10.0, 1.0)
        return size_weight * 0.4 + recency_weight * 0.4 + freq_weight * 0.2


class TrafficObserver:
    def __init__(self):
        self.stats: dict[str, HostStats] = defaultdict(HostStats)
        self.global_stats = HostStats()
        self.start_time = time.time()

        # Token bucket for rate limiting
        self.rate_limit = 0.0
        self.tokens = 0.0
        self.last_token_time = time.time()
        self.bucket_size = 10.0

        # Bandwidth limiting
        self.bandwidth_limit = 0
        self.bytes_this_second = 0
        self.bandwidth_second = int(time.time())

        # Response cache: URL -> CacheEntry, ordered by insertion for iteration
        self.cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self.cache_max_bytes = 0  # 0 = disabled
        self.cache_current_bytes = 0
        self.cache_total_hits = 0
        self.cache_total_misses = 0
        self.cache_evictions = 0

        # Connect timeout
        self.connect_timeout = 0  # seconds, 0 = default

        # Periodic summary
        self.last_summary = time.time()
        self.summary_interval = 30.0

    def load(self, loader):
        loader.add_option("rate_limit", int, 0,
                          "Max requests per second (0 = unlimited)")
        loader.add_option("bandwidth_mbps", int, 0,
                          "Max bandwidth in MB/s (0 = unlimited)")
        loader.add_option("cache_max_mb", int, 0,
                          "Max cache size in MB (0 = no caching)")
        loader.add_option("connect_timeout", int, 0,
                          "TCP connect timeout in seconds (0 = default)")

    def configure(self, updated):
        if "rate_limit" in updated:
            self.rate_limit = float(ctx.options.rate_limit)
            if self.rate_limit > 0:
                self.tokens = min(self.bucket_size, self.rate_limit)
                ctx.log.info(f"Rate limit: {self.rate_limit} req/s (burst: {self.bucket_size})")
            else:
                ctx.log.info("Rate limiting: disabled")

        if "bandwidth_mbps" in updated:
            mbps = ctx.options.bandwidth_mbps
            self.bandwidth_limit = int(mbps * 1024 * 1024) if mbps > 0 else 0
            if self.bandwidth_limit > 0:
                ctx.log.info(f"Bandwidth limit: {mbps} MB/s ({self.bandwidth_limit:,} bytes/s)")
            else:
                ctx.log.info("Bandwidth limiting: disabled")

        if "cache_max_mb" in updated:
            mb = ctx.options.cache_max_mb
            self.cache_max_bytes = mb * 1024 * 1024 if mb > 0 else 0
            if self.cache_max_bytes > 0:
                ctx.log.info(f"Cache: {mb}MB max")
            else:
                ctx.log.info("Caching: disabled")

        if "connect_timeout" in updated:
            self.connect_timeout = ctx.options.connect_timeout
            if self.connect_timeout > 0:
                ctx.log.info(f"Connect timeout: {self.connect_timeout}s")
            else:
                ctx.log.info("Connect timeout: default")

    # ── Rate / bandwidth helpers ──────────────────────────────────────

    def _acquire_token(self):
        if self.rate_limit <= 0:
            return 0.0
        now = time.time()
        elapsed = now - self.last_token_time
        self.last_token_time = now
        self.tokens = min(self.bucket_size, self.tokens + elapsed * self.rate_limit)
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return 0.0
        return (1.0 - self.tokens) / self.rate_limit

    def _check_bandwidth(self, size):
        if self.bandwidth_limit <= 0:
            return 0.0
        now_sec = int(time.time())
        if now_sec != self.bandwidth_second:
            self.bytes_this_second = 0
            self.bandwidth_second = now_sec
        if self.bytes_this_second + size > self.bandwidth_limit:
            return 1.0
        self.bytes_this_second += size
        return 0.0

    # ── Cache helpers ─────────────────────────────────────────────────

    def _cache_key(self, flow):
        """Cache key = method + full URL."""
        return f"{flow.request.method}:{flow.request.pretty_url}"

    def _cache_evict(self, needed_bytes):
        """Evict entries until we have room. Size-weighted LRU: evict lowest-scoring first."""
        if not self.cache:
            return
        while self.cache_current_bytes + needed_bytes > self.cache_max_bytes and self.cache:
            # Sort by eviction score, remove lowest
            worst_key = min(self.cache, key=lambda k: self.cache[k].eviction_score)
            entry = self.cache.pop(worst_key)
            self.cache_current_bytes -= entry.size
            self.cache_evictions += 1

    def _cache_get(self, key):
        if key in self.cache:
            entry = self.cache[key]
            entry.last_accessed = time.time()
            entry.access_count += 1
            # Move to end (most recently used)
            self.cache.move_to_end(key)
            return entry
        return None

    def _cache_put(self, key, flow):
        """Store response in cache if caching is enabled and response is cacheable."""
        if self.cache_max_bytes <= 0:
            return
        content = flow.response.raw_content or b""
        size = len(content)
        if size == 0:
            return
        # Only cache successful GET responses
        if flow.request.method != "GET" or flow.response.status_code != 200:
            return
        # Don't cache if larger than 25% of total cache
        if size > self.cache_max_bytes * 0.25:
            return

        self._cache_evict(size)
        now = time.time()
        entry = CacheEntry(
            url=flow.request.pretty_url,
            status_code=flow.response.status_code,
            headers=list(flow.response.headers.items()),
            content=content,
            size=size,
            created=now,
            last_accessed=now,
        )
        self.cache[key] = entry
        self.cache_current_bytes += size

    # ── Stats JSON builder ────────────────────────────────────────────

    def _build_stats_json(self):
        elapsed = time.time() - self.start_time
        g = self.global_stats
        avg_ms = g.total_response_time_ms / g.responses if g.responses else 0

        hosts = {}
        for host, hs in sorted(self.stats.items(), key=lambda x: -x[1].bytes_received):
            havg = hs.total_response_time_ms / hs.responses if hs.responses else 0
            hosts[host] = {
                "requests": hs.requests,
                "responses": hs.responses,
                "errors": hs.errors,
                "bytes_sent": hs.bytes_sent,
                "bytes_received": hs.bytes_received,
                "status_codes": dict(hs.status_codes),
                "avg_response_ms": round(havg, 1),
                "min_response_ms": round(hs.min_response_time_ms, 1) if hs.min_response_time_ms != float("inf") else 0,
                "max_response_ms": round(hs.max_response_time_ms, 1),
                "cache_hits": hs.cache_hits,
                "cache_misses": hs.cache_misses,
            }
        return {
            "uptime_seconds": round(elapsed, 1),
            "global": {
                "requests": g.requests,
                "responses": g.responses,
                "errors": g.errors,
                "bytes_sent": g.bytes_sent,
                "bytes_received": g.bytes_received,
                "avg_response_ms": round(avg_ms, 1),
                "rate_req_per_sec": round(g.requests / elapsed, 2) if elapsed > 0 else 0,
                "throughput_mbps": round(g.bytes_received / elapsed / 1024 / 1024, 2) if elapsed > 0 else 0,
                "status_codes": dict(g.status_codes),
            },
            "config": {
                "rate_limit_rps": self.rate_limit,
                "bandwidth_limit_mbps": self.bandwidth_limit / 1024 / 1024 if self.bandwidth_limit else 0,
                "cache_max_mb": self.cache_max_bytes / 1024 / 1024 if self.cache_max_bytes else 0,
                "connect_timeout_s": self.connect_timeout,
            },
            "cache": {
                "entries": len(self.cache),
                "current_mb": round(self.cache_current_bytes / 1024 / 1024, 2),
                "max_mb": round(self.cache_max_bytes / 1024 / 1024, 2) if self.cache_max_bytes else 0,
                "total_hits": self.cache_total_hits,
                "total_misses": self.cache_total_misses,
                "evictions": self.cache_evictions,
                "hit_rate_pct": round(self.cache_total_hits / max(1, self.cache_total_hits + self.cache_total_misses) * 100, 1),
            },
            "hosts": hosts,
        }

    def _build_cache_detail(self):
        """Return cache contents for the /cache endpoint."""
        entries = []
        for key, entry in self.cache.items():
            entries.append({
                "url": entry.url[:120],
                "size_kb": round(entry.size / 1024, 1),
                "accesses": entry.access_count,
                "age_s": round(time.time() - entry.created, 0),
                "score": round(entry.eviction_score, 3),
            })
        entries.sort(key=lambda e: -e["score"])
        return {
            "entries": len(entries),
            "current_mb": round(self.cache_current_bytes / 1024 / 1024, 2),
            "max_mb": round(self.cache_max_bytes / 1024 / 1024, 2) if self.cache_max_bytes else 0,
            "total_hits": self.cache_total_hits,
            "total_misses": self.cache_total_misses,
            "evictions": self.cache_evictions,
            "contents": entries,
        }

    # ── mitmproxy hooks ───────────────────────────────────────────────

    def request(self, flow: http.HTTPFlow):
        host = flow.request.pretty_host
        now = time.time()

        # Management API
        if host == "proxy.stats":
            path = flow.request.path.rstrip("/")
            if path == "/reset":
                self.stats.clear()
                self.global_stats = HostStats()
                self.start_time = time.time()
                flow.response = http.Response.make(200, b'{"status":"reset"}\n',
                                                    {"Content-Type": "application/json"})
            elif path == "/summary":
                stats = self._build_stats_json()
                g = stats["global"]
                c = stats["cache"]
                lines = [
                    f"Uptime: {stats['uptime_seconds']:.0f}s",
                    f"Requests: {g['requests']}  Responses: {g['responses']}  Errors: {g['errors']}",
                    f"Sent: {g['bytes_sent']/1024/1024:.1f}MB  Received: {g['bytes_received']/1024/1024:.1f}MB",
                    f"Rate: {g['rate_req_per_sec']:.1f} req/s  Throughput: {g['throughput_mbps']:.2f} MB/s",
                    f"Status codes: {g['status_codes']}",
                    f"Cache: {c['entries']} entries, {c['current_mb']:.1f}/{c['max_mb']:.0f}MB, "
                    f"hit={c['total_hits']} miss={c['total_misses']} evict={c['evictions']} "
                    f"({c['hit_rate_pct']:.0f}% hit rate)",
                    f"",
                    f"Per-host:",
                ]
                for h, hs in stats["hosts"].items():
                    lines.append(
                        f"  {h:40s} req={hs['requests']:<5d} "
                        f"recv={hs['bytes_received']/1024/1024:>6.1f}MB "
                        f"avg={hs['avg_response_ms']:>6.0f}ms "
                        f"hit={hs['cache_hits']} miss={hs['cache_misses']} "
                        f"codes={hs['status_codes']}"
                    )
                flow.response = http.Response.make(200, "\n".join(lines) + "\n",
                                                    {"Content-Type": "text/plain"})
            elif path == "/cache":
                detail = self._build_cache_detail()
                flow.response = http.Response.make(
                    200, json.dumps(detail, indent=2) + "\n",
                    {"Content-Type": "application/json"})
            else:
                stats = self._build_stats_json()
                flow.response = http.Response.make(
                    200, json.dumps(stats, indent=2) + "\n",
                    {"Content-Type": "application/json"})
            return

        # Check cache before making upstream request
        cache_key = self._cache_key(flow)
        cached = self._cache_get(cache_key)
        if cached:
            self.cache_total_hits += 1
            self.stats[host].cache_hits += 1
            # Serve from cache
            flow.response = http.Response.make(
                cached.status_code,
                cached.content,
                dict(cached.headers),
            )
            flow.response.headers["X-Cache"] = "HIT"
            ctx.log.info(f"[CACHE HIT] {host}{flow.request.path[:60]} {cached.size:,}B")
            return
        else:
            if self.cache_max_bytes > 0:
                self.cache_total_misses += 1
                self.stats[host].cache_misses += 1

        # Rate limiting
        wait = self._acquire_token()
        if wait > 0:
            ctx.log.info(f"[THROTTLE] {host}: waiting {wait:.2f}s (rate limit)")
            time.sleep(wait)

        hs = self.stats[host]
        hs.requests += 1
        if hs.first_request == 0:
            hs.first_request = now
        hs.last_request = now
        hs.bytes_sent += len(flow.request.raw_content or b"")

        self.global_stats.requests += 1
        self.global_stats.bytes_sent += len(flow.request.raw_content or b"")

        flow.metadata["request_start"] = now

    def server_connect(self, data: connection.ServerConnection):
        """Enforce connect timeout."""
        if self.connect_timeout > 0:
            data.connection.settimeout(self.connect_timeout)

    def responseheaders(self, flow: http.HTTPFlow):
        host = flow.request.pretty_host
        if host == "proxy.stats":
            return
        cl = flow.response.headers.get("content-length")
        if cl:
            try:
                flow.metadata["content_length"] = int(cl)
            except (ValueError, TypeError):
                pass

    def response(self, flow: http.HTTPFlow):
        host = flow.request.pretty_host
        if host == "proxy.stats":
            return
        hs = self.stats[host]

        resp_size = len(flow.response.raw_content or b"")
        if resp_size == 0:
            resp_size = flow.metadata.get("content_length", 0)
        status = flow.response.status_code

        # Bandwidth limiting
        wait = self._check_bandwidth(resp_size)
        if wait > 0:
            ctx.log.info(f"[THROTTLE] {host}: waiting {wait:.1f}s (bandwidth: {resp_size:,} bytes)")
            time.sleep(wait)

        hs.responses += 1
        hs.bytes_received += resp_size
        hs.status_codes[status] += 1

        self.global_stats.responses += 1
        self.global_stats.bytes_received += resp_size
        self.global_stats.status_codes[status] += 1

        # Response time
        start = flow.metadata.get("request_start", 0)
        elapsed_ms = 0
        if start:
            elapsed_ms = (time.time() - start) * 1000
            hs.total_response_time_ms += elapsed_ms
            hs.min_response_time_ms = min(hs.min_response_time_ms, elapsed_ms)
            hs.max_response_time_ms = max(hs.max_response_time_ms, elapsed_ms)
            self.global_stats.total_response_time_ms += elapsed_ms

        # Cache the response (only for non-streamed responses with content)
        cache_key = self._cache_key(flow)
        self._cache_put(cache_key, flow)

        # Per-request log line
        method = flow.request.method
        path = flow.request.path[:80]
        ctx.log.info(
            f"[{status}] {method} {host}{path} "
            f"{resp_size:,}B {elapsed_ms:.0f}ms"
        )

        # Periodic summary
        now = time.time()
        if now - self.last_summary > self.summary_interval:
            self._print_summary()
            self.last_summary = now

    def error(self, flow: http.HTTPFlow):
        host = flow.request.pretty_host
        self.stats[host].errors += 1
        self.global_stats.errors += 1
        ctx.log.warn(f"[ERROR] {host}: {flow.error.msg if flow.error else 'unknown'}")

    def _print_summary(self):
        elapsed = time.time() - self.start_time
        g = self.global_stats
        avg_ms = g.total_response_time_ms / g.responses if g.responses else 0
        ctx.log.info(
            f"\n{'─'*70}\n"
            f"TRAFFIC SUMMARY ({elapsed:.0f}s elapsed)\n"
            f"  Requests: {g.requests}  Responses: {g.responses}  Errors: {g.errors}\n"
            f"  Sent: {g.bytes_sent/1024/1024:.1f}MB  Received: {g.bytes_received/1024/1024:.1f}MB\n"
            f"  Avg response: {avg_ms:.0f}ms  Rate: {g.requests/elapsed:.1f} req/s\n"
            f"  Cache: {len(self.cache)} entries, {self.cache_current_bytes/1024/1024:.1f}MB, "
            f"hit={self.cache_total_hits} miss={self.cache_total_misses}\n"
            f"{'─'*70}"
        )

    def done(self):
        ctx.log.info("\n\nFINAL SUMMARY:")
        self._print_summary()


addons = [TrafficObserver()]
