---
title: "Cloudflare"
---

# Cloudflare

Cloudflare DNS API helpers using [niquests](https://github.com/jawah/niquests).

## Installation

```bash
uv add tracktolib[cf]
```

## Dependencies

- [niquests](https://github.com/jawah/niquests) - Modern HTTP client with HTTP/3 support

## Overview

This module provides an async client for the [Cloudflare DNS API](https://developers.cloudflare.com/api/resources/dns/subresources/records/):

- DNS record management (get, create, update, delete)
- Support for any record type (CNAME, A, AAAA, TXT, etc.)
- Existence checks

## Authentication

The client uses environment variables by default:

- `CLOUDFLARE_API_TOKEN` - API token with DNS edit permissions
- `CLOUDFLARE_ZONE_ID` - Zone ID for the domain

```python
from tracktolib.cf import CloudflareDNSClient

async with CloudflareDNSClient() as cf:
    # ... use client
```

Or pass credentials explicitly:

```python
async with CloudflareDNSClient(token="xxx", zone_id="yyy") as cf:
    # ... use client
```

## DNS Records

### `get_dns_record(name, record_type) -> DnsRecord | None`

Get a DNS record by name and type. Returns `None` if not found.

```python
record = await cf.get_dns_record("app.example.com", "CNAME")
if record:
    print(f"{record['name']} -> {record['content']}")
```

### `create_dns_record(name, content, record_type, *, ttl, proxied, comment) -> DnsRecord`

Create a DNS record. The `ttl` parameter defaults to `1` (automatic).

```python
record = await cf.create_dns_record(
    "app.example.com",
    "target.example.com",
    record_type="CNAME",
    ttl=60,
    proxied=True,
    comment="Created by deployment script",
)
print(f"Created record {record['id']}")
```

### `update_dns_record(record_id, *, content, name, record_type, ttl, proxied, comment) -> DnsRecord`

Update a DNS record by ID. Only the provided fields will be updated; omitted fields remain unchanged.

```python
record = await cf.update_dns_record(
    "023e105f4ecef8ad9ca31a8372d0c353",
    content="new-target.example.com",
    proxied=True,
)
print(f"Updated record {record['name']} -> {record['content']}")
```

### `delete_dns_record(record_id) -> None`

Delete a DNS record by ID.

```python
await cf.delete_dns_record("023e105f4ecef8ad9ca31a8372d0c353")
```

### `delete_dns_record_by_name(name, record_type) -> bool`

Delete a DNS record by name and type. Returns `True` if deleted, `False` if not found.

```python
if await cf.delete_dns_record_by_name("app.example.com", "CNAME"):
    print("Record deleted")
else:
    print("Record not found")
```

### `dns_record_exists(name, record_type) -> bool`

Check if a DNS record exists.

```python
if await cf.dns_record_exists("app.example.com"):
    print("Record exists")
```

## Configuration

### Retries

Configure automatic retries:

```python
from urllib3.util.retry import Retry

retry = Retry(total=3, backoff_factor=0.5)
async with CloudflareDNSClient(retries=retry) as cf:
    # ...
```

### Request Hooks

Add custom hooks for logging or metrics:

```python
def log_response(response, **kwargs):
    print(f"{response.request.method} {response.url} -> {response.status_code}")

async with CloudflareDNSClient(hooks={"response": [log_response]}) as cf:
    # ...
```

## Error Handling

The client raises `CloudflareError` on API failures:

```python
from tracktolib.cf import CloudflareDNSClient, CloudflareError

try:
    await cf.create_dns_record("invalid", "target.com")
except CloudflareError as e:
    print(f"Cloudflare API error: {e}")
    print(f"Status code: {e.status_code}")
    print(f"Errors: {e.errors}")
```

## Types

The module exports a TypedDict for DNS record responses:

- `DnsRecord` - DNS record data (id, name, type, content, ttl, proxied, etc.)

```python
from tracktolib.cf import DnsRecord
```