---
title: "S3 (Niquests)"
---

# S3 (Niquests)

S3 helpers using [niquests](https://github.com/jawah/niquests) and [botocore](https://github.com/boto/botocore).

## Installation

```bash
uv add tracktolib[s3-niquests]
```

## Dependencies

- [niquests](https://github.com/jawah/niquests) - HTTP/3 capable requests replacement
- [botocore](https://github.com/boto/botocore) - AWS SDK core

## Overview

This module provides S3 functionality using `niquests` as the HTTP backend instead of the default `urllib3`. This can be useful when you need:

- HTTP/3 support
- Better async compatibility
- Different connection pooling behavior

## Usage

```python
from tracktolib.s3.niquests import ...
```

See the source code at `tracktolib/s3/niquests.py` for available functions.
