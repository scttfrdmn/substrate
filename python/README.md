# pytest-substrate

pytest plugin for the [substrate](https://github.com/scttfrdmn/substrate) AWS emulator.

## Installation

pytest-substrate is installed from source until it is published to PyPI
([#1616](https://github.com/scttfrdmn/substrate/issues/1616)):

```bash
pip install "git+https://github.com/scttfrdmn/substrate.git#subdirectory=python"
# or, from a checkout:
pip install -e /path/to/substrate/python
```

The plugin does not depend on boto3 — bring your own client. To run *this package's* own
tests, install the `test` extra, which adds the boto3 the botocore-driven tests need:

```bash
pip install -e '/path/to/substrate/python[test]'
```

## Usage

Add the `substrate` fixture to any test that needs AWS services:

```python
import boto3

def test_s3_upload(substrate):
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket="my-bucket")
    s3.put_object(Bucket="my-bucket", Key="hello.txt", Body=b"hello")
    obj = s3.get_object(Bucket="my-bucket", Key="hello.txt")
    assert obj["Body"].read() == b"hello"
```

The `substrate` fixture:
- Starts a substrate server once per test session (via `substrate_server`)
- Resets all state before each test
- Sets `AWS_ENDPOINT_URL`, `AWS_DEFAULT_REGION`, and dummy credentials via `monkeypatch`

## Fixtures

### `substrate_server` (session-scoped)

Manages the substrate process lifecycle. Starts once, stops after all tests complete.

### `substrate` (function-scoped)

Resets state and configures env vars. Yields the `SubstrateServer` instance.

## Configuration

Set `SUBSTRATE_BINARY` to override the binary path. Otherwise, the plugin looks for:
1. `~/src/substrate/bin/substrate`
2. `substrate` on `PATH`

## Deterministic runs

Every server the plugin launches gets `SUBSTRATE_RUN_SEED=1`, so request IDs, and the
identifiers substrate mints from them, are the same on every run. The per-test reset
rewinds the sequence. To pin the simulated clock as well, mark the test:

```python
@pytest.mark.substrate(deterministic=True)  # clock frozen at 2026-01-01T00:00:00Z
def test_stable(substrate_isolated):
    ...
```

The marker also takes `epoch=` (a datetime or an RFC 3339 string) and `seed=` (0 for
unseeded IDs). On the shared `substrate` fixture it applies to that test alone. Configure
the session server with the ini options `substrate_seed`, `substrate_epoch` and
`substrate_deterministic`.
