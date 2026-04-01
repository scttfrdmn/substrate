# pytest-substrate

pytest plugin for the [substrate](https://github.com/scttfrdmn/substrate) AWS emulator.

## Installation

```bash
pip install pytest-substrate
# or, from source:
pip install -e /path/to/substrate/python
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
