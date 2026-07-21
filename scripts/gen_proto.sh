#!/usr/bin/env bash
# Regenerate the protobuf/gRPC stubs. Run after editing any .proto.
#
#   ./scripts/gen_proto.sh
#
# Output is COMMITTED to git so that `uv sync && uv run pytest` works with no
# codegen step, and so a diff shows when the wire contract moved.
#
# The proto lives at proto/pyka/v1/broker.proto and is compiled with -I proto,
# so its package path becomes pyka/v1/ and generated imports resolve as
# `from pyka.v1 import broker_pb2`. Getting this wrong is the classic
# gRPC-in-Python papercut: protoc emits a bare `import broker_pb2` that fails
# inside a package, and people patch it with sed. Matching the directory
# layout to the proto package avoids the problem instead of repairing it.
set -euo pipefail
cd "$(dirname "$0")/.."

uv run python -m grpc_tools.protoc \
    -I proto \
    --python_out=src \
    --grpc_python_out=src \
    --pyi_out=src \
    proto/pyka/v1/broker.proto

echo "generated:"
ls -1 src/pyka/v1/
