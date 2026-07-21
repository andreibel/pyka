# pyKA broker.
#
#   docker build -t pyka:dev .
#   docker run --rm -p 9092:9092 -p 8080:8080 -v pyka-data:/var/lib/pyka pyka:dev
#
# Two stages: the builder has uv and a compiler toolchain, the runtime has
# neither. Only the finished virtualenv crosses over, so nothing that could
# build code ships to production.

# ---------------------------------------------------------------- builder
FROM python:3.13-slim AS builder

# uv from its own distroless image rather than `pip install uv`: one COPY,
# no pip resolution, and the version is pinned by the tag.
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Dependencies first, project second. Dependencies change rarely and source
# changes constantly, so this ordering means editing a .py file rebuilds one
# small layer instead of reinstalling grpcio every time.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project --no-editable

COPY src/ ./src/
# --no-editable matters: the default installs the project as a .pth pointing
# back at /app/src, so a venv copied without the source tree imports nothing.
# This builds a real wheel into site-packages, and the venv is self-contained.
RUN uv sync --frozen --no-dev --no-editable

# ---------------------------------------------------------------- runtime
FROM python:3.13-slim AS runtime

LABEL org.opencontainers.image.title="pyKA" \
      org.opencontainers.image.description="A mini Kafka: append-only log storage with a gRPC broker" \
      org.opencontainers.image.source="https://github.com/andreibel/pyKA" \
      org.opencontainers.image.licenses="MIT"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

# A fixed, high UID rather than whatever `useradd` picks. Kubernetes pins the
# same number in runAsUser/fsGroup, and a mismatch is the single most common
# reason a first StatefulSet cannot write to its PersistentVolumeClaim.
RUN groupadd --gid 10001 pyka \
 && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin pyka \
 && mkdir -p /var/lib/pyka \
 && chown 10001:10001 /var/lib/pyka

# Only the virtualenv crosses the stage boundary — no uv, no compilers, no
# lockfiles. Paths match the builder's so the venv's shebangs stay valid.
COPY --from=builder --chown=10001:10001 /app/.venv /app/.venv

WORKDIR /app
USER 10001:10001

# The data root. In Kubernetes this is a PersistentVolumeClaim that outlives
# the pod; with plain docker it is a named volume. Either way the container
# filesystem is ephemeral and the log must not live in it.
ENV PYKA_DATA_DIR=/var/lib/pyka
VOLUME ["/var/lib/pyka"]

EXPOSE 9092 8080

# Plain docker only — Kubernetes ignores this and uses its own probes, which
# are better (a gRPC health check, and readiness separate from liveness).
# urllib rather than curl: no extra package, and the slim image has no curl.
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD ["python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/readyz', timeout=2).status == 200 else 1)"]

# Exec form, deliberately. The shell form runs `/bin/sh -c ...`, which makes
# the shell PID 1 — and sh does not forward SIGTERM to its child. The broker
# would never run its shutdown sequence, and every stop would end in SIGKILL
# with a torn segment tail to recover. This is the whole reason we can drain
# in 0 seconds instead of waiting out the grace period.
CMD ["pyka-broker"]
