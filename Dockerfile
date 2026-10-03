# =============================================================
# StackUp Engineering Academy — Data Engineering Assessment
# Pillar 4: Infrastructure & Governance — Task 4.1: Containerisation
# Multi-Stage Dockerfile for High Performance Presight ETL
# Candidate: Akhilesh
# File: Dockerfile
# =============================================================

# ── Stage 1: Build & Dependency Resolution ─────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Pinned minimal requirements with extended network timeout & retries for resilient downloads
RUN pip install --no-cache-dir --user \
    --default-timeout=1000 \
    --retries 10 \
    --prefer-binary \
    pandas==2.2.2 \
    numpy==1.26.4 \
    duckdb==0.10.2

# ── Stage 2: Minimal Production Runtime ────────────────────────
FROM python:3.11-slim AS runtime

WORKDIR /app

# Copy installed Python packages from builder stage
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1

ENV DATA_DIR=/app/datasets
ENV OUTPUT_DIR=/app/outputs

# Copy source code and datasets
COPY datasets /app/datasets
COPY starter_files /app/starter_files
COPY solutions /app/solutions

# Ensure files inside submissions are accessible directly under /app/solutions
RUN if [ -d "/app/solutions/submissions" ]; then cp -rn /app/solutions/submissions/*/* /app/solutions/ 2>/dev/null || true; fi

RUN mkdir -p /app/outputs && chmod 777 /app/outputs

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import pandas, numpy, duckdb; print('Health OK')" || exit 1

ENTRYPOINT ["python", "starter_files/etl_starter.py"]
