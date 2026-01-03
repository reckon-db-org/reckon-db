# erl-esdb Docker image
# Multi-stage build for optimized production image

# Build stage
FROM erlang:27-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy rebar config first for layer caching
COPY rebar.config rebar.lock ./

# Install rebar3
RUN curl -o /usr/local/bin/rebar3 https://s3.amazonaws.com/rebar3/rebar3 && \
    chmod +x /usr/local/bin/rebar3

# Fetch dependencies
RUN rebar3 get-deps

# Copy source code
COPY src/ src/
COPY include/ include/
COPY config/ config/
COPY priv/ priv/

# Build release with production profile (includes ERTS)
RUN rebar3 as prod release

# Runtime stage
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libncurses6 \
    openssl \
    && rm -rf /var/lib/apt/lists/*

# Create app user
RUN useradd --create-home --shell /bin/bash app

# Set working directory
WORKDIR /app

# Copy release from builder (includes ERTS)
COPY --from=builder /app/_build/prod/rel/erl_esdb .

# Create data directory
RUN mkdir -p /app/data && chown -R app:app /app

# Switch to non-root user
USER app

# Expose ports
# Cluster/Discovery
EXPOSE 45892/udp
# Raft consensus
EXPOSE 4369
EXPOSE 9100-9200

# Environment variables
ENV ERL_ESDB_NODE_NAME=store@localhost
ENV ERL_ESDB_CLUSTER_NODES=
ENV ERL_ESDB_DATA_DIR=/app/data

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD /app/bin/erl_esdb ping || exit 1

# Start command
CMD ["/app/bin/erl_esdb", "foreground"]
