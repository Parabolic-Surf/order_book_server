# syntax=docker/dockerfile:1

# Build on an older glibc than the runtime image so the binary also runs when
# it is copied out of the image onto an Ubuntu 24.04 host.
FROM rust:1.98-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock rustfmt.toml ./
COPY server ./server
COPY bin ./bin
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin orderbook_server \
    && cp target/release/orderbook_server /orderbook_server

# Ubuntu 24.04 matches the node host: in direct snapshot mode the server runs
# a copy of the node's own binary, which is built for that release.
#
# The image holds only the server. Run it with --snapshot-mode direct, and mount
# the node's data directory and a copy of the node binary (--hlnode-binary).
# The default docker snapshot mode cannot work here: there is no docker client.
FROM ubuntu:24.04
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /orderbook_server /usr/local/bin/orderbook_server
# The node user on our hosts; it owns the node data the server reads.
USER 2001:2001
EXPOSE 8000 9090
ENTRYPOINT ["/usr/local/bin/orderbook_server"]
