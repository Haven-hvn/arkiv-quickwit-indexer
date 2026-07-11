FROM rust:bookworm AS builder

WORKDIR /bridge
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get -y update \
    && apt-get -y install ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /bridge/target/release/arkiv-quickwit-bridge /usr/local/bin/arkiv-quickwit-bridge

# State store lives on a volume mounted here.
RUN mkdir -p /var/lib/arkiv-quickwit-bridge

ENTRYPOINT ["arkiv-quickwit-bridge"]
CMD ["--config", "/etc/arkiv-quickwit-bridge/config.yaml"]
