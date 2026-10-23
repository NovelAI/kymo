# Pin to bookworm so the binary links against glibc 2.36, matching the
# debian:bookworm-slim runtime below. rust:latest tracks Debian trixie
# (glibc 2.38); some deps (metrics-exporter-prometheus) reference 2.38-versioned
# symbols, which then fail to load on the bookworm runtime with
# "GLIBC_2.38 not found". Keep builder + runtime on the same Debian release.
FROM rust:1-bookworm AS builder

COPY security/refresh-trust.sh security/verify-ca-bundle.sh /tmp/kymo-security/
RUN sh /tmp/kymo-security/refresh-trust.sh --rustup && rm -rf /tmp/kymo-security

WORKDIR /build
COPY . /build

RUN cargo build --locked --release --package kymo-server

FROM debian:bookworm-slim

COPY security/refresh-trust.sh security/verify-ca-bundle.sh /tmp/kymo-security/
RUN sh /tmp/kymo-security/refresh-trust.sh && rm -rf /tmp/kymo-security

COPY --from=builder /build/target/release/kymo-server /usr/local/bin/kymo-server

ENTRYPOINT ["kymo-server"]
