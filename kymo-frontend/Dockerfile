# Only the web bundle leaves this builder; its distribution need not match the runtime image.
FROM rust:1-trixie AS builder

COPY security /tmp/kymo-security
RUN sh /tmp/kymo-security/refresh-trust.sh --rustup

RUN apt-get update && apt-get install -y protobuf-compiler patch && rm -rf /var/lib/apt/lists/*

RUN rustup target add wasm32-unknown-unknown
RUN sh /tmp/kymo-security/install-dx.sh /usr/local && rm -rf /tmp/kymo-security

WORKDIR /build
COPY . /build

ARG KYMO_FRONTEND_SERVER_ORIGIN
RUN : "${KYMO_FRONTEND_SERVER_ORIGIN:?build arg required: the server HTTP origin, e.g. http://kymo.example:8080}"
RUN cd kymo-frontend && dx bundle --package kymo-frontend --release --debug-symbols=false --cargo-args=--locked
RUN kymo-frontend/scripts/preload_wasm.sh /build/target/dx/kymo-frontend/release/web/public
# nginx.conf serves these with gzip_static; the originals stay for clients without gzip.
RUN find /build/target/dx/kymo-frontend/release/web/public/assets -type f \( -name '*.wasm' -o -name '*.js' -o -name '*.css' -o -name '*.ttf' \) -exec gzip -9 -k {} +

FROM nginx:alpine

COPY --from=builder /build/target/dx/kymo-frontend/release/web/public /usr/share/nginx/html
COPY kymo-frontend/nginx.conf /etc/nginx/conf.d/default.conf
# A directive this nginx lacks fails the image build instead of the pod.
RUN nginx -t

EXPOSE 80
