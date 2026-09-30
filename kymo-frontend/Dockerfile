FROM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

# Install wasm target + dx CLI. Build dioxus-cli from source rather than
# cargo-binstall: the prebuilt 0.7.9 binary links GLIBC_2.39, newer than the
# pinned bookworm base (glibc 2.36), so the downloaded dx fails to run.
RUN rustup target add wasm32-unknown-unknown
RUN cargo install dioxus-cli@0.7.9 --locked

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
