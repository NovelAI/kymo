# Trixie because the prebuilt dx needs glibc 2.39; only the web bundle leaves this stage, so it need not match any runtime image.
FROM rust:1-trixie AS builder

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

RUN rustup target add wasm32-unknown-unknown
# Same release as the `dioxus-cli@0.7.9` pins in the CI workflows; bump them together with this URL and both checksums.
RUN arch="$(dpkg --print-architecture)" && case "$arch" in \
      amd64) target=x86_64-unknown-linux-gnu sha=3b132551b480bc96f938f9f0d37936ee1190f994977539dcc347eaf38540d005 ;; \
      arm64) target=aarch64-unknown-linux-gnu sha=8cf14db0b11b43b31dd6d39e71b00e567f2fccfde85ae3a8f7ef0f8745e5ccfb ;; \
      *) echo "no prebuilt dx for $arch" >&2; exit 1 ;; \
    esac \
    && curl -fsSL -o /tmp/dx.tar.gz "https://github.com/DioxusLabs/dioxus/releases/download/v0.7.9/dx-$target.tar.gz" \
    && echo "$sha  /tmp/dx.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/dx.tar.gz -C /usr/local/cargo/bin dx

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
