# Dockerfile
ARG TARGET=x86_64-unknown-linux-musl

FROM clux/muslrust:stable AS build
ARG TARGET
# Add certificate and uncomment if building behind proxy with custom cert
# COPY ./gitignore/ca-certificates.crt /usr/local/share/ca-certificates/ca.crt
# RUN update-ca-certificates
COPY . /project
WORKDIR /project

# Install the target if not x86_64 (default)
RUN if [ "$TARGET" != "x86_64-unknown-linux-musl" ]; then \
    rustup target add $TARGET; \
    fi

RUN --mount=type=cache,id=cargo-git,target=/home/rust/.cargo/git \
    --mount=type=cache,id=cargo-registry,target=/home/rust/.cargo/registry \
    --mount=type=cache,id=rust-target,target=/home/rust/src/target \
    cargo build -p leaf-server --release --target $TARGET

FROM alpine:3
ARG TARGET
COPY --from=build /project/target/${TARGET}/release/leaf /leaf
CMD ["server"]
ENTRYPOINT ["/leaf"]
EXPOSE 5530