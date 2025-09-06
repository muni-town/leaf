FROM clux/muslrust:stable AS build

# Add certificate and uncomment if building behind proxy with custom cert
# COPY ./gitignore/ca-certificates.crt /usr/local/share/ca-certificates/ca.crt
# RUN update-ca-certificates

COPY . /project
WORKDIR /project
RUN --mount=type=cache,target=/home/rust/.cargo/git \
    --mount=type=cache,target=/home/rust/.cargo/registry \
    --mount=type=cache,target=/home/rust/src/target \
    cargo b -p leaf-server --release --target x86_64-unknown-linux-musl

FROM scratch
COPY --from=build /project/target/x86_64-unknown-linux-musl/release/leaf /leaf
CMD ["server"]
ENTRYPOINT "/leaf"