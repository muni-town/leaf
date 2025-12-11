# Leaf Framework

This is the third major iteration of Muni Town's Leaf Framework. Under heavy development.

The Leaf server and client libraries are currently in a functional proof-of-concept state.

Meri's [notes from first reading the code](https://leaflet.pub/3abc7a5c-0790-4a4b-8ca1-a0a988bd7def) might be a little bit useful in lieu of more fleshed out documentation

To compile and run: `cargo r -- --otel  server -D did:web:localhost`

## Dockerfile build

The Dockerfile for `leaf-server` uses statically linked C code that is compiled for x86 architectures, which is also what the server is running. On x86 machines we can run `docker build -t leaf-server:x86_64 .`

For local dev on arm64 machines, we can pass in a build arg: `docker build --build-arg TARGET=aarch64-unknown-linux-musl -t leaf-server:arm64 .`

Then in either case we can run it as usual: `docker run -it --rm -p 5530:5530 -v $(pwd)/data:/data leaf-server`