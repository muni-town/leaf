# Podman In Podman

This has a modified compose YAML that will work when running using podman as root
inside of a rootless podman container.

There are a lot of tricky things to making that work so things like port bindings
and networks don't work normally, so this puts the services in host networking
mode and adjusts their connection strings compared to the normal compose file.