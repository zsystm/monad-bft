Monad Package Creation Support
==============================

The top-level `Test` script builds this.

`Dockerfile` is substantially similar to various `docker/*/Dockerfile`
files (all of which duplicate quite a bit of code), and builds things in
much the same way at the moment. Eventually we should probably be building
everything through this `Dockerfile` and extracting just the `.deb` package
from it for further use in other images, as the final release package, etc.

The other files are package metadata templates and support tools for doing
the package build.
