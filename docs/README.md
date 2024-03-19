# Developing

Run `make dev` to run a dev server with the contents of the docs page.

Note that the Sphinx builder sometimes gets out of sync and you'll need
to kill the process and start again. This most commonly occurs if the main
TOCTree changes significantly, you want to refresh the linked docstrings,
or you update anything in `_static`.

# Building

Run `make build` to build the site statically to `_build/html`. This site
should be statically servable by any simple HTTP server.
