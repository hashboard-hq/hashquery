clean:
	rm -rf _build/

build: clean
	sphinx-build -M dirhtml . _build $(O)

dev: clean
	sphinx-autobuild . _build/html --port 8002 $(O)

