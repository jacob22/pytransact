APT_GET=sudo apt-get -o Dpkg::Use-Pty=no -q -y

DEPENDENCIES = \
	python3-bson \
	python3-bson-ext- \
	python3-future \
	python3-pymongo \
	python3-pymongo-ext- \
	python3-pytest \
	python3-stdeb

dev-deps:
	$(APT_GET) install $(DEPENDENCIES)
