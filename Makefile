# Some simple testing tasks (sorry, UNIX only).

FLAGS=
SCALA_VERSION=2.11
KAFKA_VERSION=0.8.2.1
DOCKER_IMAGE_NAME=aiokafka/tests:$(SCALA_VERSION)_$(KAFKA_VERSION)

flake:
	flake8 aiokafka tests

# test: flake .docker-test
test: .docker-test

vtest: flake
	nosetests -s -v $(FLAGS) ./tests/

cov cover coverage: .docker-test-cov
	@echo "open file://`pwd`/cover/index.html"

clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*~' `
	rm -f `find . -type f -name '.*~' `
	rm -f `find . -type f -name '@*' `
	rm -f `find . -type f -name '#*#' `
	rm -f `find . -type f -name '*.orig' `
	rm -f `find . -type f -name '*.rej' `
	rm -f .coverage
	rm -rf coverage
	rm -rf build
	rm -rf cover
	rm -rf dist

doc:
	make -C docs html
	@echo "open file://`pwd`/docs/_build/html/index.html"

.docker-build:
	@echo "Building docker image with Scala $(SCALA_VERSION) and Kafka $(KAFKA_VERSION)"
	@docker build -qt $(DOCKER_IMAGE_NAME) --build-arg SCALA_VERSION=$(SCALA_VERSION) --build-arg KAFKA_VERSION=$(KAFKA_VERSION) ./tests/.docker

.docker-clean:
	@docker rmi -f $(DOCKER_IMAGE_NAME)

.docker-test: .docker-build
	@DOCKER_IMAGE_NAME=$(DOCKER_IMAGE_NAME)  FLAGS=$(FLAGS) sh runtests.sh

.docker-test-cov: .docker-build
	@DOCKER_IMAGE_NAME=$(DOCKER_IMAGE_NAME) EXTRA_NOSE_ARGS="--with-cover --cover-html --cover-branches --cover-package aiokafka" FLAGS=$(FLAGS) sh runtests.sh

.PHONY: all flake test vtest cov clean doc .docker-build .docker-clean .docker-test
