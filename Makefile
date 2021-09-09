#########################################################
##################### Globals ###########################
#########################################################

.DEFAULT_GOAL := dev-help
COMMIT_HASH = $(shell git log -1 --pretty=%H)
DEFAULT_ORG ?= hightouchio

dev-help:
	@echo "base-image -> builds gitlab-data/base"
	@echo "docker-clean -> deletes all the build artifacts"
	@echo "docker-images -> builds the base and prod images"
	@echo "install-dev -> installs local package with dev dependencies"
	@echo "permifrost -> starts a shell in a container with the local Permifrost installed"
	@echo 'requirements.txt -> pins dependency versions in `requirements.txt`'
	@echo "prod-image -> builds gitlab-data/permifrost which is an all-in-one production image"
	@echo "test -> runs pytest"

#########################################################
################### Development #########################
#########################################################
.PHONY: compose-down permifrost

compose-build:
	@docker-compose build

compose-down:
	@docker-compose down

permifrost: compose-down compose-build
	@docker-compose run permifrost /bin/bash -c "pip install -e . && clear && /bin/bash"

#########################################################
#################### CI Tests ###########################
#########################################################
.PHONY: base-image prod-image lint show_lint test clean docker-images release

ifdef DOCKER_REGISTRY
base_image_tag = ${DOCKER_REGISTRY}/${DEFAULT_ORG}/permifrost/base
prod_image_tag = ${DOCKER_REGISTRY}/${DEFAULT_ORG}/permifrost
else
base_image_tag = ${DEFAULT_ORG}/permifrost/base
prod_image_tag = ${DEFAULT_ORG}/permifrost
endif

# Testing
test: compose-down compose-build
	@docker-compose run permifrost /bin/bash \
		-c "pip install -e . && pytest -x -v --disable-pytest-warnings"

typecheck: compose-build
	@docker-compose run permifrost /bin/bash \
		-c "pip install -e . && mypy src/permifrost/ --ignore-missing-imports"

# Docker
docker-images: prod-image

base-image:
	@docker build \
		--file docker/base/Dockerfile \
		-t $(base_image_tag) \
		.

prod-image: base-image
	@docker build \
		--file docker/prod/Dockerfile \
		-t $(prod_image_tag) \
		--build-arg BASE_IMAGE=$(base_image_tag) \
		.


# Linting
BLACK_RUN = black src/permifrost tests/
MYPY_RUN = mypy src
FLAKE8_RUN = flake8
ISORT_RUN = isort src/

lint: compose-build
	@docker-compose run permifrost /bin/bash -c "make local-lint"

show-lint: compose-build
	@docker-compose run permifrost /bin/bash -c "make local-show-lint"

local-lint:
	${BLACK_RUN}
	${ISORT_RUN}
	${MYPY_RUN}
	${FLAKE8_RUN}

local-show-lint:
	${BLACK_RUN} --check --diff
	${ISORT_RUN} --check
	${MYPY_RUN} --show-error-context --show-column-numbers --pretty
	${FLAKE8_RUN}

local-show-coverage:
	pytest --disable-pytest-warnings --cov-report term-missing --cov permifrost

#########################################################
#################### Deployment #########################
#########################################################

# Packaging Related
requirements.txt: setup.py
	@docker-compose run permifrost /bin/bash \
		-c "pip install -e .'[dev]' && pip freeze --exclude-editable > $@"

install-dev:
	pip install -e '.[dev]'

# Release
ifdef type
  override type := --$(type)
endif

release:
	git diff --quiet || { echo "Working directory is dirty, please commit or stash your changes."; exit 1; }
	yes | changelog release $(type)
	git add CHANGELOG.md
	bumpversion --tag --allow-dirty --new-version `changelog current` minor

dist: compose-build
	@docker-compose run permifrost /bin/bash \
		-c "python setup.py sdist"

docker-clean:
	@docker-compose run permifrost /bin/bash \
		-c "rm -rf dist"
