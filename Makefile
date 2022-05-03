#########################################################
##################### Globals ###########################
#########################################################

.DEFAULT_GOAL := help
COMMIT_HASH = $(shell git log -1 --pretty=%H)
DEFAULT_ORG ?= ${GITLAB_ORGANISATION}

help:
	@echo "base-image -> builds gitlab-data/base"
	@echo "docker-clean -> deletes all the build artifacts"
	@echo "docker-images -> builds the base and prod images"
	@echo "install-dev -> installs local package with dev dependencies"
	@echo "initial-setup -> installs local package with dev dependencies and pre-commit hooks"
	@echo "permifrost -> starts a shell in a container with the local Permifrost installed"
	@echo 'requirements.txt -> pins dependency versions in `requirements.txt`'
	@echo "prod-image -> builds gitlab-data/permifrost which is an all-in-one production image"
	@echo "test -> runs pytest"
	@echo "local-lint -> runs local linting suite to refactor code"
	@echo "local-show-lint -> shows linting suite results"
	@echo "show-coverage -> runs pytest coverage report inside docker instance when permifrost local initiated"

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
FLAKE8_RUN = flake8 src/ tests/
ISORT_RUN = isort src/

lint: compose-build
	@docker-compose run permifrost /bin/bash -c "make local-lint"

show-lint: compose-build
	@docker-compose run permifrost /bin/bash -c "make local-show-lint"

local-lint:
	pre-commit run --hook-stage push
	${BLACK_RUN}
	${ISORT_RUN}
	${MYPY_RUN}
	${FLAKE8_RUN}

local-show-lint:
	pre-commit run --hook-stage push
	${BLACK_RUN} --check --diff
	${ISORT_RUN} --check
	${MYPY_RUN} --show-error-context --show-column-numbers --pretty
	${FLAKE8_RUN}

ci-show-lint:
	pre-commit run --hook-stage merge-commit
	${BLACK_RUN} --check --diff
	${ISORT_RUN} --check
	${MYPY_RUN} --show-error-context --show-column-numbers --pretty
	${FLAKE8_RUN}

ci-lint:
	pre-commit run --hook-stage merge-commit
	${BLACK_RUN}
	${ISORT_RUN}
	${MYPY_RUN}
	${FLAKE8_RUN}

show-coverage:
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

initial-setup:
	pip install -e '.[dev]'
	pre-commit install -f -t pre-commit
	pre-commit install -f -t pre-push

# Release

# BEFORE running `make release` create a Releasing Update issue on GitLab
# and follow the instructions to determine what type of semantic release to perform

suggest:
	@(echo "Permifrost version " && changelog current && echo " should be updated to: " \
		&& changelog suggest) | tr -d "\n" && echo "\n"

# The `make release` command requires a type of release.
# (i.e. `make release type=<patch|minor|major>`)
# Which adheres to semantic versioning standards
ifdef type
  type_flag := --$(type)
endif

release:
	git diff --quiet || { echo "Working directory is dirty, please commit or stash your changes."; exit 1; }
	yes | changelog release $(type_flag)
	git add CHANGELOG.md
	bumpversion --tag --allow-dirty --new-version `changelog current` $(type)

dist: compose-build
	@docker-compose run permifrost /bin/bash \
		-c "python setup.py sdist"

docker-clean:
	@docker-compose run permifrost /bin/bash \
		-c "rm -rf dist"
