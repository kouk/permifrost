#########################################################
##################### Globals ###########################
#########################################################

.DEFAULT_GOAL := dev-help
COMMIT_HASH = $(shell git log -1 --pretty=%H)

dev-help:
	@echo "base-image -> builds gitlab-data/base"
	@echo "docker-clean -> deletes all the build artifacts"
	@echo "docker-images -> builds the base and prod images"
	@echo "permifrost -> starts a shell in a container with the local Permifrost installed"
	@echo "requirements.txt -> pins dependency versions in `requirements.txt`"
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
.PHONY: base_image prod_image lint show_lint test clean docker_images release

ifdef DOCKER_REGISTRY
base_image_tag = ${DOCKER_REGISTRY}/gitlab-data/permifrost/base
prod_image_tag = ${DOCKER_REGISTRY}/gitlab-data/permifrost
else
base_image_tag = gitlab-data/permifrost/base
prod_image_tag = gitlab-data/permifrost
endif

# Testing
test: compose-build
	@docker-compose run permifrost /bin/bash \
		-c "pip install -e . && pytest -v --disable-pytest-warnings"

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

prod-image: base_image
	@docker build \
		--file docker/prod/Dockerfile \
		-t $(prod_image_tag) \
		--build-arg BASE_IMAGE=$(base_image_tag) \
		.


# Linting
BLACK_RUN = black src/permifrost tests/

lint: compose-build
	@docker-compose run permifrost /bin/bash -c "make local-lint"

show-lint: compose-build
	@docker-compose run permifrost /bin/bash -c "make local-show-lint"

local-lint:
	${BLACK_RUN}

local-show-lint:
	${BLACK_RUN} --check --diff

#########################################################
#################### Deployment #########################
#########################################################

# Packaging Related
requirements.txt: setup.py
	pip freeze --exclude-editable > $@

# Release
ifdef type
  override type := --$(type)
endif

release:
	git diff --quiet || { echo "Working directory is dirty, please commit or stash your changes."; exit 1; }
	yes | changelog release $(type)
	git add CHANGELOG.md
	bumpversion --tag --allow-dirty --new-version `changelog current` minor
