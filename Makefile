# General
# =======
#
# - `make test` runs pytest
# - `make clean` deletes all the build artifacts
# - `make docker_images` builds all the docker images including the production
#   image

ifdef DOCKER_REGISTRY
base_image_tag = ${DOCKER_REGISTRY}/meltano/meltano-permissions/base
prod_image_tag = ${DOCKER_REGISTRY}/meltano/meltano-permissions
else
base_image_tag = meltano/meltano-permissions/base
prod_image_tag = meltano/meltano-permissions
endif

DOCKER_RUN=docker run -it --rm -v $(shell pwd):/app -w /app
PYTHON_RUN=${DOCKER_RUN} --name python-$(shell uuidgen) python
DCR=docker-compose run --rm
DCRN=${DCR} --no-deps

.PHONY: test clean docker_images release

test:
	${DCRN} ./setup.py test

# pip related
TO_CLEAN  = ./build ./dist

clean:
	rm -rf ${TO_CLEAN}

docker_images: base_image prod_image

# Docker Image Related
# ====================
#
# - `make base_image` builds meltano/base
# - `make prod_image` builds meltano/meltano-permissions which is an all-in-one production
#   image that includes the static ui artifacts in the image.

.PHONY: base_image prod_image

base_image:
	docker build \
		--file docker/base/Dockerfile \
		-t $(base_image_tag) \
		.

prod_image: base_image
	docker build \
		--file docker/prod/Dockerfile \
		-t $(prod_image_tag) \
		--build-arg BASE_IMAGE=$(base_image_tag) \
		.

# Packaging Related
# ===========
#
# - `make requirements.txt` pins dependency versions. We use requirements.txt
#   as a lockfile essentially.

requirements.txt: setup.py
	pip freeze --exclude-editable > $@

sdist:
	python setup.py sdist

docker_sdist: base_image
	docker run --rm -v `pwd`:/meltano_permissions ${base_image_tag} \
	bash -c "make sdist" && \
	bash -c "chmod 777 dist/*"

# Lint Related Tasks
# ==================
#

.PHONY: lint show_lint

BLACK_RUN = black src/meltano_permissions tests/

lint_black:
	${BLACK_RUN}

show_lint_black:
	${BLACK_RUN} --check --diff

lint: lint_black
show_lint: show_lint_black

# Makefile Related Tasks
# ======================
#
# - `make explain_makefile` will bring up a web server with this makefile annotated.
explain_makefile:
	docker stop explain_makefile || echo 'booting server'
	${DOCKER_RUN} --name explain_makefile -p 8081:8081 node ./Makefile_explain.sh

# Release
# =====================
ifdef type
  override type := --$(type)
endif

release:
	git diff --quiet || { echo "Working directory is dirty, please commit or stash your changes."; exit 1; }
	yes | changelog release $(type)
	git add CHANGELOG.md
	bumpversion --tag --allow-dirty --new-version `changelog current` minor
