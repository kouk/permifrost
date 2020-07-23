# General
# =======
#
# - `make test` runs pytest
# - `make clean` deletes all the build artifacts
# - `make docker_images` builds all the docker images including the production
#   image

ifdef DOCKER_REGISTRY
base_image_tag = ${DOCKER_REGISTRY}/gitlab-data/permifrost/base
prod_image_tag = ${DOCKER_REGISTRY}/gitlab-data/permifrost
else
base_image_tag = gitlab-data/permifrost/base
prod_image_tag = gitlab-data/permifrost
endif

DOCKER_RUN=docker run -it --rm -v $(shell pwd):/app -w /app
PYTHON_RUN=${DOCKER_RUN} --name python-$(shell uuidgen) python

.PHONY: test clean docker_images release

test:
	docker run -it --rm -v $(shell pwd):/project --entrypoint pytest gitlab-data/permifrost:latest -v

# pip related
TO_CLEAN  = ./build ./dist

docker_clean:
	docker run --rm -v `pwd`:/permifrost -w /permifrost ${base_image_tag} \
	bash -c "make clean"

clean:
	rm -rf ${TO_CLEAN}

docker_images: base_image prod_image

# Docker Image Related
# ====================
#
# - `make base_image` builds gitlab-data/base
# - `make prod_image` builds gitlab-data/permifrost which is an all-in-one production
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
	docker run --rm -v `pwd`:/permifrost -w /permifrost ${base_image_tag} \
	bash -c "make sdist && chmod 777 dist/*"


# Lint Related Tasks
# ==================
#

.PHONY: lint show_lint

BLACK_RUN = black src/permifrost tests/

lint_black:
	${BLACK_RUN}

show_lint_black:
	${BLACK_RUN} --check --diff

lint: lint_black

docker_lint:
	docker run --rm -v `pwd`:/permifrost -w /permifrost ${base_image_tag} \
	bash -c "make lint"

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
