ARG BASE_IMAGE=gitlab-data/permifrost/base
FROM $BASE_IMAGE as builder

WORKDIR /permifrost

# building the code
COPY . .

# this enables the build process to have access to local
# `permifrost` module
ENV PYTHONPATH=src/
RUN python setup.py sdist

FROM $BASE_IMAGE as runtime
# grab the built package
COPY --from=builder /permifrost/dist /permifrost/dist

# install the built artifact to make sure we are
# installing the application the same way our user
# do when using PyPI
RUN pip install /permifrost/dist/permifrost-*.tar.gz

# permifrost project directory, this is where you should
# mount your Permifrost project
WORKDIR /project

ENTRYPOINT ["permifrost"]
