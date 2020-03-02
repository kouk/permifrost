ARG BASE_IMAGE=meltano/meltano-permissions/base
FROM $BASE_IMAGE as builder

WORKDIR /meltano_permissions

# building the code
COPY . .

# this enables the build process to have access to local
# `meltano` module
ENV PYTHONPATH=src/
RUN make sdist

FROM $BASE_IMAGE as runtime
# grab the built package
COPY --from=builder /meltano_permissions/dist /meltano_permissions/dist

# install the built artifact to make sure we are
# installing the application the same way our user
# do when using PyPI
RUN pip install /meltano_permissions/dist/meltano_permissions-*.tar.gz

# meltano project directory, this is where you should
# mount your Meltano project
WORKDIR /project

ENTRYPOINT ["meltano_permissions"]
