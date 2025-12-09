# -----------------------------------------------------------------------------
# Stages
# -----------------------------------------------------------------------------

ARG IMAGE_BUILDER=golang:1.25.5-bookworm@sha256:cbd59ce363d162d31192b1bcf928773b6f8490ffd529c51594fc4d4ba755b8a5
ARG IMAGE_FINAL=senzing/senzingsdk-runtime:4.1.0@sha256:e57d751dc0148bb8eeafedb7accf988413f50b54a7e46f25dfe4559d240063e5

# -----------------------------------------------------------------------------
# Stage: senzingsdk_runtime
# -----------------------------------------------------------------------------

FROM ${IMAGE_FINAL} AS senzingsdk_runtime

# -----------------------------------------------------------------------------
# Stage: builder
# -----------------------------------------------------------------------------

FROM ${IMAGE_BUILDER} AS builder
ENV REFRESHED_AT=2024-07-01
LABEL Name="senzing/go-builder" \
      Maintainer="support@senzing.com" \
      Version="0.1.0"

# Run as "root" for system installation.

USER root

# Copy local files from the Git repository.

COPY ./rootfs /
COPY . ${GOPATH}/src/move

# Copy files from prior stage.

COPY --from=senzingsdk_runtime  "/opt/senzing/er/lib/"   "/opt/senzing/er/lib/"
COPY --from=senzingsdk_runtime  "/opt/senzing/er/sdk/c/" "/opt/senzing/er/sdk/c/"

# Set path to Senzing libs.

ENV LD_LIBRARY_PATH=/opt/senzing/er/lib/

# Build go program.

WORKDIR ${GOPATH}/src/move
RUN make build

# Copy binaries to /output.

RUN mkdir -p /output \
 && cp -R ${GOPATH}/src/move/target/*  /output/

# -----------------------------------------------------------------------------
# Stage: final
# -----------------------------------------------------------------------------

FROM ${IMAGE_FINAL} AS final
ENV REFRESHED_AT=2024-07-01
LABEL Name="senzing/move" \
      Maintainer="support@senzing.com" \
      Version="0.1.0"
HEALTHCHECK CMD ["/app/healthcheck.sh"]
USER root

# Install packages via apt-get.

# Copy files from repository.

COPY ./rootfs /

# Copy files from prior stage.

COPY --from=builder /output/linux/move /app/move

# Run as non-root container

USER 1001

# Runtime environment variables.

ENV LD_LIBRARY_PATH=/opt/senzing/er/lib/

# Runtime execution.

WORKDIR /app
ENTRYPOINT ["/app/move"]
