# Build stage pinned to the toolchain in go.mod; the final image is distroless
# static because the binary is CGO-free and needs nothing else.
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/tenant-syncer ./cmd

# The CSI node plugin image (build with --target csi-node). Same binary in
# `csi-node` mode, but on Alpine rather than distroless: NodeStageVolume shells
# out to mkfs.ext4/fsck/blkid/mount, so the filesystem tools must exist. It
# runs privileged in the GUEST with hostPath /dev — and holds no credentials,
# which is the design's whole point.
FROM alpine:3.21 AS csi-node
RUN apk add --no-cache e2fsprogs e2fsprogs-extra util-linux blkid
COPY --from=build /out/tenant-syncer /tenant-syncer
ENTRYPOINT ["/tenant-syncer", "csi-node"]

# The controller image: kept as the FINAL stage so a target-less build keeps
# producing the same image it always has.
FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/tenant-syncer /tenant-syncer
USER 65532:65532
ENTRYPOINT ["/tenant-syncer"]
