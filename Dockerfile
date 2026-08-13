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

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/tenant-syncer /tenant-syncer
USER 65532:65532
ENTRYPOINT ["/tenant-syncer"]
