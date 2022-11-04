# SPDX-License-Identifier: MIT
# SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
#
# Container for running the Brandy indexer in production.

FROM rust:1.65.0-alpine3.16 AS builder
RUN apk add --no-cache musl-dev
COPY . /build
WORKDIR /build
RUN cargo fetch --locked
RUN cargo build --release --locked
RUN cargo test --release --locked

FROM alpine:3.16
COPY --from=builder /build/target/release/brandy-indexer \
    /usr/local/bin/brandy-indexer
