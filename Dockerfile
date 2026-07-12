# trade-core matching node — multi-stage build, zero crate dependencies so the
# build is just this repo's compilation.
FROM rust:1-slim AS build
WORKDIR /src
COPY Cargo.toml ./
COPY src ./src
COPY assets ./assets
COPY benches ./benches
COPY examples ./examples
RUN cargo build --release \
    --bin trade-core --bin order --bin market-data --bin order_client --bin order_load

FROM debian:stable-slim
COPY --from=build /src/target/release/trade-core /usr/local/bin/trade-core
COPY --from=build /src/target/release/order /usr/local/bin/order
COPY --from=build /src/target/release/market-data /usr/local/bin/market-data
COPY --from=build /src/target/release/order_client /usr/local/bin/order_client
COPY --from=build /src/target/release/order_load /usr/local/bin/order_load

# Journal + snapshots live here; mount a volume to survive container restarts
# (the server recovers state from snapshot + journal on startup).
VOLUME /data/journal
EXPOSE 9001 9101 8080

# Positional args: ADDR SHARDS STRATEGY JOURNAL_DIR POOL_MB BAND_BPS MD_ADDR
ENV ADDR=0.0.0.0:9001 \
    SHARDS=4 \
    STRATEGY=price-time \
    JOURNAL_DIR=/data/journal \
    POOL_MB=3072 \
    BAND_BPS=1000 \
    MD_ADDR=0.0.0.0:9101

# Default runs the matching node (trade-core); compose overrides `command`
# per service to run market-data / order from the same image.
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["exec trade-core \"$ADDR\" \"$SHARDS\" \"$STRATEGY\" \"$JOURNAL_DIR\" \"$POOL_MB\" \"$BAND_BPS\" \"$MD_ADDR\""]
