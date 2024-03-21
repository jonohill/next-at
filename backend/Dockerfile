FROM alpine:3.19

ARG TARGETPLATFORM

RUN addgroup -S app && adduser -S app -G app
RUN mkdir -p /data && chown -R app:app /data

COPY ./target/${TARGETPLATFORM}/release/next-at-rs /next-at-rs
RUN chmod +x /next-at-rs

USER app

VOLUME [ "/data" ]

ENV PORT="8080"
ENV LISTEN_ADDRESS="0.0.0.0:${PORT}"
ENV DATABASE_PATH="/data/data.db"

ENTRYPOINT [ "/next-at-rs" ]
