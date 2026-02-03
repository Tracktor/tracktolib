FROM alpine:3.20

RUN apk add --no-cache ca-certificates curl gawk

COPY --from=dxflrs/garage:v2.2.0 /garage /usr/local/bin/garage

COPY garage-init.sh /init.sh
ENTRYPOINT ["/bin/sh", "/init.sh"]