FROM alpine:edge

ADD ./dumb-init /dumb-init
ADD ./historian /historian
RUN chmod +x /historian /dumb-init

ENTRYPOINT ["/dumb-init", "/historian"]
