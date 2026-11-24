# telelem Mix runner image (TEL-001).
#
# Pinned base: official Docker Library image elixir:1.20.5-otp-28 (Debian 13
# trixie), resolved by multi-arch index digest. No apt-get here: every OS
# package (OpenSSL, ca-certificates, git, gcc, ...) comes from that digest, so
# a rebuild yields the same toolchain.
#
# Build (needs network once, for the Hex archive):  scripts/setup
FROM elixir:1.20.5-otp-28@sha256:9deb9fe8bf7c8cfa02bd3b7b6b1932fbb9c313151eb1e76eba112fdde7628337

ARG HEX_VERSION=2.7.0

# MIX_HOME holds the Hex archive and is read-only at run time. Per-checkout
# writable state (HOME, HEX_HOME) is set by scripts/mix to paths under /work.
ENV LANG=C.UTF-8 \
    MIX_HOME=/opt/mix \
    MIX_REBAR3=/usr/local/bin/rebar3 \
    HEX_HOME=/opt/hex

RUN mix local.hex "${HEX_VERSION}" --force \
 && mix hex.info \
 && rm -rf /opt/hex \
 && chmod -R a+rX /opt/mix

LABEL org.opencontainers.image.title="telelem-mix" \
      telelem.elixir="1.20.5" \
      telelem.otp="28.5.0.8" \
      telelem.hex="${HEX_VERSION}"

WORKDIR /work
