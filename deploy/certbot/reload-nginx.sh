#!/bin/sh
set -eu

if ! validation_output=$(/usr/sbin/nginx -t 2>&1); then
    printf '%s\n' "$validation_output" >&2
    exit 1
fi
/usr/bin/systemctl reload nginx.service
