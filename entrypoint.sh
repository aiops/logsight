#!/bin/sh
# Copyright 2021-2022 logsight.ai

set -e

check_license() {
  if ! (echo "$ACCEPT_LOGSIGHT_LICENSE" | grep -Eq  ^.*accept-license.*$); then
    echo "Logsight license not accepted. You need to accept the EULA when using this image."
    echo "Please set the environment variable ACCEPT_LOGSIGHT_LICENSE='accept-license'"
    echo "For example: docker run -e ACCEPT_LOGSIGHT_LICENSE=accept-license logsight/logsight"
    echo ""
    # TODO: Add print with link to license text
    exit 1
  fi
}

check_license
python3 -u "${LOGSIGHT_HOME}/run.py"
