#!/bin/bash
SCRIPT_DIR="$( dirname "${BASH_SOURCE[0]}" )"
cd "$SCRIPT_DIR" &> /dev/null
MY_DIR="$(pwd)"
echo "[INFO] Executing in ${MY_DIR}"

IMPORT_UPDATES="Y"
import_content=(${MY_DIR}/import/content/*)
if [[ ${#import_content[@]} -gt 0 ]]; then
  echo "[INFO] Existing imports found in ${MY_DIR}/import/content"
  for file in ${import_content[@]}; do
    echo " >>  $(basename $file)"
  done
  echo ""
  while true; do
    read -p "Do you wish to update imports (yes/no)? " yn
    case $yn in
      [Yy]* ) IMPORT_UPDATES="Y"; break;;
      [Nn]* ) IMPORT_UPDATES="N"; break;;
      * ) echo "[ERROR] Please answer yes or no.";;
    esac
  done
fi

if [[ "x$IMPORT_UPDATES" == "xY" ]]; then
  xmake -ci --debug-xmake --buildruntime linuxx86_64 \
    -I DOCKER=docker.wdf.sap.corp:50002
else
  xmake -c --debug-xmake --buildruntime linuxx86_64 \
    -I DOCKER=docker.wdf.sap.corp:50002
fi
