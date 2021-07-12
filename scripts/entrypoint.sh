#! /usr/bin/env bash

set -eu

if [ -z ${GITHUB_ACTION+x} ]
then
  echo '### `crystal tool format --check`'
  crystal tool format --check

  echo '### `ameba`'
  crystal lib/ameba/bin/ameba.cr
fi

watch="false"
while [[ $# -gt 0 ]]
do
  arg="$1"
  case $arg in
    -w|--watch)
    watch="true"
    shift
    ;;
  esac
done

echo "### \`crystal spec\`"

if [[ "$watch" == "true" ]]; then
  CRYSTAL_WORKERS=$(nproc) watchexec -e cr -c -r -w src -w spec -- crystal spec --error-trace -v
else
  CRYSTAL_WORKERS=$(nproc) crystal spec --error-trace -v
fi
