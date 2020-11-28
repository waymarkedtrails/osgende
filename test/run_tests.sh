#!/bin/bash

PYVERSION=`python3 --version 2>&1 | sed 's:Python ::;s:\.[0-9]\+$::'`
PYPLATFORM=`python3 -m sysconfig | grep Platform | sed 's/Platform: *"//;s:"::'`

export PYTHONPATH=..:../../pyosmium/build/lib.${PYPLATFORM}-${PYVERSION}
export TESTEXEC='pytest'

if [ "x$1" == "x-n" ]; then
    shift
    $TESTEXEC "$@"
elif [ "x$1" == "x-s" ]; then
    shift
    pg_virtualenv -s $TESTEXEC "$@"
else
    pg_virtualenv $TESTEXEC "$@"
fi
