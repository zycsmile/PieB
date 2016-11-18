#! /bin/bash
#
# build.sh
# Copyright (C) 2015 work <work@yf-l2-bfetest05.yf01.baidu.com>
#
# Distributed under terms of the MIT license.
#


export GOPATH=/home/work/zhangyuchao02/golang-lib:$GOPATH
WORKSPACE=$(pwd)
export GOPATH=$WORKSPACE:$GOPATH
cd src/main && go build
if [ $? -ne 0 ];
then
    echo "build failed"
    exit 1
fi
cp main ../../output/server
cd client && go build
if [ $? -ne 0 ];
then
    echo "build client failed"
	exit 1
fi
cp client ../../../output/client
cd ..
