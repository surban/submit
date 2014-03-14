#!/bin/bash

if [ "$device" == "cpu" ] ; then
    if ( [ "$CPU_PRECISION" == "" ] || [ "$CPU_PRECISION" == "64" ] ) ; then
        export GNUMPY_CPU_PRECISION=64
        export GNUMPY_USE_GPU=no
        export THEANO_FLAGS=floatX=float64,device=cpu
    elif [ "$CPU_PRECISION" == "32" ] ; then
        export GNUMPY_CPU_PRECISION=32
        export GNUMPY_USE_GPU=no
        export THEANO_FLAGS=floatX=float32,device=cpu
    else
        echo "Unrecognized CPU_PRECISION: $CPU_PRECISION"
        exit 1
    fi
elif [ "$device" == "gpu" ] ; then
    export GNUMPY_CPU_PRECISION=32
    export GNUMPY_USE_GPU=yes
    export THEANO_FLAGS=floatX=float32,device=gpu
else
    echo "Unknown device: $device"
    exit 1
fi

