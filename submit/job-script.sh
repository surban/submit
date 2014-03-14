#!/bin/bash
#SBATCH --signal=INT@60
#SBATCH --open-mode=append

function get_job_state {
    scontrol show job "$1" | grep JobState | cut -d" " -f4 | cut -d = -f2
}

runner="$1"
cfg="$2"
device="$3"
xpu_twin="$4"
prolog="$5"

echo "Job id is $SLURM_JOB_ID"
echo "Device is $device"

if [ "$SLURM_RESTART_COUNT" != "" ] ; then
    echo "Restart count is $SLURM_RESTART_COUNT"
    cont="continue"
else
    cont=""

    # handle GPU prefered jobs
    if [ "$xpu_twin" != "0" ] ; then
        if [ "$device" == "gpu" ] ; then
            # if CPU twin job was already running or completed that it would have
            # canceled this job
            echo "Canceling CPU twin $xpu_twin"
            scancel "$xpu_twin"
        elif [ "$device" == "cpu" ] ; then
            state=$(get_job_state $xpu_twin)
            if [ "$state" == "PENDING" ] ; then
                echo "Canceling pending GPU twin $xpu_twin"
                scancel "$xpu_twin"
            else
                echo "State of GPU twin $xpu_twin is $state"
                echo "Canceling this CPU job"
                scancel $SLURM_JOB_ID
                sleep 10
                exit 0
            fi
        fi
        sleep 10
    fi
fi

. "$prolog"

echo
echo "Execution directory is $(pwd)"
echo $runner "$cfg" "$cont"
echo

$runner "$cfg" "$cont"

if [ "$?" == "9" ] ; then
    echo
    echo "Requeing job..."
    scontrol requeue $SLURM_JOB_ID
fi



