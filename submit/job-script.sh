#!/bin/bash
#SBATCH --signal=INT@300
#SBATCH --open-mode=append

function get_job_state {
    scontrol show job "$1" | grep JobState | cut -d" " -f4 | cut -d = -f2
}

function on_sigterm {
    rm -f "$cfg/_running"
    exit 1
}

runner="$1"
cfg="$2"
device="$3"
xpu_twin="$4"
prolog="$5"

echo "Job id is $SLURM_JOB_ID"
echo "Hostname is $(hostname)"
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

trap on_sigterm TERM
rm -f "$cfg/_finished" "$cfg/_failed" "$cfg/_requeued"
echo $SLURM_JOB_ID > "$cfg/_running"

. "$prolog"

echo
echo "Execution directory is $(pwd)"
echo $runner "$cfg" "$cont"
echo

srun $runner "$cfg" "$cont"
retval=$?

rm -f "$cfg/_running"
echo
echo "Exit code is $retval"

if [ "$retval" == "0" ] ; then
    touch "$cfg/_finished"
elif [ "$?" == "9" ] ; then
    echo "Requeing job..."
    touch "$cfg/_requeued"
    scontrol requeue $SLURM_JOB_ID
else
    touch "$cfg/_failed"
fi

exit $retval



