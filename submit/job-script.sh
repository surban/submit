#!/bin/bash
#SBATCH --signal=INT@60
#SBATCH --open-mode=append

runner="$1"
cfg="$2"
cont=""

if [ "$SLURM_RESTART_COUNT" != "" ] ; then
    echo "Restart count is $SLURM_RESTART_COUNT"
    cont="continue"
fi

srun python -m "$runner" "$cfg" "$cont"

if [ "$?" == "9" ] ; then
    echo "Requeing job..."
    scontrol requeue $SLURM_JOB_ID
fi



