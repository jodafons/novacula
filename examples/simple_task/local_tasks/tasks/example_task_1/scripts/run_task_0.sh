#!/bin/bash
#SBATCH --array=0-3
#SBATCH --output=/home/joao.pinto/new_git_repos/novacula/examples/simple_task/local_tasks/tasks/example_task_1/works/job_%a/output.out
#SBATCH --error=/home/joao.pinto/new_git_repos/novacula/examples/simple_task/local_tasks/tasks/example_task_1/works/job_%a/output.err
#SBATCH --partition=cpu-large
#SBATCH --job-name=example_task_1
source /home/joao.pinto/new_git_repos/novacula/novacula-env/bin/activate
njob -i /home/joao.pinto/new_git_repos/novacula/examples/simple_task/local_tasks/tasks/example_task_1/jobs/job_$SLURM_ARRAY_TASK_ID.json --message-level INFO -o /home/joao.pinto/new_git_repos/novacula/examples/simple_task/local_tasks/tasks/example_task_1/works/job_$SLURM_ARRAY_TASK_ID -j $SLURM_ARRAY_JOB_ID
