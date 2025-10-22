#!/bin/bash
#SBATCH --output=/home/joao.pinto/new_git_repos/novacula/examples/simple_task/local_tasks/tasks/example_task_1/scripts/output_1.out
#SBATCH --error=/home/joao.pinto/new_git_repos/novacula/examples/simple_task/local_tasks/tasks/example_task_1/scripts/output_1.err
#SBATCH --job-name=task_0_to_task_1
#SBATCH --dependency=afterok:325
source /home/joao.pinto/new_git_repos/novacula/novacula-env/bin/activate
ntask -t /home/joao.pinto/new_git_repos/novacula/examples/simple_task/local_tasks/tasks.json -i 1
