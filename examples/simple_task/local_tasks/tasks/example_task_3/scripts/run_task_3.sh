#!/bin/bash
#SBATCH --output=/home/joao.pinto/new_git_repos/novacula/examples/simple_task/local_tasks/tasks/example_task_3/scripts/output_3.out
#SBATCH --error=/home/joao.pinto/new_git_repos/novacula/examples/simple_task/local_tasks/tasks/example_task_3/scripts/output_3.err
#SBATCH --job-name=task_2_to_task_3
#SBATCH --dependency=afterok:327
source /home/joao.pinto/new_git_repos/novacula/novacula-env/bin/activate
ntask -t /home/joao.pinto/new_git_repos/novacula/examples/simple_task/local_tasks/tasks.json -i 3
