__all__ = []
import argparse

from novacula.models.task import load
from novacula import get_context
from novacula.db import get_db_service, models 
from novacula.db import JobStatus as job_status
from novacula.db import TaskStatus as task_status


def create(args):
    ctx = get_context( clear=True )
    db_service = get_db_service( args.db_file )
    
    load( args.task_file , ctx)
    tasks = {task.task_id: task for task in ctx.tasks.values()}
    task = tasks.get( args.index )
    
    if not task:
        raise Exception(f"Task with index {args.index} not found.")

    # change the current status to running
    with db_service() as session:
        try:
            task_db = session.query( models.Task ).filter( models.Task.name == task.name ).one()
            task_db.status = task_status.RUNNING
            session.commit()
        finally:
            session.close()


    job_id = task.run()
    script = Script()
    
    script = Script( f"{task.path}/scripts/run_task_{task.task_id}.sh", 
                    args = {
                        #"array"     : ",".join( [str(job_id) for job_id in task._get_array_of_jobs_with_status( models.JobStatus.ASSIGNED ) ]),
                        "output"    : f"{task.path}/works/job_%a/output.out",
                        "error"     : f"{task.path}/works/job_%a/output.err",
                        "partition" : task.partition,
                        "job-name"  : f"task-{task.task_id}",
                    }
                    )
    script += f"source {ctx.virtualenv}/bin/activate"
    script += f"njob -i {task.path}/jobs/job_$SLURM_ARRAY_TASK_ID.json --message-level INFO -o {task.path}/works/job_$SLURM_ARRAY_TASK_ID -j $SLURM_ARRAY_JOB_ID"
    script.dump()

    try:
        job_id = script.submit()
    except Exception as e:
        raise Exception(f"Failed to submit job for task {task.name}: {str(e)}")
    
    
        
    script = Script( f"{self.path}/scripts/close_task_{task.task_id}.sh",
                    args = {
                        "output"    : f"{task.path}/scripts/run_task_{task.task_id}.out",
                        "error"     : f"{task.path}/scripts/run_task_{task.task_id}.err",
                        "job-name"  : f"{task.task_id}-{task.task_id}",
                        "dependency": f"afterok:{job_id}",
                    }
    )
    script += f"source {ctx.virtualenv}/bin/activate"
    script += f"ntask close --task-file {ctx.path}/tasks.json --index {task.task_id} --db-file {ctx.path}/db/data.db"
    script.dump()
    script.submit()
        
    
    
    
    
def close(args):
    
    ctx = get_context( clear=True )
    db_service = get_db_service( args.db_file )
    
    load( args.task_file , ctx)
    tasks = {task.task_id: task for task in ctx.tasks.values()}
    task = tasks.get( args.index )
    
    if not task:
        raise Exception(f"Task with index {args.index} not found.")

    ok = True
    with db_service() as session:
        try:
            task_db = session.query( models.Task ).filter( models.Task.name == task.name ).one()
            if not task_db:
                raise Exception(f"Task with index {args.index} not found in database.")
            if all( job.status == job_status.COMPLETED for job in task_db.jobs ):
                task_db.status = task_status.COMPLETED
            elif sum([ job.status == job_status.FAILED for job in task_db.jobs ]) / len(task_db.jobs) > 0.1:
                task_db.status = task_status.FAILED
                ok = False
            else:
                task_db.status = task_status.FINALIZED
            session.commit()
        finally:
            session.close()
            
    
    # if the current task is failed, we need to cancel the entire graph
    if not ok:
        with db_service() as session:
            try:
                for task_db in session.query( models.Task ).all():
                    if task_db.status == task_status.ASSIGNED:
                        task_db.status = task_status.CANCELED
                session.commit()
            finally:
                session.close()
                
    else: 
        print(f"Task {task.name} finalized successfully.")
        
        # need to start the other tasks that depend on this one
        for task in task.next:
            print(f"Starting dependent task {task.name}.")
            
            script = Script( f"{task.path}/scripts/create_task_{task.task_id}.sh",
                    args = {
                        "output"    : f"{task.path}/scripts/create_task_{task.task_id}.out",
                        "error"     : f"{task.path}/scripts/create_task_{task.task_id}.err",
                        "job-name"  : f"create-{task.task_id}",
                    }
            )
            script += f"source {ctx.virtualenv}/bin/activate"
            script += f"ntask create --task-file {ctx.path}/tasks.json --index {task.task_id} --db-file {ctx.path}/db/data.db"
            script.dump()
            script.submit()
    

def args_parser():

    parser = argparse.ArgumentParser(description = '', add_help = False)
    parser.add_argument('-i','--index', action='store', dest='index', required = True,
                        help = "The task index", type=int)   
    parser.add_argument('--task-file', action='store', dest='task_file', required=True,
                        help="The task file input")
    parser.add_argument('--db-file', action='store', dest='db_file', required=True,
                        help="The database file input")
    return [parser]





def build_argparser():

    formatter_class = get_argparser_formatter()

    parser    = argparse.ArgumentParser(formatter_class=formatter_class)
    mode = parser.add_subparsers(dest='mode')


    run_parent = argparse.ArgumentParser(formatter_class=formatter_class, add_help=False, )
    option = run_parent.add_subparsers(dest='option')
    option.add_parser("job"   , parents = job_parser()   ,help='',formatter_class=formatter_class)
    option.add_parser("create", parents = task_parser()  ,help='',formatter_class=formatter_class)
    option.add_parser("close" , parents = task_parser()  ,help='',formatter_class=formatter_class)
    mode.add_parser( "run", parents=[run_parent], help="",formatter_class=formatter_class)
    

    return parser

def run_parser(args):
    if args.mode == "run":
        if args.option == "job":
            from .job import run
            run(args)
        elif args.option == "create":
            from .task import create
            create(args)
        elif args.option == "close":
            from .task import close
            close(args)
       

def run():

    parser = build_argparser()
    if len(sys.argv)==1:
        print(parser.print_help())
        sys.exit(1)

    args = parser.parse_args()
    run_parser(args)



if __name__ == "__main__":
  run()