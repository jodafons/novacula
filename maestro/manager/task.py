__all__ = ["TaskManager"]


from loguru     import logger
from datetime   import datetime
from typing     import Dict, List
from maestro    import StatusCode, schemas, random_id
from maestro.db import get_db_service, models, job_status
from maestro.db import TaskStatus, JobStatus
from maestro.io import get_io_service

GB=1024


class TaskManager:

    def __init__(
        self, 
        user_id : str,
        envs    : Dict[str,str],
    ):
        self.envs=envs
        self.user_id=user_id
        self.user_name=get_db_service().user(user_id).fetch_name()


    def status(
        self,
        task_id : str
    ) -> StatusCode:
        
        db_service = get_db_service()

        if not db_service.task(task_id).check_existence():
            reason=f"task {task_id} does not exist into db."
            logger.error(reason)
            return StatusCode.FAILURE(reason=reason)

        status = db_service.task(task_id).fetch_status()
        return StatusCode.SUCCESS(status)


    def describe(
        self,
        task_id : str 
    ):

        db_service = get_db_service()
        
        if not db_service.check_task_existence(task_id):
            reason=f"task {task_id} does not exist into db."
            logger.error(reason)
            return StatusCode.FAILURE(reason=reason)
        
        task = schemas.TaskInfo()

        with db_service() as session:
            table = {status.value:0 for status in job_status}
            table["retry"]=0
            task_db = session.query(models.Task).filter_by(task_id=task_id).one()
            task.task_id   = task_db.task_id
            task.name      = task_db.name
            task.user_id   = task_db.user_id
            task.partition = task_db.partition
            task.status    = task_db.status.value
            jobs = []
            for job_db in task_db.jobs:
                table[job_db.status.value]+=1
                jobs.append(job_db.job_id)
                task.retry    += job_db.retry
            task.jobs      = jobs
            task.counts    = table
            
        return StatusCode.SUCCESS(task)


    def list(
        self,
        match_with="*"
    ) -> StatusCode:
        
        db_service = get_db_service()
        match_with = match_with.replace("*","%")
        names = []
        with db_service() as session:
            tasks_from_db = session.query(models.Task).filter(models.Task.name.like(match_with)).all()
            for task_db in tasks_from_db:
                names.append(task_db.task_id)
        tasks = [self.describe(name).result() for name in names]
        return StatusCode.SUCCESS(tasks)


    def create_task_group( 
        self,    
        tasks : List[schemas.TaskInputs] 
    ) -> StatusCode:

        
        db_service = get_db_service()
        
        datasets = {
            'inputs'   : {},
            'outputs'  : {},
        }
        
                
        #
        # NOTE: stage 1, collect all datasets (inputs/outputs)
        #
        for task in tasks:
            for key, value in task.secondary_data.items():
                if not value in datasets["inputs"]:
                    datasets['inputs'][value]=[task.name]
                else:
                    datasets["inputs"].append(task.name)
            if task.input:
                if not task.input in datasets["inputs"]:
                    datasets['inputs'][task.input]=[task.name]
                else:
                    datasets["inputs"][task.input].append(task.name)
                    
            for key, value in task.outputs.items():
                name = f"{task.name}.{value}"
                if not name in datasets["outputs"]:
                    datasets["outputs"][name] = [task.name]
                else:
                    datasets["outputs"].append(name)
                    

        logger.info("validating all tasks inside of this group")
        for task in tasks:

            logger.info(f"validating task with name {task.name}...")

            #
            # NOTE: stage 1, checking task
            #
            if not task.name.startswith( f"user.{self.user_name}."):
                reason=f"the name dataset must follow the name rule: 'user.{self.user_name}.name'"
                return StatusCode.FAILURE(reason=reason)
            
            if db_service.check_task_existence_by_name( task.name ):
                reason=f"the task with name {task.name} exist into the database."
                return StatusCode.FAILURE(reason=reason)

            #
            # NOTE: stage 2, checking image
            #
            if not db_service.check_dataset_existence_by_name( task.image ):
                reason=f"the image with name {task.image} does not exist into the database."
                return StatusCode.FAILURE(reason=reason)


            #
            # NOTE: stage 3, checking input dataset
            #
         
            if (task.input!="") and ( (task.input not in datasets["outputs"]) and (not db_service.check_dataset_existence_by_name(task.input)) ):
                reason=f"input dataset with name {task.input} does not exist into the database or is not expected inside of this group of tasks as an output dataset."
                logger.error(reason)
                return StatusCode.FAILURE(reason=reason)
            
            if not db_service.check_dataset_existence_by_name(task.input):
                logger.info(f"the dataset with name {task.input} will be create in future.")


            #
            # NOTE: stage 4, checking extra inputs
            #
            for key, value in task.secondary_data.items():
                if not f"%{key}" in task.command:
                    reason=f"you should have %{key} as extra input inside of the command."
                    return StatusCode.FAILURE(reason=reason)

                if (not db_service.check_dataset_existence_by_name(value)) and (not value in datasets['outputs']):
                    reason=f"extra dataset with name {value} does not exist into the database or is not expected inside of this group of tasks as an output dataset."
                    logger.error(reason)
                    return StatusCode.FAILURE(reason=reason)
            
                if not db_service.check_dataset_existence_by_name(value): 
                    logger.info(f"the dataset with name {value} will be create in future.")

            #
            # NOTE: stage 5, check input dataset
            #
            if len(task.outputs)==0:
                reason=f"you should have at least one output for this task"
                logger.error(reason)
                return StatusCode.FAILURE(reason=reason)
            
            
            #
            # NOTE: stage 6, checkout outputs
            #
            logger.info("checking outputs...")
            for key, value in task.outputs.items():
                # format? outputs = {"OUT_RESULT":"result.pkl", "OUT_CIRCUIT":"circuit.json"}
                if not f"%{key}" in task.command:
                    reason=f"you should have %{key} inside of the command."
                    return StatusCode.FAILURE(reason=reason)
                name = f"{task.name}.{value}"
                
                if db_service.check_dataset_existence_by_name(name) and ( name in datasets["outputs"] and len(datasets["outputs"][name])!=1 ):
                    reason =f"output dataset with name {name} exist into the database or another task inside of this group uses the same name as output."
                    reason+=f"you should change the name of the task before launch a new one."
                    return StatusCode.FAILURE(reason=reason)
            # NOTE: if here, this task is validated
            
        # LOOP: end of tasks
            
            
        #
        # NOTE: stage 7, create all tasks and put as pre-registered
        #
        task_ids = []
        logger.info("creating all tasks")
        for task in tasks:
            task_id           = random_id()
            task_db           = models.Task()
            task_db.task_id   = task_id
            task_db.user_id   = self.user_id
            task_db.name      = task.name
            task_db.status    = TaskStatus.PRE_REGISTERED
            task_db.partition = task.partition

            parent_tasks      = []
            if task.input in datasets["outputs"]:
                parent_tasks.extend( datasets["outputs"][task.input] )
                
            for dataset in task.secondary_data.values():
                if dataset in datasets["outputs"]:
                    parent_tasks.extend(datasets['outputs'][dataset])                
            parent_tasks       = list(set(parent_tasks)) # remove duplicates
            task_db.parents    = parent_tasks
            task_db.task_inputs= task
            db_service.save_task(task_db)
            task_ids.append(task_id)
            
        return StatusCode.SUCCESS(task_ids)



    def run_task_group( self, task_id : str ) -> StatusCode:
        
        db_service = get_db_service()
        io_service = get_io_service()
        db_string  = db_service.db_string
        volume     = io_service.volume
            
        if not db_service.check_task_existence( task_id ):
            reason=f"task with id {task_id} not exist into the database."
            return StatusCode.FAILURE(reason=reason) 
        
        logger.info(f"checking {task_id}...")
        
        task = db_service.task(task_id).fetch_task_inputs()
        parents = db_service.task(task_id).fetch_parents()
        
        for name in parents:
            logger.info(f"checking {name} parent for task {task_id}")
            if not db_service.check_task_existence_by_name( name ):
                reason=f"parent task with name {name} not exist into the database."
                return StatusCode.FAILURE(reason=reason) 
        
            parent_task_id = db_service.fetch_task_from_name( name )
            status = db_service.task(parent_task_id).fetch_status()
            if status not in [TaskStatus.COMPLETED, TaskStatus.FINALIZED]:
                reason=f"the current task {task_id} is not ready since the parent task with name {name} has status {status.value}."
                return StatusCode.FAILURE(reason=reason)

     
        logger.info(f"prepering task with name {task.name} to run...")
        
        if task.input!="":
            if not db_service.check_dataset_existence_by_name(task.input):
                reason=f"input dataset with name {task.input} does not exist into the database."
                return StatusCode.FAILURE(reason=reason)
            
            if not "%IN" in task.command:
                reason=f"command must have %IN inside since this task requires a input dataset."
                return StatusCode.FAILURE(reason=reason)
            
            dataset_id=db_service.fetch_dataset_from_name(task.input)
            files = io_service.dataset(dataset_id).files(with_file_id=True)
            files=[ f"{dataset_id}:{file_id}" for file_id in files.keys()]
        else:
            files = [""]
         
        
        for key, name in task.secondary_data.items():
            if not f"%{key}" in task.command:
                reason=f"the command must have {key} inside since this task requires extra datasets."
                return StatusCode.FAILURE(reason=reason)

            if not db_service.check_dataset_existence_by_name(name) :
                reason=f"the dataset with name {name} does not exists into the database."
                return StatusCode.FAILURE(reason=reason)
            
            
        for key, name in task.outputs.items():
            logger.info(f"looking for {task.name}.{name} dataset output...")
            if not f"%{key}" in task.command:
                reason=f"the command must have {key} inside since this task requires extra datasets."
                return StatusCode.FAILURE(reason=reason)

            if db_service.check_dataset_existence_by_name(name) :
                reason=f"the dataset with name {name} does not exists into the database."
                return StatusCode.FAILURE(reason=reason)
            
            # create the output dataset
            dataset_id = random_id()
            dataset_db = models.Dataset()
            dataset_db.dataset_id = dataset_id
            dataset_db.user_id = self.user_id
            dataset_db.name = f"{task.name}.{name}"
            dataset_db.updated_time = datetime.now()
            db_service.save_dataset( dataset_db )
            io_service.dataset(dataset_id).mkdir()

            
        with db_service() as session:
            
            task_db  = session.query(models.Task).filter_by(task_id=task_id).one()
            image_id = db_service.fetch_dataset_from_name(task.image)

            for job_index, file_id in enumerate(files):

                job_id         = random_id()
                job_db         = models.Job()
                job_db.job_id  = job_id
                job_db.task_id = task_id
                job_db.user_id = self.user_id
                job_db.job_index=job_index

                # Where and How?
                job_db.partition              = task_db.partition
                job_db.status                 = JobStatus.REGISTERED
                job_db.device                 = "cpu" if task.device=="" else task.device
                job_db.reserved_cpu_number    = task.cpu_cores
                job_db.reserved_sys_memory_mb = task.memory_mb if task.memory_mb>0 else 5*GB
                job_db.reserved_gpu_memory_mb = task.gpu_memory_mb if task.gpu_memory_mb>0 else (0 if job_db.device=="cpu" else 5*GB)

                job_name    = f"job-{job_index}"
                task_name   = task.name
                command = f"maestro run job"
                command+= f" --volume {volume}"
                command+= f" --database-string {db_string}"
                command+= f" --task-id {task_id}"
                command+= f" --job-id {job_id}"
                command+= f" --image-id {image_id}"
                command+= f" --input {file_id}"
                command+= f" --job-name {job_name}"
                command+= f" --task-name {task_name}"
                if self.envs["WANDB_API_KEY"]!="":
                    command+= f" --wandb-token {self.envs['WANDB_API_KEY']}"
                if self.envs["WANDB_USER"]!="":
                    command+= f" --wandb-user {self.envs['WANDB_USER']}"
                
                print(command)
                job_db.command = command
                task_db+=job_db
            
            task_db.status = TaskStatus.REGISTERED
            session.commit()
        
        logger.info(f"creating task id {task_id}...")
        return StatusCode.SUCCESS(task_id)
    