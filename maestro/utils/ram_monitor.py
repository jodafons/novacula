__all__ = ["MemoryMonitor"]


from loguru        import logger
from maestro.db    import models, get_db_service
from maestro.utils import Popen
from datetime      import datetime


class MemoryMonitor:
    def __init__(
        self, 
        T          : int=60,
        percentage : float=0.8,
        dynamic    : bool=False,
        ):
        
        self.t1 = None
        self.t2 = None
        self.proc_sys_memory_mb_t1 = None
        self.proc_sys_memory_mb_t2 = None
        self.proc_gpu_memory_mb_t1 = None
        self.proc_gpu_memory_mb_t2 = None
        self.T = T
        self.percentage = percentage
        self.dynamic = dynamic
        
    def __call__( self, proc : Popen, job_id : str , log = None) -> bool:
        
        db_service          = get_db_service()
        health              = True
        metrics             = proc.metrics()
        proc_sys_memory_mb  = metrics['sys_memory_mb_peak']
        proc_gpu_memory_mb  = metrics['gpu_memory_mb_peak']

        if log:
            log.log(metrics)
      


        if not self.t2: # NOTE: at first time, just collect the 'a' and return health since not be able to evaluate.
            self.t2=datetime.now()
            self.proc_gpu_memory_mb_t2=proc_gpu_memory_mb
            self.proc_sys_memory_mb_t2=proc_sys_memory_mb
            return health

        self.t1 = self.t2
        self.proc_sys_memory_mb_t1 = self.proc_sys_memory_mb_t2
        self.proc_gpu_memory_mb_t1 = self.proc_gpu_memory_mb_t2
        
        self.t2 = datetime.now()
        self.proc_sys_memory_mb_t2 = proc_sys_memory_mb
        self.proc_gpu_memory_mb_t2 = proc_gpu_memory_mb
        
        delta_t = (self.t2-self.t1).total_seconds()
        

        increase_sys_memory = False
        increase_gpu_memory = False
        with db_service() as session:
            job_db = session.query(models.Job).filter_by(job_id=job_id).one()
            job_db.used_sys_memory_mb = proc_sys_memory_mb
            job_db.used_gpu_memory_mb = proc_gpu_memory_mb
            reserved_sys_memory_mb    = job_db.reserved_sys_memory_mb
            reserved_gpu_memory_mb    = job_db.reserved_gpu_memory_mb
            if job_db.reserved_sys_memory_mb > 0 and (proc_sys_memory_mb > job_db.reserved_sys_memory_mb*self.percentage):
                increase_sys_memory=True
            if job_db.reserved_gpu_memory_mb > 0 and (proc_gpu_memory_mb > job_db.reserved_gpu_memory_mb*self.percentage):
                increase_gpu_memory=True
            session.commit()
            
        health = not (increase_gpu_memory or increase_sys_memory)

        if not health and self.dynamic: 
            
            pred_sys_memory_mb  = (self.proc_sys_memory_mb_t2 - self.proc_sys_memory_mb_t1) * (self.T/delta_t) + self.proc_sys_memory_mb_t1 
            pred_gpu_memory_mb  = (self.proc_gpu_memory_mb_t2 - self.proc_gpu_memory_mb_t1) * (self.T/delta_t) + self.proc_gpu_memory_mb_t1
            delta_sys_memory_mb = (pred_sys_memory_mb - reserved_sys_memory_mb)
            delta_gpu_memory_mb = (pred_gpu_memory_mb - reserved_gpu_memory_mb)
            increase_sys_memory = False if delta_sys_memory_mb < 0 else increase_sys_memory
            increase_gpu_memory = False if delta_gpu_memory_mb < 0 else increase_gpu_memory

            if increase_sys_memory or increase_gpu_memory:
                with db_service() as session:
                    job_db = session.query(models.Job).filter_by(job_id=job_id).one() 
                    if increase_sys_memory:
                        job_db.reserved_sys_memory_mb+=delta_sys_memory_mb
                        increase_sys_memory=False
                    if increase_gpu_memory:
                        job_db.reserved_gpu_memory_mb+=delta_gpu_memory_mb
                        increase_gpu_memory=False
                    session.commit()
        if not health:
            logger.warning(f"job is not health.")
        return health
