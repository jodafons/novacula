__all__ = ["get_scheduler_service"]


__scheduler_service = None

def get_scheduler_service():
    global __scheduler_service
    if not __scheduler_service:
        from .scheduler import SchedulerFIFO as Scheduler
        __scheduler_service = Scheduler()
    return __scheduler_service