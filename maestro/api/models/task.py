__all__ = ["Task", "Group"]

from time     import sleep
from typing   import List
from tabulate import tabulate
from maestro  import schemas


class Group:
    def __init__(self, task_ids : List[str]):
        self.tasks = [Task(task_id) for task_id in task_ids]
        
        
    def print(self):
        rows  = []
        tasks = [task.describe() for task in self.tasks]
        for task in tasks:
            row = [task.name]
            row.extend( [value for value in task.counts.values()])
            row.extend([task.retry, task.status])
            rows.append(row)
        cols = ['taskname']
        cols.extend([name for name in tasks[0].counts.keys()])
        cols.extend(["Retry", "Status"])
        table = tabulate(rows ,headers=cols, tablefmt="psql")
        print(table)


class Task:

    def __init__(
        self,
        task_id    : str,
    ) -> None:
        from maestro  import get_session_api
        self.__api_client = get_session_api()
        self.task_id = task_id
        self._result = None
        self._kill   = False

    def task_completed(self):
        '''
        Returns task if any of the following cases are found. The cases are: Failed, Completed or Canceled.
        '''
        status = self.status()
        return (
            status    == "failed"
            or status == "completed"
            or status == "killed"
        )

    def join(self):
        while not self.task_completed():
            sleep(5)

    def results(self):
        return self.__api_client.task().results(self.task_id)
       
    def cancel(self) -> bool:
        '''
        Cancel the task.
        '''
        if not self._kill:
            self._kill = self.__api_client.task().cancel_task(self.task_id)
        return self._kill
        
    def status(self) -> str:
        '''
        Return the status of the job.
        '''
        return self.__api_client.task().status(self.task_id)

    def describe(self) -> schemas.TaskInfo:
        return self.__api_client.task().describe(self.task_id)

    def print(self):
        task   = self.describe()
        row    = [[value for value in task.counts.values()]]
        row[0].append(task.status)
        cols   = [name for name in task.counts.keys()]
        cols.append("Status")
        table = tabulate(row, headers=cols, tablefmt="psql")
        print(table)