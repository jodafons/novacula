

import os, json
from novacula import LocalProvider, Task, Dataset, Image


basepath = os.getcwd()
input_path = f"{basepath}/input_data_1"
os.makedirs(input_path, exist_ok=True)
for i in range(4):
    with open(f"{input_path}/{i}.json",'w') as f:
        d={'a':i*10,'b':i*2}
        json.dump(d,f)

input_path = f"{basepath}/input_data_2"
os.makedirs(input_path, exist_ok=True)
for i in range(4):
    with open(f"{input_path}/{i}.json",'w') as f:
        d={'a':i*10,'b':i*2}
        json.dump(d,f)

with LocalProvider(name="local_provider", path=f"{basepath}/local_tasks") as session:


    input_dataset_1  = Dataset(name="input_data_1", path=f"{basepath}/input_data_1")
    input_dataset_2  = Dataset(name="input_data_2", path=f"{basepath}/input_data_2")


    #secondary_data   = Dataset(name="secondary_data", path=f"{basepath}/secondary_data")
    image            = Image(name="python", path=f"{basepath}/python3.10.sif")

    command = f"python {basepath}/app.py --job %IN --output %OUT"

    binds = {"/mnt/cern_data" : "/mnt/cern_data"}

    task_1 = Task(name="example_task_1",
                  image=image,
                  command=command,
                  input_data=input_dataset_1,
                  outputs={'OUT':'output.json'},
                  partition='cpu-large',
                  binds=binds)
    
    task_2 = Task(name="example_task_2",
                  image=image,
                  command=command,
                  input_data=task_1.output('OUT'),
                  outputs= {'OUT':'output.json'},
                  partition='cpu-large',
                  binds=binds)

    task_3 = Task(name="example_task_3",
                  image=image,
                  command=command,
                  input_data=input_dataset_2,
                  outputs= {'OUT':'output.json'},
                  partition='cpu-large',
                  binds=binds)
    task_4 = Task(name="example_task_4",
                  image=image,
                  command=command,
                  input_data=task_2.output('OUT'),
                  outputs= {'OUT':'output.json'},
                  partition='cpu-large',
                  binds=binds,
                  secondary_data={'DATA_2': task_3.output('OUT')}
                  )
    task_5 = Task(name="example_task_5",
                  image=image,
                  command=command,
                  input_data=task_2.output('OUT'),
                  outputs= {'OUT':'output.json'},
                  partition='cpu-large',
                  binds=binds)

    #task_1 >> task_2 >> task_3
    session.run()
    