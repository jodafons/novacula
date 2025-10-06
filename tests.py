import os, json 
from novacula import Dataset, Provider


provider = Provider("","")


basepath     = os.getcwd()
dataset_name = "user.joao.pinto.dataset.3"
dataset_path = f"{basepath}/{dataset_name}" 

os.makedirs(dataset_path, exist_ok=True)
for i in range(10):
    with open (f"{dataset_path}/file_{i}.json",'w') as f:
        json.dump({"i":i}, f)

dataset = Dataset( dataset_name)
dataset.create()
dataset.upload(dataset_path, as_link=True)

dataset.download(targetfolder=f"{basepath}/downloaded", as_link=True)