datasets = {'inputs'   : {},'outputs'  : {},}
        
        # NOTE: stage 1, collect all datasets (inputs/outputs)
        for task in __tasks__.values():
            if task.input_data:
                if not task.input_data.name in datasets["inputs"]:
                    datasets['inputs'][task.input_data.name]=[task.name]
                else:
                    datasets["inputs"][task.input_data.name].append(task.name)    
            # datasets outputs
            for key, value in task.outputs_data.items():
                name = value['data'].name
                datasets["outputs"][name]=task.name
            # datasets secondary data
            for key, value in task.secondary_data.items():
                name = value.name
                if not name in datasets["inputs"]:
                    datasets["inputs"][name] = [task.name]
                else:
                    datasets["inputs"][name].append(task.name)

        # NOTE: stage 2: organize all names
        task_names_dependency = {}
        for key in datasets['inputs'].keys():
            if key in datasets['outputs']:
                task_names_dependency[datasets['outputs'][key]] = datasets['inputs'][key]
            else:
                task_names_dependency[key]=datasets['inputs'][key]

        # NOTE: stage 3, create task links
        task_names = { name:task for name, task in __tasks__.items() }
        for task_name, next_tasks in task_names_dependency.items():
            if task_name in task_names:
                task = task_names[ task_name ]
                for next_task_name in next_tasks:
                    if next_task_name in task_names:
                        next_task = task_names[ next_task_name ]
                        task.next.append( next_task )
                        next_task.before.append( task )

        for task in __tasks__.values():
            print([t.name for t in task.before], " --> ", task.name, " --> ", [t.name for t in task.next] )
            task.init( virtualenv=self.virtualenv )
        
        jobs = {}
