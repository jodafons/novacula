



class task_parser:

  def __init__(self , host, args=None):

    self.db = postgres(host)
    if args:

      # Create Task
      create_parser = argparse.ArgumentParser(description = '', add_help = False)
      delete_parser = argparse.ArgumentParser(description = '', add_help = False)
      retry_parser  = argparse.ArgumentParser(description = '', add_help = False)
      list_parser   = argparse.ArgumentParser(description = '', add_help = False)


      create_parser.add_argument('-t','--task', action='store', dest='taskname', required=True,
                          help = "The task name to be created.")
      create_parser.add_argument('-i','--inputfile', action='store',
                          dest='inputfile', required = True,
                          help = "The input file path.")
      create_parser.add_argument('--image', action='store', dest='image', required=False, default="",
                          help = "The image name")
      create_parser.add_argument('--exec', action='store', dest='command', required=True,
                          help = "The exec command")
      create_parser.add_argument('--dry_run', action='store_true', dest='dry_run', required=False, default=False,
                          help = "Use this as debugger.")
      create_parser.add_argument('--binds', action='store', dest='binds', required=False, default="{}",
                          help = "image volume bindd like {'/home':'/home','/mnt/host_volume:'/mnt/image_volume'}")
      create_parser.add_argument('-p', '--partition',action='store', dest='partition', required=True,
                          help = f"The selected partitions. Availables: {partitions}")


      delete_parser.add_argument('--id', action='store', dest='id_list', required=False, default='',
                    help = "All task ids to be deleted", type=str)
      delete_parser.add_argument('--force', action='store_true', dest='force', required=False,
                    help = "Force delete.")

      retry_parser.add_argument('--id', action='store', dest='id_list', required=False, default='',
                    help = "All task ids to be retried", type=str)

      kill_parser = argparse.ArgumentParser(description = '', add_help = False)
      kill_parser.add_argument('--id', action='store', dest='id_list', required=False, default='',
                    help = "All task ids to be killed", type=str)


      parent = argparse.ArgumentParser(description = '', add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('create', parents=[create_parser])
      subparser.add_parser('retry' , parents=[retry_parser])
      subparser.add_parser('delete', parents=[delete_parser])
      subparser.add_parser('list'  , parents=[list_parser])
      subparser.add_parser('kill'  , parents=[kill_parser])
      args.add_parser( 'task', parents=[parent] )

  

  def parser( self, args ):

    if args.mode == 'task':

      if args.option == 'create':
        self.create(os.getcwd(), args.taskname, args.inputfile,
                    args.image, args.command, args.email, dry_run=args.dry_run,
                    do_test=args.do_test, binds=args.binds, partition=args.partition)

      elif args.option == 'retry':
        self.retry(convert_string_to_range(args.id))
        
      elif args.option == 'delete':
        self.delete(convert_string_to_range(args.id), force=args.force)
        
      elif args.option == 'list':
        self.list()
        
      elif args.option == 'kill':
        self.kill(convert_string_to_range(args.id))
        
      else:
        logger.error("Option not available.")


  def create(self, basepath: str, taskname: str, inputfile: str,
                   image: str, command: str, email: str, dry_run: bool=False, do_test=True,
                   extension='.json', binds="{}", partition='cpu' ):


    session = APIClient(args.host, args.token)
    input_ds = Dataset(args.taskname + "_input")
    input_ds.upload(args.inputfile, as_link=True)
    image Image("docker://"+args.image, args.image, as_link=True)
    task = Task(taskname, input_ds, image, command, email, dry_run=dry_run, do_test=do_test, binds=binds, partition=partition)
    session.run(task)



    with self.db as session:
      return create(session, basepath, taskname, inputfile, image, command, email, 
                    dry_run=dry_run, do_test=do_test, binds=binds, partition=partition)

  def kill(self, task_ids):
    with self.db as session:
      for task_id in task_ids:
        kill(session, task_id)

  def delete(self, task_ids, force=False):
    with self.db as session:
      for task_id in task_ids:
        delete(session, task_id, force=force)

  def retry(self, task_ids):
    with self.db as session:
      for task_id in task_ids:
        retry(session, task_id)

  def list(self):
    print('not implemented')
  #  print(self.db.resume())

