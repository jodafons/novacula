#!/usr/bin/env python

import sys
import argparse

from maestro           import get_argparser_formatter
from maestro.app       import args_parser as app_parser
from maestro.loop.job  import args_parser as job_parser


def build_argparser():

    formatter_class = get_argparser_formatter()

    parser    = argparse.ArgumentParser(formatter_class=formatter_class)
    mode = parser.add_subparsers(dest='mode')


    run_parent = argparse.ArgumentParser(formatter_class=formatter_class, add_help=False, )
    option = run_parent.add_subparsers(dest='option')
    option.add_parser("app", parents = app_parser()  ,help='Run the app',formatter_class=formatter_class)
    option.add_parser("job", parents = job_parser()  ,help='Run the job',formatter_class=formatter_class)
    mode.add_parser( "run", parents=[run_parent], help="",formatter_class=formatter_class)
    
    
    
    #args_parent = argparse.ArgumentParser(formatter_class=formatter_class, add_help=False, )
    #option = task_parent.add_subparsers(dest='option')
    #option.add_parser("executor"   , parents = executor_parser()    ,help='',formatter_class=formatter_class)
    #mode.add_parser( "job", parents=[job_parent], help="",formatter_class=formatter_class)

    return parser

def run_parser(args):
    if args.mode == "run":
        if args.option == "app":
            from maestro.app import run
            run(args)
        elif args.option == "job":
            from maestro.loop.job import run
            run(args)
      

def run():

    parser = build_argparser()
    if len(sys.argv)==1:
        print(parser.print_help())
        sys.exit(1)

    args = parser.parse_args()
    run_parser(args)



if __name__ == "__main__":
  run()
