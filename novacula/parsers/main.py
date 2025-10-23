#!/usr/bin/env python

import sys
import argparse

from novacula   import get_argparser_formatter
from .job       import args_parser as job_parser
from .task      import args_parser as task_parser


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