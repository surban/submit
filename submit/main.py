from argparse import ArgumentParser
import sys
from jobsubmitter import JobSubmitter, SubmissionError
from ConfigParser import ConfigParser
import os.path

def arg_split(args, sep):
    return [f for f in args.split(sep) if len(f) > 0]

def run():
    mydir = os.path.dirname(__file__)
    cfg_parser = ConfigParser({"script": os.path.join(mydir, "job-script.sh"),
                               "cfg-files": "cfg.py",
                               "input-files": "",
                               "output-files": "output.txt",
                               "slurm-log": "output.txt",
                               "slurm-options": "",
                               })
    cfg_parser.read("submit.cfg")

    arg_parser = ArgumentParser(description="Submit SLURM jobs.")
    arg_parser.add_argument("directories", metavar="directory", nargs='+',
                            help="directories to submit")
    arg_parser.add_argument("--script", "-s", default=cfg_parser.get("DEFAULT", "script"),
                            help="script to submit to SLURM using sbatch "
                                 "(runner will be passed as first argument and directory name will be"
                                 " passed as second argument)")
    arg_parser.add_argument("--runner", "-r", default=None,
                            help="program that runs the configuration "
                                 "(if none is specified then it must be specified in the configuration file using the"
                                 " marker RUNNER: )")
    arg_parser.add_argument("--update", "-u", action='store_true',
                            help="only submit job if input is more recent than output")
    arg_parser.add_argument("--cfg-files", "-c", default=cfg_parser.get("DEFAULT", "cfg-files"),
                            help="configuration files for each job")
    arg_parser.add_argument("--input-files", "-i", default=cfg_parser.get("DEFAULT", "input-files"),
                            help="input files for each job")
    arg_parser.add_argument("--output-files", "-o", default=cfg_parser.get("DEFAULT", "output-files"),
                            help="output files for each job")
    arg_parser.add_argument("--slurm-log", "-l", default=cfg_parser.get("DEFAULT", "slurm-log"),
                            help="file to redirect standard output and error to")
    arg_parser.add_argument("--slurm-options", "-O", default=cfg_parser.get("DEFAULT", "slurm-options"),
                            help="comma seperated options (without --) that should be passed to sbatch")
    args = arg_parser.parse_args()

    slurm_options = ["--" + o for o in arg_split(args.slurm_options, ",")]

    try:
        js = JobSubmitter(script=args.script,
                          runner=args.runner,
                          cfg_files=arg_split(args.cfg_files, ","),
                          input_files=arg_split(args.input_files, ","),
                          output_files=arg_split(args.output_files, ","),
                          update=args.update,
                          slurm_log=args.slurm_log,
                          slurm_options=slurm_options)
    except SubmissionError as e:
        print e.message
        sys.exit(1)

    for directory in args.directories:
        print "%20s: " % directory,
        try:
            sys.stdout.flush()
            js.submit_directory(directory)
            print "\r",
            sys.stdout.flush()
            #print "submitted"
        except SubmissionError as e:
            print e.message


if __name__ == '__main__':
    run()

