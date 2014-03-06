from argparse import ArgumentParser
import sys
from jobsubmitter import JobSubmitter, SubmissionError
from ConfigParser import ConfigParser


def run():
    cfg_parser = ConfigParser({"script": None,
                               "input-files": "",
                               "output-files": "slurm.out",
                               "slurm-log": "slurm.out"})
    cfg_parser.read("submit.cfg")

    arg_parser = ArgumentParser(description="Submit SLURM jobs.")
    arg_parser.add_argument("directories", metavar="directory", nargs='+',
                            help="directories to submit")
    arg_parser.add_argument("--script", "-s", default=cfg_parser.get("DEFAULT", "script"),
                            help="script to submit to SLURM using sbatch (directory name will be "
                                 "passed as first argument)")
    arg_parser.add_argument("--update", "-u", action='store_true',
                            help="only submit job if input is more recent than output")
    arg_parser.add_argument("--input-files", "-i", default=cfg_parser.get("DEFAULT", "input-files"),
                            help="input files for each job")
    arg_parser.add_argument("--output-files", "-o", default=cfg_parser.get("DEFAULT", "output-files"),
                            help="output files for each job")
    arg_parser.add_argument("--slurm-log", "-l", default=cfg_parser.get("DEFAULT", "slurm-log"),
                            help="file to redirect standard output and error to")
    args = arg_parser.parse_args()

    input_files = [f for f in args.input_files.split(",") if len(f) > 0]
    output_files = [f for f in args.output_files.split(",") if len(f) > 0]

    try:
        js = JobSubmitter(submit_script=args.script,
                          input_files=input_files,
                          output_files=output_files,
                          only_older=args.update)
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

