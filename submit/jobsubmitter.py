import glob
import os
import os.path
import sys
import logging
import re

from argparse import ArgumentParser
from ConfigParser import ConfigParser

# logging
log = logging.getLogger("jobsubmitter")


class SubmissionError(Exception):
    pass


class JobSubmitter(object):
    def __init__(self, script=None, runner=None, cfg_files=[], input_files=[], output_files=[],
                 update=False, slurm_log=None, slurm_options=None):
        self.script = script
        self.runner = runner
        self.cfg_files = cfg_files
        self.input_files = input_files
        self.output_files = output_files
        self.update = update
        self.slurm_log = slurm_log
        self.slurm_options = slurm_options

        log.debug("Using job batch script %s" % self.script)
        log.debug("Using slurm options: %s" % str(self.slurm_options))

    @staticmethod
    def find_submit_script():
        candidates = []
        for cand in glob.glob("*.sh") + glob.glob("../*.sh"):
            log.debug("Checking submit script candidate %s" % cand)
            if os.path.isfile(cand):
                try:
                    with open(cand, 'r') as f:
                        if "SBATCH" in f.read():
                            candidates.append(cand)
                except IOError:
                    pass

        if len(candidates) == 1:
            return candidates[0]
        elif len(candidates) == 0:
            raise SubmissionError("No submit script was specified and it could not be found automatically")
        else:
            raise SubmissionError("No submit script was specified and more than one candidates were found: "
                                  + str(candidates))

    def find_config_file(self, directory):
        """Returns the first configuration file in the given directory."""
        for filename in self.cfg_files:
            cfg_path = os.path.join(directory, filename)
            if os.path.exists(cfg_path):
                return cfg_path, filename
        return None, None

    def get_runner_from_config_file(self, cfg_path):
        """Reads the runner from the specified configuration file."""
        with open(cfg_path, 'r') as f:
            for line in f.readlines():
                m = re.search("RUNNER:(.*)$", line)
                if m is not None:
                    return m.group(1).strip()
        return None

    def get_slurm_options_from_config_file(self, cfg_path):
        """Reads SBATCH options from the specified configuration file."""
        opts = []
        with open(cfg_path, 'r') as f:
            for line in f.readlines():
                m = re.search("^#SBATCH (.*)$", line)
                if m is not None:
                    opts.append(m.group(1).strip())
        return opts

    def get_mtimes(self, directory, files, ignore_missing=False):
        """Returns an array of the modification times of the specified files."""
        mtimes = []
        for file in files:
            path = os.path.join(directory, file)
            if ignore_missing and not os.path.isfile(path):
                continue
            else:
                mtimes.append(os.path.getmtime(path))
        return mtimes

    def check_file_existance(self, directory, files):
        """Checks that all specified files exist and raises and exception otherwise."""
        for file in files:
            path = os.path.join(directory, file)
            if not os.path.isfile(path):
                raise SubmissionError("file %s is missing" % path)

    def submit_directory(self, directory):
        """Submits the given directory to the job scheduler."""
        if not os.path.isdir(directory):
            raise SubmissionError("specified path is not a directory")

        cfg_path, cfg_filename = self.find_config_file(directory)
        log.debug("Using configuration file %s" % cfg_path)

        if self.runner is not None:
            runner = self.runner
        else:
            if cfg_path is not None:
                runner = self.get_runner_from_config_file(cfg_path)
                if runner is None:
                    raise SubmissionError("no runner specified on command line or in configuration file %s" %
                                          cfg_path)
            else:
                raise SubmissionError("no runner specified and no configuration file was found")
        log.debug("Using runner %s" % runner)

        input_files = self.input_files[:]
        if cfg_path is not None:
            input_files.append(cfg_filename)
        self.check_file_existance(directory, input_files)

        input_mtimes = self.get_mtimes(directory, input_files, ignore_missing=False)
        output_mtimes = self.get_mtimes(directory, self.output_files, ignore_missing=True)
        if len(input_mtimes) > 0 and len(output_mtimes) > 0:
            if self.update and min(output_mtimes) > max(input_mtimes):
                raise SubmissionError("output is up to date")

        sbatch_opts = self.slurm_options[:]
        if cfg_path is not None:
            sbatch_opts += self.get_slurm_options_from_config_file(cfg_path)
        opts_str = " ".join(["\"" + opt + "\"" for opt in sbatch_opts])

        out_path = os.path.join(directory, self.slurm_log)
        if os.path.exists(out_path):
            os.unlink(out_path)

        cmdline = "sbatch --quiet \"--job-name=%s\" \"--output=%s\" %s \"%s\" \"%s\" \"%s\"" % \
                  (directory, out_path, opts_str, self.script, runner, directory)
        log.debug("Executing: " + cmdline)
        os.system(cmdline)


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
    arg_parser.add_argument("--debug", action='store_true',
                            help="displays debug output")
    args = arg_parser.parse_args()

    if args.debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.WARNING
    logging.basicConfig(level=log_level)

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
