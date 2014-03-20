import glob
import os
import os.path
import sys
import logging
import re

from argparse import ArgumentParser
from ConfigParser import ConfigParser
import subprocess

log = None


def arg_split(args, sep=","):
    return [f.strip() for f in args.split(sep) if len(f) > 0]


class SubmissionError(Exception):
    pass


class Configuration(object):
    def __init__(self, cfg_path):
        self.cfg_path = cfg_path

        self.runner = None
        self.input_files = []
        self.log_file = "output.txt"
        self.gpu = 'no'
        self.slurm_options = None
        self.slurm_options_gpu = None
        self.slurm_options_cpu = None

        self.parse()
        self.parse_slurm_options()

    def parse(self):
        with open(self.cfg_path, 'r') as f:
            for line in f.readlines():
                line = line.strip()
                m = re.match(r"^#\s*SUBMIT:\s*([a-zA-Z\-]+)\s*=\s*(.*)$", line)
                if m:
                    name = m.group(1).lower()
                    value = m.group(2).strip()

                    if name == "runner":
                        self.runner = value
                    elif name == "input-files":
                        self.input_files = arg_split(value)
                    elif name == "log-file":
                        self.log_file = value
                    elif name == "gpu":
                        value = value.lower()
                        if value not in ['yes', 'prefer', 'no']:
                            raise SubmissionError("GPU option %s not recognized" % value)
                        self.gpu = value
                    else:
                        raise SubmissionError("submit option %s in config file not recognized" % name)
        if not self.runner:
            raise SubmissionError("required option runner was not specified in configuration file")

    def parse_slurm_options(self):
        """Reads SBATCH options from the specified configuration file."""
        self.slurm_options = []
        self.slurm_options_gpu = []
        self.slurm_options_cpu = []
        with open(self.cfg_path, 'r') as f:
            for line in f.readlines():
                m = re.match(r"^#\s*SBATCH GPU (.*)$", line)
                if m is not None:
                    self.slurm_options_gpu.append(m.group(1).strip())
                    continue

                m = re.match(r"^#\s*SBATCH CPU (.*)$", line)
                if m is not None:
                    self.slurm_options_cpu.append(m.group(1).strip())
                    continue

                m = re.match(r"^#\s*SBATCH (.*)$", line)
                if m is not None:
                    self.slurm_options.append(m.group(1).strip())


class JobSubmitter(object):
    def __init__(self, script=None, cfg_files=[], update=False, retry_failed=False,
                 log_file=None, slurm_options=None, gpu=None, prolog=None, options=[]):
        self.script = script
        self.cfg_files = cfg_files
        self.update = update
        self.retry_failed = retry_failed
        self.log_file = log_file
        self.slurm_options = slurm_options
        self.gpu = gpu
        self.prolog = prolog
        self.options = options

        log.debug("Using job batch script %s" % self.script)
        log.debug("Using slurm options: %s" % str(self.slurm_options))

    def find_config_file(self, directory):
        """Returns the first configuration file in the given directory."""
        for filename in self.cfg_files:
            cfg_path = os.path.join(directory, filename)
            if os.path.exists(cfg_path):
                return cfg_path, filename
        return None, None

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

    def submit_job(self, job_name, log_path, opts, runner, cfg, device, xpu_twin):
        """Submits to SLURM using sbatch and returns the job id."""
        cmd = ["sbatch", "--job-name=" + job_name, "--output=" + log_path]
        cmd += opts
        cmd += [self.script, runner, cfg, device, str(xpu_twin), self.prolog]
        cmd += self.options

        log.debug("Executing: " + " ".join(cmd))
        try:
            output = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            raise SubmissionError("sbatch failed")

        m = re.search("Submitted batch job (\d+)", output)
        if m:
            return int(m.group(1))
        else:
            return SubmissionError("sbatch failed with unexpected output: " + output)

    def cancel_job(self, job_id):
        """Cancels the SLURM job with the given id."""
        retval = subprocess.check_call(["scancel", str(job_id)])
        if retval != 0:
            raise SubmissionError("scancel failed for job %d" % job_id)

    def release_job(self, job_id):
        """Releases the held SLURM job with the given id."""
        retval = subprocess.check_call(["scontrol", "release", str(job_id)])
        if retval != 0:
            raise SubmissionError("scontrol release failed for job %d" % job_id)

    def submit_directory(self, directory):
        """Submits the given directory to the job scheduler."""
        if not os.path.isdir(directory):
            raise SubmissionError("specified path is not a directory")

        cfg_path, cfg_filename = self.find_config_file(directory)
        if not cfg_path:
            raise SubmissionError("no config file found for directory %s" % directory)
        log.debug("Using configuration file %s" % cfg_path)
        cfg = Configuration(cfg_path)
        log.debug("parsed configuration: {" +
                  ", ".join("%s: %s" % item for item in vars(cfg).items()) + "}")

        # log file
        if self.log_file:
            log_file = self.log_file
        else:
            log_file = cfg.log_file
        log_path = os.path.join(directory, log_file)

        # input files
        input_files = cfg.input_files[:]
        input_files.append(cfg_filename)
        self.check_file_existance(directory, input_files)

        # check modification times if update is specified
        if self.update:
            input_mtimes = self.get_mtimes(directory, input_files, ignore_missing=False)
            most_recent_input = max(input_mtimes)

            fail_filename = os.path.join(directory, "_failed")
            if (os.path.exists(fail_filename) and os.path.getmtime(fail_filename) >= most_recent_input and
                    not self.retry_failed):
                raise SubmissionError("failed previously")

            finished_filename = os.path.join(directory, "_finished")
            if os.path.exists(finished_filename) and os.path.getmtime(finished_filename) >= most_recent_input:
                raise SubmissionError("current")

        # slurm options
        slurm_options = self.slurm_options[:]
        slurm_options += cfg.slurm_options
        cpu_options = slurm_options[:] + cfg.slurm_options_cpu
        gpu_options = slurm_options[:] + cfg.slurm_options_gpu
        gpu_options.append("--gres=gpu")

        # gpu
        if self.gpu:
            gpu = self.gpu
        else:
            gpu = cfg.gpu

        # remove previous log
        if os.path.exists(log_path):
            os.unlink(log_path)

        gpu_job_id = None
        cpu_job_id = None

        if gpu == 'yes':
            gpu_job_id = self.submit_job(directory, log_path, gpu_options, cfg.runner, directory, 'gpu', 0)
        elif gpu == 'no':
            cpu_job_id = self.submit_job(directory, log_path, cpu_options, cfg.runner, directory, 'cpu', 0)
        elif gpu == 'prefer':
            cpu_options.append("--hold")
            gpu_options.append("--hold")

            while True:
                # submit dummy job to get current slurm job id
                dummy_id = self.submit_job(directory + "_dummy", log_path, cpu_options, cfg.runner, directory, 'cpu', 0)
                gpu_job_id = dummy_id + 1
                cpu_job_id = dummy_id + 2
                log.debug("Dummy job id is %d, expecting gpu jobid=%d and cpu jobid=%d" %
                          (dummy_id, gpu_job_id, cpu_job_id))

                gpu_job_id_real = self.submit_job(directory + "(gpu)", log_path, gpu_options,
                                                  cfg.runner, directory, 'gpu', cpu_job_id)
                cpu_job_id_real = self.submit_job(directory + "(cpu)", log_path, cpu_options,
                                                  cfg.runner, directory, 'cpu', gpu_job_id)
                log.debug("Got gpu jobid=%d and cpu jobid=%d" %
                          (gpu_job_id_real, cpu_job_id_real))

                self.cancel_job(dummy_id)
                if gpu_job_id == gpu_job_id_real and cpu_job_id == cpu_job_id_real:
                    # job id okay. cancel dummy and release real jobs.
                    log.debug("Got expected jobids.")
                    self.release_job(gpu_job_id)
                    self.release_job(cpu_job_id)
                    break
                else:
                    # did not get expected job ids. cancel and retry.
                    log.debug("Did not get expected jobids. Retrying...")
                    self.cancel_job(gpu_job_id_real)
                    self.cancel_job(cpu_job_id_real)

        return cpu_job_id, gpu_job_id


def run():
    global log

    mydir = os.path.dirname(__file__)
    cfg_parser = ConfigParser({"cfg-files": "cfg.py",
                               "slurm-options": "",
                               "prolog": os.path.join(mydir, "prolog.sh"),
                               })
    cfg_parser.read("submit.cfg")

    arg_parser = ArgumentParser(description="Submit SLURM jobs.")
    arg_parser.add_argument("directories", metavar="directory", nargs='+',
                            help="directories to submit")
    arg_parser.add_argument("--update", "-u", action='store_true',
                            help="only submit job if input is more recent than output")
    arg_parser.add_argument("--retry-failed", "-r", action='store_true',
                            help="retry previously failed, unchanged configurations if --update is specified")
    arg_parser.add_argument("--cfg-files", "-c", default=cfg_parser.get("DEFAULT", "cfg-files"),
                            help="configuration files for each job")
    arg_parser.add_argument("--log-file", "-l",
                            help="file to redirect standard output and error to. "
                                 "Overrides the value specified in the job configuration file.")
    arg_parser.add_argument("--slurm-option", "-O",
                            default=arg_split(cfg_parser.get("DEFAULT", "slurm-options"), ","),
                            action='append',
                            help="option (without --) that should be passed to sbatch "
                                 "(specify multiple times for more than one option)")
    arg_parser.add_argument("--gpu", "-g",
                            default=None, choices=["yes", "prefer", "no"],
                            help="GPU access. "
                                 "Specify 'yes' if the job requires a GPU to run. "
                                 "Specify 'prefer' if the gpu can make use of a GPU but does not require it to run. "
                                 "Specify 'no' if no GPU should be allocated. "
                                 "Overrides the value specified in the job configuration file.")
    arg_parser.add_argument("--prolog",
                            default=cfg_parser.get("DEFAULT", "prolog"),
                            help="script that should be sourced before executing the task. "
                                 "Specify 'none' for no prolog script.")
    arg_parser.add_argument("--option", "-o", default=[], action='append',
                            help="additional argument that should be passed to the runner "
                                 "(specify multiple times for more than one option)")
    arg_parser.add_argument("--debug", action='store_true',
                            help="displays debug output")
    args = arg_parser.parse_args()

    # initialize logging
    logging.basicConfig()
    log = logging.getLogger("jobsubmitter")
    if args.debug:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.WARNING)

    slurm_options = ["--" + o for o in args.slurm_option]
    prolog=args.prolog
    if prolog and prolog == "none":
        prolog = None

    try:
        js = JobSubmitter(script=os.path.join(mydir, "job-script.sh"),
                          cfg_files=arg_split(args.cfg_files, ","),
                          update=args.update,
                          retry_failed=args.retry_failed,
                          log_file=args.log_file,
                          slurm_options=slurm_options,
                          gpu=args.gpu,
                          prolog=prolog,
                          options=args.option)
    except SubmissionError as e:
        print e.message
        sys.exit(1)

    for directory in args.directories:
        print "%20s: " % directory,
        if args.debug:
            print
        sys.stdout.flush()
        try:
            cpu_id, gpu_id = js.submit_directory(directory)
            if cpu_id and gpu_id:
                print "cpu: %5d   gpu: %5d" % (cpu_id, gpu_id)
            elif cpu_id:
                print "%5d" % cpu_id
            else:
                print "%5d" % gpu_id
        except SubmissionError as e:
            print e.message


if __name__ == '__main__':
    run()
