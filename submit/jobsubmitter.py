import glob
import os
import sys
import logging

# logging
log_level = logging.DEBUG
#log_level = logging.WARNING
logging.basicConfig(level=log_level)
log = logging.getLogger("jobsubmitter")


class SubmissionError(Exception):
    pass


class JobSubmitter(object):
    def __init__(self, submit_script=None, input_files=[], output_files=[], only_older=False,
                 slurm_log="slurm.out"):
        if submit_script is not None:
            self.submit_script = submit_script
        else:
            self.submit_script = self.find_submit_script()
        log.debug("Using submit script %s" % self.submit_script)

        self.input_files = input_files
        self.output_files = output_files
        self.only_older = only_older
        self.slurm_log = slurm_log

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

    def submit_directory(self, directory):
        if not os.path.isdir(directory):
            raise SubmissionError("specified path is not a directory")

        input_mtimes = []
        for input_file in self.input_files:
            input_filename = os.path.join(directory, input_file)
            if not os.path.isfile(input_filename):
                raise SubmissionError("input file %s missing" % input_file)
            else:
                input_mtimes.append(os.path.getmtime(input_filename))

        output_mtimes = []
        for output_file in self.output_files:
            output_filename = os.path.join(directory, output_file)
            if os.path.isfile(output_filename):
                output_mtimes.append(os.path.getmtime(output_filename))

        if len(input_mtimes) > 0 and len(output_mtimes) > 0:
            if self.only_older and min(output_mtimes) > max(input_mtimes):
                raise SubmissionError("output is up to date")

        out_path = os.path.join(directory, self.slurm_log)
        if os.path.exists(out_path):
            os.unlink(out_path)

        os.system("sbatch --quiet \"--job-name=%s\" \"--output=%s\" \"%s\" \"%s\"" %
                  (directory, out_path, self.submit_script, directory))


