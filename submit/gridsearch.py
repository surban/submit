from argparse import ArgumentParser
import logging
import os
import glob
import numpy as np
import re
import shutil
from warnings import warn

log = logging.getLogger("gridsearch")
logging.basicConfig(level=logging.DEBUG)


class GridSearchError(Exception):
    pass


class GridSearch(object):
    predefined_parameters = ["CFG_INDEX"]

    def __init__(self, name, template, parameter_ranges):
        self._name = name
        self._template = template
        self._parameter_ranges = self._parse_parameters(parameter_ranges)

        log.debug("parsed parameters: %s" % str(self._parameter_ranges))

        self._check_parameters()

    def _parse_parameters(self, para_strs):
        parameters = {}
        for p, rng_spec in para_strs.iteritems():
            try:
                if isinstance(rng_spec, basestring):
                    val = self._parse_rng_str(rng_spec)
                else:
                    val = []
                    for e in rng_spec:
                        if isinstance(e, basestring):
                            val.extend(self._parse_rng_str(rng_spec))
                        else:
                            val.append(e)
                parameters[p.upper()] = val
            except ValueError as e:
                log.debug("inner exception:" + str(e))
                raise GridSearchError("could not parse parameter %s: %s" % (p, e.message))
        return parameters

    def _parse_value_str(self, value_str):
        values = []
        for rng_str in value_str.split(","):
            values.extend(self._parse_rng_str(rng_str))
        return values

    def _parse_rng_str(self, rng_str):
        if ":" in rng_str:
            rng_parts = rng_str.split(":")
            if len(rng_parts) == 3:
                start = float(rng_parts[0])
                step = float(rng_parts[1])
                end = float(rng_parts[2])
            elif len(rng_parts) == 2:
                start = float(rng_parts[0])
                step = 1
                end = float(rng_parts[1])
            else:
                raise ValueError("range specification %s is not recognized" % rng_str)
            logging.debug("Range string %s parsed as: start=%g step=%g end=%g" %
                          (rng_str, start, step, end))
            return np.arange(start, end + step/100., step)
        else:
            try:
                return [float(rng_str)]
            except ValueError:
                return [rng_str]

    def _get_used_parameters(self):
        params = []
        for m in re.finditer(r"\$(\w+)\$", self._template + " " + self._name):
            params.append(m.group(1).upper())
        return set(params)

    def _check_parameters(self):
        used_params = self._get_used_parameters()
        used_params |= set(self.predefined_parameters)
        specified_params = set(self._parameter_ranges.keys())
        specified_params |= set(self.predefined_parameters)
        used_but_not_specified = used_params - specified_params
        if used_but_not_specified:
            raise GridSearch("parameter(s) %s used in template but no range was specified" % str(used_but_not_specified))
        specified_but_not_used = specified_params - used_params
        if specified_but_not_used:
            warn("parameter(s) %s specified but not used in template" % str(specified_but_not_used))

    def _instantiate(self, template, parameters):
        inst = template
        rpl_tag = "###REPLACEMENT_TAG###"
        for p, val in parameters.iteritems():
            inst = re.sub(r"\$%s\$" % re.escape(p), rpl_tag, inst, flags=re.IGNORECASE)
            inst = inst.replace(rpl_tag, str(val))
        return inst

    def _generate_rec(self, p_rest):
        if p_rest:
            p = p_rest[0]
            for val in self._parameter_ranges[p]:
                for rest in self._generate_rec(p_rest[1:]):
                    p_vals = {p: val}
                    p_vals.update(rest)
                    yield p_vals
        else:
            yield {}

    def generate(self):
        plist = self._parameter_ranges.keys()
        cfg_index = 0

        for p_vals in self._generate_rec(plist):
            cfg_index += 1
            p_vals["CFG_INDEX"] = "%03d" % cfg_index

            name = self._instantiate(self._name, p_vals)
            data = self._instantiate(self._template, p_vals)

            dirname, filename = os.path.split(name)
            try:
                os.makedirs(dirname)
            except:
                pass
            with open(name, 'w') as f:
                f.write(data)


def gridsearch(name, template, parameter_ranges):
    GridSearch(name, template, parameter_ranges).generate()


def remove_index_dirs():
    """Deletes all subfolders of the current directory whose name is an integer number."""
    for filename in glob.glob("*"):
        if filename == ".." or filename == ".":
            continue
        if os.path.isdir(filename):
            try:
                int(filename)
            except ValueError:
                continue

            shutil.rmtree(filename)
