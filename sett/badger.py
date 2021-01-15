# Copyright (C) 2015 SINTEF ICT,
# Applied Mathematics, Norway.
#
# Contact information:
# E-mail: eivind.fonn@sintef.no
# SINTEF ICT, Department of Applied Mathematics,
# P.O. Box 4760 Sluppen,
# 7045 Trondheim, Norway.
#
# This file is part of BADGER.
#
# BADGER is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# BADGER is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public
# License along with BADGER. If not, see
# <http://www.gnu.org/licenses/>.
#
# In accordance with Section 7(b) of the GNU Affero General Public
# License, a covered work must retain the producer line in every data
# file that is created or manipulated using BADGER.
#
# Other Usage
# You can be released from the requirements of the license by purchasing
# a commercial license. Buying such a license is mandatory as soon as you
# develop commercial activities involving BADGER without disclosing the
# source code of your own applications.
#
# This file may be used in accordance with the terms contained in a
# written agreement between you and SINTEF ICT.

import operator
import shlex
import shutil
import subprocess
import tempfile

from functools import reduce
from socket import gethostname
from datetime import datetime
from itertools import chain, product
from os.path import join, dirname
from operator import methodcaller
from jinja2 import Environment, FileSystemLoader

from sett import output, input, log
from sett.utils import *


def interpolate_vars(string, namespace):
    for name, value in namespace.items():
        string = string.replace('$' + name, str(value))
    return string


def build_initial_namespace(setup, parameters):
    namespace = dict(zip(setup.parameters, parameters))
    for name, expr in setup.dependencies.items():
        try:
            namespace[name] = eval(expr, {}, namespace)
        except (TypeError, SyntaxError, NameError):
            namespace[name] = expr
    return namespace


def render_files(templates, namespace):
    env = Environment(loader=FileSystemLoader(searchpath='.'))
    ret = {}
    for fn in templates:
        template = env.get_template(fn)
        ret[fn] = template.render(**namespace)
    return ret


def render_templates(templates, namespace):
    return list(map(methodcaller('render', **namespace), templates))


def work(args, setup):
    results = []
    output_root = dirname(args.output)
    num_cases = reduce(operator.mul, [len(vals) for vals in setup.parameters.values()], 1)
    num = 1

    for tp in product(*(l for _, l in setup.parameters.items())):
        namespace = build_initial_namespace(setup, tp)
        namespace['case_number'] = num

        templates = render_templates(setup.templates, namespace)
        template_data = render_files(templates, namespace)
        copy_files = render_templates(setup.files, namespace)

        if num_cases > 1:
            target_dir = join(output_root, setup.target_dir.render(**namespace))
        else:
            target_dir = output_root
        ensure_path_exists(target_dir, file=False)

        with tempfile.TemporaryDirectory() as path:
            for fn in chain(template_data, copy_files):
                ensure_path_exists(join(path, fn))
            for fn, data in template_data.items():
                with open(join(path, fn), 'w') as f:
                    f.write(data)
            for fn in copy_files:
                shutil.copy(fn, join(path, fn))

            log.log('running', 'Running ' + ', '.join('{}={}'.format(var, namespace[var])
                                                      for var in setup.parameters) + ' ...')
            for fn, data in template_data.items():
                log.log('templates', data, 'Template: {}'.format(fn))

            result = {}
            for cmd in setup.commands:
                cmdargs = render_templates(cmd.args, namespace)
                if args.dry:
                    log.log('results', ' '.join(shlex.quote(a) for a in cmdargs))
                else:
                    stdout, stderr, retcode = run_process(cmdargs, path)
                    cmd.capture_files(path, target_dir, namespace)
                    result.update(cmd.capture_stdout(stdout, namespace))
                    log.log('stdout', stdout, 'Captured stdout')
                    log.log('stderr', stderr, 'Captured stderr')
                    if retcode != 0:
                        log.log('retcode', '!! Process exited with code {}'.format(retcode))
            coerce_types(result, setup.types)
            results.append(result)
            log.log('results', ', '.join('{}={}'.format(t, result[t]) for t in sorted(result)))

        num += 1

    if not args.dry:
        all_output = set().union(*results)
        for out in all_output:
            if out not in setup.types:
                setup.types['out'] = 'str'

        return {
            'metadata': {
                'hostname': gethostname(),
                'time': str(datetime.now()),
            },
            'parameters': [{'name': param, 'values': values}
                           for param, values in setup.parameters.items()],
            'results': {output: [result.get(output) for result in results]
                        for output in all_output},
        }
