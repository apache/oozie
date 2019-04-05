#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import sys
import getopt
import re
from datetime import datetime


def main(argv):
    inputfile = ''
    outputfile = ''
    parameters = []

    try:
        opts, args = getopt.getopt(argv, "hi:o:p:",["input=", "output=", "parameters="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-i", "--input"):
            inputfile = arg
        elif opt in ("-o", "--output"):
            outputfile = arg
        elif opt in ("-p", "--parameters"):
            parameters = arg.split(',')

    if inputfile == '' or outputfile == '' or parameters == []:
        error('Arguments not set correctly.')
        usage()
        sys.exit(1)

    print 'Input file is: ', inputfile
    print 'Output file is: ', outputfile
    print 'Paremeters are: ', parameters

    parse_instrumentation_log(inputfile, outputfile, parameters)


def usage():
    print 'USAGE: python instrumentation-log-parser.py -i <inputfile> -o <outputfile> -p <path/to/json/value1,path/to/json/value2>'


def info(message):
    log('INFO', message)


def debug(message):
    log('DEBUG', message)


def error(message):
    log('ERROR', message)


def warn(message):
    log('WARN', message)


def log(level, message):
    print '[%s] [%s] %s' % (datetime.now(), level, message)


def parse_instrumentation_log(inputfile, outputfile, parameters):
    info('Parsing instrumentation log. [inputfile=%s;outputfile=%s;parameters=%s]' % (inputfile, outputfile, parameters))

    with open(inputfile) as i:
        content = i.readlines()

    debug('Input file has %d lines' % (len(content)))

    header = re.compile('^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}).*$')
    json_begin = re.compile('^\{$')
    json_end = re.compile('^\}$')

    json_content = ''
    timestamp = ''
    output_line_count = 0

    with open(outputfile, 'w+') as o:
        o.write('Timestamp;%s\n' % (';'.join(parameters)))
        input_line_count = 0
        for line in content:
            if header.match(line) != None:
                timestamp = header.match(line).group(1)
            elif json_begin.match(line):
                json_content = json_content + line
            elif json_end.match(line):
                json_content = json_content + line
                try:
                    json_object = json.loads(json_content)
                    output_line = timestamp
                    for parameter in parameters:
                        json_fragment = json_object
                        for parameter_key in parameter.split('/'):
                            json_fragment = json_fragment[parameter_key]
                        output_line = output_line + ';' + str(json_fragment)
                    output_line_count = output_line_count + 1
                    o.write(output_line + '\n')
                except ValueError:
                    warn('Unparseable JSON input at line %d' % input_line_count)
                json_content = ''
            else:
                json_content = json_content + line
            input_line_count = input_line_count + 1
            if input_line_count % 1000000 == 0:
                debug('%d input lines processed, %d output lines written' % (input_line_count, output_line_count))

    info('Parsing instrumentation log finished. In total %d input lines processed, %d output lines written'
         % (input_line_count, output_line_count))


if __name__ == "__main__":
    main(sys.argv[1:])
