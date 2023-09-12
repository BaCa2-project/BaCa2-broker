#!/usr/bin/env python3
# vim:ts=4:sts=4:sw=4:expandtab
import os, sys
if __name__ == '__main__':
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'kolejka-judge'))
    from kolejka.judge import main
    main(__file__)
from kolejka.judge.commands import *
from kolejka.judge.parse import *
from kolejka.judge.tasks import *

def judge(args):
    tool_time = parse_time('60s')
    prepare_time = parse_time('5s')
    source_size_limit = parse_memory(args.test.get('source_size', '100K'))
    binary_size_limit = parse_memory(args.test.get('binary_size', '10M'))
    compile_time = parse_time(args.test.get('compile_time', '10s'))
    compile_memory = parse_memory(args.test.get('compile_memory', '1G'))
    c_standard = args.test.get('c_standard', 'c11')
    cpp_standard = args.test.get('cpp_standard', 'c++17')
    gcc_arguments = [ arg.strip() for arg in args.test.get('gcc_arguments', '').split() if arg.strip() ]
    time_limit = parse_time(args.test.get('time', '10s'))
    memory_limit = parse_memory(args.test.get('memory', '1G'))
    output_size_limit = parse_memory(args.test.get('output_size', '1G'))
    error_size_limit  = parse_memory(args.test.get('error_size', '1M'))
    basename = args.test.get('basename', None)
    args.add_steps(
        system=SystemPrepareTask(default_logs=False),
        source=SolutionPrepareTask(source=args.solution, basename=basename, allow_extract=True, override=args.test.get('environment', None), limit_real_time=prepare_time),
        source_rules=SolutionSourceRulesTask(max_size=source_size_limit),
        builder=SolutionBuildAutoTask([
            [SolutionBuildCMakeTask, [], {}],
            [SolutionBuildMakeTask, [], {}],
            [SolutionBuildGXXTask, [], {'standard': cpp_standard, 'build_arguments': gcc_arguments}],
            [SolutionBuildGCCTask, [], {'standard': c_standard, 'build_arguments': gcc_arguments, 'libraries': ['m']}],
            [SolutionBuildPython3ScriptTask, [], {}],
        ], limit_real_time=compile_time, limit_memory=compile_memory),
        build_rules=SolutionBuildRulesTask(max_size=binary_size_limit),
    )
    args.add_steps(io=SingleIOTask(
        input_path=args.test.get('input', None),
        tool_override=args.test.get('tools', None),
        tool_time=tool_time,
        tool_c_standard=c_standard,
        tool_cpp_standard=cpp_standard,
        tool_gcc_arguments=gcc_arguments,
        generator_source=args.test.get('generator', None),
        verifier_source=args.test.get('verifier', None),
        hint_path=args.test.get('hint', None),
        hinter_source=args.test.get('hinter', None),
        checker_source=args.test.get('checker', None),
        limit_cores=1,
        limit_time=time_limit,
        limit_memory=memory_limit,
        limit_output_size=output_size_limit,
        limit_error_size=error_size_limit,
        )
    )
    if parse_bool(args.test.get('debug', 'no')):
        args.add_steps(debug=CollectDebugTask())
    args.add_steps(logs=CollectLogsTask())
    result = args.run()
    print('Result {} on test {}.'.format(result.status, args.id))
