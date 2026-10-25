#!/usr/bin/env python3
"""Name the local command that reproduces a failed CI full job.

CI full is qualification, not a test loop. A failed job ends with the command
to run locally until it passes, in its log and in the run summary.
"""
import argparse
import os


def annotation(text):
    return text.replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', help='the shell command line that reproduces this job')
    args = parser.parse_args()
    job = os.environ.get('GITHUB_JOB', 'this job')
    print(f'::error title=Reproduce {job} locally::{annotation(args.command)}', flush=True)
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary, 'a') as out:
            out.write(f'### Reproduce `{job}` locally\n\nRun this until it passes before dispatching '
                      f'CI full again:\n\n```bash\n{args.command}\n```\n\n')


if __name__ == '__main__':
    main()
