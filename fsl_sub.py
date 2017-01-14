#!/usr/bin/env python

from __future__ import print_function
import os
import sys
import pwd
import socket
import logging
import argparse
import subprocess

# Global logger. Initialized in init_logging.
logger = logging.getLogger(__name__)


def init_logging(verbose=False):
    '''Configures a console logger with log level based on verbosity'''
    console = logging.StreamHandler()
    formatter = logging.Formatter
    console.setFormatter(formatter("%(levelname)s: %(message)s"))
    console.setLevel(logging.DEBUG)
    logger.addHandler(console)
    # TODO: support extra verbosity levels? e.g. -v -v -v
    if verbose:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.WARNING)

def main():
    epilog = '''
Queues:

There are several batch queues configured on the cluster, each with defined CPU
time limits. All queues, except bigmem.q, have a 8GB memory limit.

veryshort.q:This queue is for jobs which last under 30mins.
short.q:    This queue is for jobs which last up to 4h.
long.q:     This queue is for jobs which last less than 24h. Jobs run with a
            nice value of 10.
verylong.q: This queue is for jobs which will take longer than 24h CPU time.
            There is one slot per node, and jobs on this queue have a nice value
            of 15.
bigmem.q:   This queue is like the verylong.q but has no memory limits.
'''

    description='''
%(prog)s V1.1 - wrapper for job control system such as SGE

%(prog)s gzip *.img *.hdr
%(prog)s -q short.q gzip *.img *.hdr
%(prog)s -a darwin regscript rawdata outputdir ...
'''

    default_mailto = '{}@mail.nih.gov'.format(get_username())

    parser = argparse.ArgumentParser(description=description, epilog=epilog,
            formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('command', nargs='?', help='FSL command')
    parser.add_argument('arg', nargs='*', help='FSL command')

    parser.add_argument('-T', metavar='minutes', help='Estimated job length in minutes, used to auto-set queue name')
    parser.add_argument('-q', metavar='queuename', help='Possible values for <queuename> are "verylong.q", "long.q" and "short.q". See below for details Default is "long.q".')
    parser.add_argument('-a', metavar='arch-name', help='Architecture [e.g., darwin or lx24-amd64]')
    parser.add_argument('-p', metavar='job-priority', help='Lower priority [0:-1024] default = 0')
    parser.add_argument('-M', '--mailto', metavar='email-address', help='Who to email, default = {}'.format(default_mailto))
    parser.add_argument('-j', '--hold', metavar='jid', help='Place a hold on this task until job jid has completed')
    parser.add_argument('-t', '--taskfile', metavar='filename', help='Specify a task file of commands to execute in parallel')
    parser.add_argument('-N', '--jobname', metavar='jobname', help='Specify jobname as it will appear on queue')
    parser.add_argument('-l', '--logdir', metavar='logdir', help='Where to output logfiles')
    parser.add_argument('-m', '--mailopts', metavar='mailoptions', help='Change the SGE mail options, see qsub for details')
    parser.add_argument('-z', '--imtest', metavar='output', help='If <output> image or file already exists, do nothing and exit')
    parser.add_argument('-F', '--scriptmode', action='store_true', help='Use flags embedded in scripts to set SGE queuing options')
    parser.add_argument('-s', metavar='pename,threads', help='Submit a multi-threaded task - requires a PE (<pename>) to be configured for the requested queues. "threads" specifies the number of threads to run')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose mode.')

    parser.set_defaults(
            logdir='./',
            mailto=default_mailto,
            mailopts='a',
            scriptmode=False
    )

    args = parser.parse_args()

    # TODO:
    # - use fsl_sub error message as needed:
    # - ensure exit code of 1 on error
    #"What? Your arguments make no sense!"

    # Prepare environment for subprocesses
    env = os.environ.copy()
    if 'module' in env:
        del env['module']

    # Configure verbosity
    verbose = args.verbose or env.get('FSLSUBVERBOSE', None)
    init_logging(verbose)

    method = "SLURM"
    # do not submit batch jobs on Helix or if $NOBATCH is set
    if socket.getfqdn() == 'helix.nih.gov' or env.get('NOBATCH', None):
        method = None

    if env.get('FSLSUBALREADYRUN', None):
        method = None
        logger.warning("job on queue attempted to submit parallel jobs" +
                " - running jobs serially instead")

    env['FSLSUBALREADYRUN'] = 'true'

    qconf = 'echo'
    if method:
        qconf = which(qconf)
        if not qconf:
            logger.warn('qconf NOT FOUND')
            qconf = 'echo'

    logger.debug("Method is {}".format(method))

    ###########################################################################
    # If you have a Parallel Environment configured for OpenMP tasks then
    # the variable omp_pe should be set to the name you have defined for that
    # PE. The script will work out which queues have that PE setup on them.
    # Note, we support openmp tasks even when Grid Engine is not in use.
    ###########################################################################
    omp_pe = 'openmp'

    # -z tells us to exit early if output image already exists
    if args.imtest:
        if os.path.exists(args.z):
            os.exit(0)
        try:
            if subprocess.check_output(
                    ['{}/bin/imtest'.format(env['FSLDIR']), args.z], env) == '1':
                os.exit(0)
        except subprocess.CalledProcessError:
            pass

    # Ensure a command or taskfil is specified, but not both
    if not args.taskfile and not args.command:
        parser.error('Either supply a command to run or a parallel task file')
    elif args.taskfile and args.command:
        parser.error('You appear to have specified both a task file and a command to run')

    # Check that command exists and is executable
    command = None
    if args.command:
        if not which(args.command):
            parser.error('The command you have requested cannot be found or is not executable')
        # Add command arguments if specified
        command = [args.command]
        if args.arg:
            command += args.arg

    ###########################################################################
    # The following sets up the default queue name, which you may want to change
    ###########################################################################
    queue = 'long.q'
    queue_cmd = ' -q {}'.format(queue)
    if args.T:
        queue = qname(args.T)
    if args.q:
        queue = args.q
    if args.q or args.T:
        queue_cmd = ' -q {} '.format(queue)
        if call([qconf, '-sq', queue], env) == 1:
            parser.error('Invalid queue specified!')

    # -a sets is the cluster submission flag for controlling the required
    # hardware architecture (normally not set by the calling program)
    sge_arch = ''
    if args.a:
        available_archs = subprocess.check_output(
                "qhost | tail -n +4 | awk '{print $2}' | sort | uniq",
                env=env, shell=True).split()
        if args.a not in available_archs:
            parser.error('Sorry arch of {} is not supported on this SGE configuration!\nShould be one of: {}'.format(
                args.a, ' '.join(available_archs)))
        else:
            sge_arch = '-l arch={}'.format(args.a)

    # -p set the priority of the job - ignore this if your cluster
    # environment doesn't have priority control in this way.
    # TODO: this is unused in Biowulf fsl_sub
    if args.p:
        sge_priority = '-p args.p'

    # -j tells the cluster not to start this job until cluster job ID $jid
    # has completed. You will need this feature.
    slurm_hold = ''
    sge_hold = ''
    if args.hold:
        jid = args.hold
        slurm_hold = ' --dependency=afterany:{} '.format(jid)
        sge_hold = '-hold_jid $jid'

    # -t will pass on to the cluster software the name of a text file
    # containing a set of commands to run in parallel; one command per line.
    ntasks = 0
    if args.taskfile:
        if not os.path.isfile(args.taskfile):
            parser.error('Task file {} does not exist'.format(args.taskfile))
        with open(args.taskfile) as f:
            ntasks = sum(1 for line in f)
            if ntasks <= 0:
                parser.error('Task file {} is empty\n' +
                        'Should be a text file listing all the commands to run!'
                        .format(args.taskfile))

    # -N option determines what the command will be called when you list
    # running processes.
    jobname = None
    if args.jobname:
        #jobname = args.jobname     # SGE
        jobname = ' --job-name={}'.format(args.jobname)
    else:
        if args.taskfile:
            jobname = os.path.basename(args.taskfile)
        else:
            jobname = os.path.basename(command[0])

    # -l tells the cluster what to call the standard output and standard
    # -error logfiles for the submitted program.
    logopts = ''
    if args.logdir:
        #logopts = '-o {0} -e {0}'.format(args.logdir)  # SGE
        logopts = ' --logdir={} '.format(args.logdir)
        if not os.path.exists(args.logdir):
            os.makedirs(args.logdir)
        elif not os.path.isdir(args.logdir):
            parser.error('Log destination is a file (should be a folder)')

    # -s configures a multi-threaded task
    # Parse multithreading options
    pename, pethreads = None, None
    if args.s:
        pevalues = args.s.split(',')
        if len(pevalues) < 2:
            parser.error('pename must be of the form <pename,nthreads>')
        pename, pethreads = pevalues[0], pevalues[1]

        # If the PE name is 'openmp' then limit the number of threads to those specified
        if pename == omp_pe:
            env['OMP_NUM_THREADS'] = pethreads

    pid = os.getpid()

    ###########################################################################
    # The following is the main call to the cluster, using the "qsub" SGE
    # program. If $tasks has not been set then qsub is running a single
    # command, otherwise qsub is processing a text file of parallel commands.
    ###########################################################################
    if method == 'SGE':
        peoptions = ''
       ###########################################################################
       # Test Parallel environment options
       ###########################################################################
        if pename:
            # Is this a configured PE?
            if call([qconf, '-sp', pename]) == 1:
                parser.error('{} is not a valid PE'.format(pename))

            # Get a list of queues configured for this PE and confirm that the queue
            # we have submitted to has that PE set up.
            if call(['qstat', '-g', 'c', '-pe', pename]) == 1:
                parser.error('No parallel environments configured!')

            if call("qstat -g c -pe {} | sed '1,2d' | awk '{ print $1 }' | grep ^{}"
                    .format(pename, queue), shell=True) == 1:
                parser.error('PE {} is not configured on {}'.format(pename, queue))

            # The -w e option will result in the job failing if there are
            # insufficient slots on any of the cluster nodes
            peoptions = '-pe {} {} -w e'.format(pename, pethreads)

        # TODO: necessary? never defined in fsl_sub
        # This might have been replaced by slurm_hold on Biowulf (Adam?)
        sge_hold = env.get('sge_hold', '')

        if command:
            if args.scriptmode:
                sge_command = 'qsub {} {} {}'.format(logopts, sge_arch, sge_hold)
            else:
                sge_command = 'qsub -V -cwd -shell n -b y -r y {} {} -M {} -N {} -m {} {} {} {}'.format(
                        queue_cmd, peoptions, args.mailto, jobname, args.mailopts,
                        logopts, sge_arch, sge_hold)
            logger.info('sge_command: {}'.format(sge_command))
            logger.info('executing: {}'.format(' '.join(command)))
            call("exec {} {} | awk '{print $3'}", stdout=sys.stdout, shell=True)
        else:
            sge_tasks = '-t 1-{}'.format(ntasks)
            sge_command = 'qsub -V -cwd {} {} -M {} -N {} -m {} {} {} {} {}'.format(
                    queue_cmd, peoptions, args.mailto, jobname, args.mailopts,
                    logopts, sge_arch, sge_hold, sge_tasks)
            logger.info('sge_command: {}'.format(sge_command))
            logger.info('control file: {}'.format(args.taskfile))
            # TODO: translate the following to Python:
            #
            # exec $sge_command <<EOF | awk '{print $3}' | awk -F. '{print $1}'
            # #!/bin/sh
            #
            # #$ -S /bin/sh
            #
            # command=\`sed -n -e "\${SGE_TASK_ID}p" $taskfile\`
            #
            # exec /bin/sh -c "\$command"
            # EOF

    elif method == 'SLURM':
        # default memory allocation for a swarm of FSL jobs is 4 GB per subjob
        # if the FSL_MEM env var is set, use that value for single and/or swarm jobs
        swarm_mem = env.get('FSL_MEM', '4')

        if command:
            cmd_string = ' '.join(command)
            cmd_filename = 'cmd.{}'.format(pid)
            with open(cmd_filename, 'w') as f:
                f.write(cmd_string + '\n')
            # Make cmd_filename executable, e.g. chmod +x
            st = os.stat(cmd_filename)
            os.chmod(cmd_filename, st.st_mode | 0111)

            swarm_command = ['swarm', '--silent', '-f', cmd_filename, '-g',
                    swarm_mem, jobname, logopts, slurm_hold]
            logger.info('swarm command: {}'.format(' '.join(swarm_command)))
            logger.info('executing {}'.format(cmd_string))
            call(swarm_command, env, stdout=sys.stdout)
        else:
            swarm_command = ['swarm', '--silent', '-f', args.taskfile, '-g',
                    swarm_mem, jobname, logopts, slurm_hold]
            logger.info('swarm command: {}'.format(' '.join(swarm_command)))
            logger.info('control file: {}'.format(args.taskfile))
            call(swarm_command, env, stdout=sys.stdout)
    else:
        ###########################################################################
        # This runs the commands directly if a cluster is not being used.
        ###########################################################################
        stdoutName = '{}.o{}'.format(os.path.join(args.logdir, jobname), pid)
        stderrName = '{}.e{}'.format(os.path.join(args.logdir, jobname), pid)
        if command:
            cmd_string = ' '.join(command)
            ret = 0
            with open(stdoutName, 'w') as stdout:
                with open(stderrName, 'w') as stderr:
                    logger.info('Executing {}'.format(cmd_string))
                    ret = call(cmd_string, env, stdout, stderr, shell=True)

            if ret != 0:
                with open(stderrName) as stderr:
                    for line in stderr:
                        print(line)
                os.exit(ret)
        else:
            with open(args.taskfile) as tasks:
                for n, task in enumerate(tasks):
                    with open('{}.{}'.format(stdoutName, n)) as stdout:
                        with open('{}.{}'.format(stderrName, n)) as stderr:
                            logger.info('Executing {}'.format(task))
                            ret = call(task, env, stdout, stderr, shell=True)
        print(pid)


def call(args, env, stdout=None, stderr=None, shell=False):
    '''Just invokes subprocess.call but requires explicit environment'''
    return subprocess.call(args, env=env, stdout=stdout, stderr=stderr, shell=shell)

def get_username():
    return pwd.getpwuid(os.getuid())[0]

def qname(estimated_duration):
    '''The following auto-decides what cluster queue to use. The calling
    FSL program will probably use the -T option when calling fsl_sub,
    which tells fsl_sub how long (in minutes) the process is expected to
    take (in the case of the -t option, how long each line in the
    supplied file is expected to take). You need to setup the following
    list to map ranges of timings into your cluster queues - it doesn't
    matter how many you setup, that's up to you.

    estimated_duration: estimated task time in minutes
    '''
    if estimated_duration < 20:
        queue = 'veryshort.q'
    elif estimated_duration < 120:
        queue = 'short.q'
    elif estimated_duration < 1440:
        queue = 'long.q'
    else:
        queue = 'verylong.q'

    logger.debug("Estimated time was {} mins: queue name is {}".format(
        (estimated_duration, queue)))
    return queue

def which(program):
    '''http://stackoverflow.com/a/377028/1689220'''
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None

if __name__ == '__main__':
    main()
