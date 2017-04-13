#!/usr/bin/env python

from __future__ import print_function
import os
import pwd
import socket
import logging
import argparse
import subprocess

# Global logger. Initialized in init_logging.
logger = logging.getLogger(__name__)

# Global environment. Used by subprocesses.
env = os.environ.copy()

# Default memory allocation for a swarm of FSL jobs is 4 GB per subjob
# if the FSL_MEM env var is set, use that value for single and/or swarm jobs
FSL_MEM = env.get('FSL_MEM', 4)

# Override the queue/partition used, e.g. FSL_QUEUE=nimh
FSL_QUEUE = env.get('FSL_QUEUE', None)

# Do not submit batch jobs on Helix or if $NOBATCH is set
NOBATCH = env.get('NOBATCH', False) or socket.getfqdn() == 'helix.nih.gov'

# If defined, will use SGE for job submission
SGE_ROOT = env.get('SGE_ROOT', None)

FSLSUBALREADYRUN = env.get('FSLSUBALREADYRUN', False)
FSLSUBVERBOSE = env.get('FSLSUBVERBOSE', False)


# If you have a Parallel Environment configured for OpenMP tasks then
# the variable OMP_PE should be set to the name you have defined for that
# PE. The script will work out which queues have that PE setup on them.
# Note, we support openmp tasks even when Grid Engine is not in use.
OMP_PE = 'openmp'


class Method(object):
    '''Interface for submission method, e.g. SGE or Slurm'''
    queues = {}

    def __init__(self):
        self._queue = None
        self.qconf = 'echo'
        self.jobname = None
        self.hold = None
        self.logdir = None
        self.ntasks = 0
        self.pid = os.getpid()
        self.mailto = '{}@mail.nih.gov'.format(get_username())
        self.mailopts = None
        self.parallel_env = None
        self.nthreads = None

    def autodetect_queue(self, duration):
        '''The following auto-decides what cluster queue to use. The calling
        FSL program will probably use the -T option when calling fsl_sub,
        which tells fsl_sub how long (in minutes) the process is expected to
        take (in the case of the -t option, how long each line in the
        supplied file is expected to take). You need to setup self.queues
        to map ranges of timings into your cluster queues - it doesn't
        matter how many you setup, that's up to you.

        duration: estimated task time in minutes
        '''
        for dur in sorted(self.queues.keys(), reverse=True):
            if duration >= dur:
                self.queue = self.queues[dur]
                logger.debug('Estimated time was {} mins: queue name is {}'.format(
                        duration, self.queue))
                return

    @property
    def queue(self):
        return self._queue

    @queue.setter
    def queue(self, q):
        if not self._queue_valid(q):
            self.fail('Invalid queue specified!')
        self._queue = q

    def _queue_valid(self, q):
        return q is not None

    def submit_command(self):
        pass

    def submit_taskfile(self):
        pass

    def fail(self, err_msg):
        pass

class SGE(Method):
    '''Runs tasks using qsub command.'''
    queues = {0: 'veryshort.q', 20: 'short.q', 120: 'long.q', 1440: 'verylong.q'}

    def __init__(self):
        super(SGE, self).__init__()
        self._queue = 'long.q'
        self.qconf = 'qconf'

    def _queue_valid(self, q):
        return call([self.qconf, '-sq', q], env) == 0

    def __setup(self):
        if not self.mailopts:
            self.mailopts = 'a'
        self.sge_hold = '-hold_jid {}'.format(self.hold)
        self.logopts = '-o {0} -e {0}'.format(self.logdir)
        self.jobname = self.jobname
        self.sge_priority = '-p {}'.format(self.priority)

        available_archs = subprocess.check_output(
                "qhost | tail -n +4 | awk '{print $2}' | sort | uniq",
                env=env, shell=True).split()
        if self.arch not in available_archs:
            self.fail('Sorry arch of {} is not supported on this SGE configuration!\nShould be one of: {}'.format(
                self.arch, ' '.join(available_archs)))
        else:
            self.sge_arch = '-l arch={}'.format(self.arch)

        self.peoptions = ''
        # Test Parallel environment options
        if self.parallel_env:
            # Is this a configured PE?
            if call([self.qconf, '-sp', self.parallel_env]) == 1:
                self.fail('{} is not a valid PE'.format(self.parallel_env))

            # Get a list of queues configured for this PE and confirm that the queue
            # we have submitted to has that PE set up.
            if call(['qstat', '-g', 'c', '-pe', self.parallel_env]) == 1:
                self.fail('No parallel environments configured!')

            if call("qstat -g c -pe {} | sed '1,2d' | awk '{ print $1 }' | grep ^{}"
                    .format(self.parallel_env, self.queue), shell=True) == 1:
                self.fail('PE {} is not configured on {}'.format(self.parallel_env, self.queue))

            # The -w e option will result in the job failing if there are
            # insufficient slots on any of the cluster nodes
            self.peoptions = '-pe {} {} -w e'.format(self.parallel_env, self.nthreads)

    def submit_command(self, command):
        if self.scriptmode:
            sge_command = 'qsub {} {} {}'.format(self.logopts, self.sge_arch, self.sge_hold)
        else:
            sge_command = 'qsub -V -cwd -shell n -b y -r y -q {} {} -M {} -N {} -m {} {} {} {}'.format(
                    self.queue, self.peoptions, self.mailto, self.jobname, self.mailopts,
                    self.logopts, self.sge_arch, self.sge_hold)
        logger.info('sge_command: {}'.format(sge_command))
        logger.info('executing: {}'.format(' '.join(command)))
        cmd_str = 'exec {} {}'.format(sge_command, command)
        call(cmd_str + " | awk '{print $3'}", shell=True)

    def submit_taskfile(self, taskfile):
        sge_tasks = '-t 1-{}'.format(self.ntasks)
        sge_command = 'qsub -V -cwd -q {} {} -M {} -N {} -m {} {} {} {} {}'.format(
                self.queue, self.peoptions, self.mailto, self.jobname, self.mailopts,
                self.logopts, self.sge_arch, self.sge_hold, sge_tasks)
        logger.info('sge_command: {}'.format(sge_command))
        logger.info('control file: {}'.format(taskfile))
        # See Line 465 of original fsl_sub for original form of code below
        script = '''
        #!/bin/sh

        #$ -S /bin/sh

        command=\`sed -n -e "\${SGE_TASK_ID}p" $taskfile\`

        exec /bin/sh -c "\$command"
        '''
        cmd_str = 'exec {} {}'.format(sge_command, script)
        call(cmd_str + " | awk '{print $3'} | awk -F. '{print $1}'", shell=True)


class Slurm(Method):
    '''Runs tasks using Biowful's swarm command.'''
    queues = {0: 'quick', 240: 'norm', 10*24*60: 'unlimited'}

    def __init__(self):
        super(Slurm, self).__init__()
        self._queue = 'norm'

    def submit_command(self, command):
        cmd_string = ' '.join(command)
        cmd_filename = os.path.join(self.logdir, 'cmd.{}'.format(self.pid))
        with open(cmd_filename, 'w') as f:
            f.write(cmd_string + '\n')
        # Make cmd_filename executable, e.g. chmod +x
        st = os.stat(cmd_filename)
        os.chmod(cmd_filename, st.st_mode | 0111)
        self.submit_taskfile(cmd_filename)

    def submit_taskfile(self, taskfile):
        swarm_command = ['swarm', '--silent', '-f', taskfile,
                '-g', str(FSL_MEM),
                '--partition', self.queue,
                '--job-name', self.jobname,
                '--logdir', self.logdir]
        if self.hold:
            swarm_command += ['--dependency', 'afterany:{}'.format(self.hold)]
        if self.nthreads:
            swarm_command += ['--threads-per-process', str(self.nthreads)]
        logger.info('swarm command: {}'.format(' '.join(swarm_command)))
        call(swarm_command, env)

class Local(Method):
    '''Runs the commands directly if a cluster is not being used.'''
    def submit_command(self, command):
        stdout_name = '{}.o{}'.format(os.path.join(self.logdir, self.jobname), self.pid)
        stderr_name = '{}.e{}'.format(os.path.join(self.logdir, self.jobname), self.pid)
        cmd_string = ' '.join(command)
        ret = self.__run(cmd_string, env, stdout_name, stderr_name)
        if ret != 0:
            with open(stderr_name) as stderr:
                for line in stderr:
                    print(line)
            os.exit(ret)
        print(self.pid)

    def submit_taskfile(self, taskfile):
        stdout_name = '{}.o{}'.format(os.path.join(self.logdir, self.jobname), self.pid)
        stderr_name = '{}.e{}'.format(os.path.join(self.logdir, self.jobname), self.pid)
        with open(taskfile) as tasks:
            for n, task in enumerate(tasks):
                self.__run(task, env, '{}.{}'.format(stdout_name, n),
                        '{}.{}'.format(stderr_name, n))
        print(self.pid)

    def __run(self, task, env, stdout_name, stderr_name):
        with open(stdout_name, 'w') as stdout:
            with open(stderr_name, 'w') as stderr:
                logger.info('Executing {}'.format(task))
                return call(task, env, stdout=stdout, stderr=stderr, shell=True)

def init_logging(verbose=False):
    '''Configures a console logger with log level based on verbosity'''
    console = logging.StreamHandler()
    formatter = logging.Formatter
    console.setFormatter(formatter('%(levelname)s: %(message)s'))
    console.setLevel(logging.DEBUG)
    logger.addHandler(console)
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

    # New epilog for Biowulf/Slurm
    epilog = '''
Queues (Partitions):

  Biowulf nodes are grouped into partitions. A partition can be specified
  when submitting a job. The default partition is 'norm'. The freen command
  can be used to see free nodes and CPUs, and available types of nodes on
  each partition.

  See also https://hpc.nih.gov/docs/userguide.html#partitions.
'''

    description='''
%(prog)s V2.0 - wrapper for job control system such as Slurm or SGE

%(prog)s gzip *.img *.hdr
%(prog)s -q short.q gzip *.img *.hdr
%(prog)s -a darwin regscript rawdata outputdir ...
'''

    warnings = []

    # Prepare environment for subprocesses
    if 'module' in env:
        del env['module']

    method = None
    if FSLSUBALREADYRUN:
        method = Local()
        warnings.append('job on queue attempted to submit parallel jobs - running jobs serially instead')
    elif NOBATCH:
        method = Local()
    elif SGE_ROOT:
        qconf = which('qconf')
        if not qconf:
            warnings.append('Warning: SGE_ROOT environment variable is set but Grid Engine software not found, will run locally')
            method = Local()
        else:
            method = SGE()
            method.qconf = qconf
    else:
        method = Slurm()

    env['FSLSUBALREADYRUN'] = 'true'

    parser = argparse.ArgumentParser(description=description, epilog=epilog,
            formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('command', nargs='?', help='FSL command')
    parser.add_argument('arguments', nargs=argparse.REMAINDER, help='FSL command arguments')

    parser.add_argument('-T', metavar='minutes', type=int, help='Estimated job length in minutes, used to auto-set queue name')
    avail_queues = ', '.join('"{}"'.format(q) for q in method.queues.values())
    parser.add_argument('-q', metavar='queuename', help='Possible values for <queuename> are {}. See below for details. Default is "{}".'.format(avail_queues, method.queue))
    parser.add_argument('-a', metavar='arch-name', help='Architecture [e.g., darwin or lx24-amd64]')
    parser.add_argument('-p', metavar='job-priority', type=int, help='Lower priority [0:-1024] default = 0')
    parser.add_argument('-M', metavar='email-address', help='Who to email, default = {}'.format(method.mailto))
    parser.add_argument('-j', metavar='jid', help='Place a hold on this task until job jid has completed')
    parser.add_argument('-t', metavar='filename', help='Specify a task file of commands to execute in parallel')
    parser.add_argument('-N', metavar='jobname', help='Specify jobname as it will appear on queue')
    parser.add_argument('-l', metavar='logdir', help='Where to output logfiles')
    parser.add_argument('-m', metavar='mailoptions', help='Change the SGE mail options, see qsub for details')
    parser.add_argument('-z', metavar='output', help='If <output> image or file already exists, do nothing and exit')
    parser.add_argument('-F', action='store_true', help='Use flags embedded in scripts to set SGE queuing options')
    parser.add_argument('-s', metavar='pename,threads', help='Submit a multi-threaded task - requires a PE (<pename>) to be configured for the requested queues. "threads" specifies the number of threads to run')
    parser.add_argument('-v', action='store_true', help='Verbose mode.')

    parser.set_defaults(l='./')

    args = parser.parse_args()

    # Configure verbosity
    verbose = args.v or FSLSUBVERBOSE
    init_logging(verbose)

    for warning in warnings:
        logger.warn(warning)

    method.fail = parser.error
    logger.debug('Method is {}'.format(method))

    # -z tells us to exit early if output image already exists
    if args.z:
        if os.path.exists(args.z):
            os.exit(0)
        try:
            if subprocess.check_output(
                    ['{}/bin/imtest'.format(env['FSLDIR']), args.z], env) == '1':
                os.exit(0)
        except subprocess.CalledProcessError:
            pass

    # Ensure a command or taskfile is specified, but not both
    taskfile = args.t
    command = None
    if args.command:
        # Check that command exists and is executable
        if not which(args.command):
            parser.error('The command you have requested cannot be found or is not executable')
        # Add command arguments if specified
        command = [args.command]
        if args.arguments:
            command += args.arguments
    if not taskfile and not command:
        parser.error('Either supply a command to run or a parallel task file')
    elif taskfile and command:
        parser.error('You appear to have specified both a task file and a command to run')

    # Specifying -T will auto-detect a queue/partition based on estimated task duration
    if args.T:
        if args.T < 0:
            args.T = 0
        method.autodetect_queue(args.T)
    # Specifying -q directly will set the queue/partition
    if args.q:
        method.queue = args.q
    # FSL Queue (Partition) can be specified via environment variable
    if FSL_QUEUE:
        method.queue = FSL_QUEUE

    # -a sets is the cluster submission flag for controlling the required
    # hardware architecture (normally not set by the calling program)
    if args.a:
        method.arch = args.a

    # -p set the priority of the job - ignore this if your cluster
    # environment doesn't have priority control in this way.
    # NOTE: this is unused in original fsl_sub...
    if args.p:
        method.priority = args.p

    # -j tells the cluster not to start this job until cluster job ID $jid
    # has completed. You will need this feature.
    if args.j:
        method.hold = args.j

    # -t will pass on to the cluster software the name of a text file
    # containing a set of commands to run in parallel; one command per line.
    if taskfile:
        if not os.path.isfile(taskfile):
            parser.error('Task file {} does not exist'.format(taskfile))
        with open(taskfile) as f:
            ntasks = sum(1 for line in f)
            if ntasks <= 0:
                parser.error('Task file {} is empty\n' +
                        'Should be a text file listing all the commands to run!'
                        .format(taskfile))
            method.ntasks = ntasks

    # -N option determines what the command will be called when you list
    # running processes.
    if args.N:
        method.jobname = args.N
    else:
        if taskfile:
            method.jobname = os.path.basename(taskfile)
        else:
            method.jobname = os.path.basename(command[0])

    # -l tells the cluster what to call the standard output and standard
    # -error logfiles for the submitted program.
    if args.l:
        logdir = args.l
        if not os.path.exists(logdir):
            os.makedirs(logdir)
        elif not os.path.isdir(logdir):
            parser.error('Log destination is a file (should be a folder)')
        method.logdir = logdir

    if args.M:
        method.mailto = args.M
    if args.m:
        method.mailopts = args.m
    if args.F:
        method.scriptmode = True

    # -s configures a multi-threaded task (SGE)
    # Parse multithreading options
    if args.s:
        pevalues = args.s.split(',')
        if len(pevalues) < 2:
            parser.error('pename must be of the form <pename,nthreads>, e.g. "openmp,4"')
        method.parallel_env, method.nthreads = pevalues[0], pevalues[1]

        # If the PE name is 'openmp' then limit the number of threads to those specified
        if method.parallel_env == OMP_PE:
            env['OMP_NUM_THREADS'] = method.nthreads

    ###########################################################################
    # The following is the main call to the cluster, using the "qsub" SGE
    # program. If $tasks has not been set then qsub is running a single
    # command, otherwise qsub is processing a text file of parallel commands.
    ###########################################################################
    if command:
        method.submit_command(command)
    else:
        method.submit_taskfile(taskfile)


def call(args, env, stdout=None, stderr=None, shell=False):
    '''Just invokes subprocess.call but requires explicit environment'''
    return subprocess.call(args, env=env, stdout=stdout, stderr=stderr, shell=shell)

def get_username():
    return pwd.getpwuid(os.getuid())[0]

def which(program):
    '''http://stackoverflow.com/a/377028/1689220'''
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ['PATH'].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None

if __name__ == '__main__':
    # For testing on Biowulf. TODO: remove
    #import datetime
    #with open('/data/naegelejd/fsl_sub/fsl_sub.log', 'a') as f:
    #    f.write('Called at {}\n'.format(datetime.datetime.now()))
    main()
