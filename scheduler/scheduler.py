#!/usr/bin/python

"""
Simple scheduler - runs a global queue of jobs, sleeping between separate runs of jobs.  Each time a job is to be
executed, the scheduler wakes up, pulls the job from the queue, runs it, and re-adds it to the queue.

This is a simplified version of a proper scheduler, allowing us to do a bit more than cron or anacron does (jobs
timed to start relative to the end of the last job OR on an absolute schedule, for instance, rather than just
on absolute, and with better time resolution than "hourly".
"""

import datetime
import time
import os
import os.path
import logging
import logging.handlers
import sys
import signal
import atexit
from looms.lib.jobs.PackageUpdater import PackageUpdater
from looms.lib.jobs.HostPackageScan import HostPackageScan
from looms.lib.jobs.UpdateHostDBEntry import UpdateHostDBEntry
from looms.lib.jobs.UpdateHosts import UpdateHosts

# Set a few globals here.  At the moment we're not reading in any config files when we start, which should be fixed.
# however, until then, we'll just cheap out and set some globals.
pidfile = '/var/run/looms/scheduler.pid'
logfile = '/var/log/looms/scheduler.log'
logger = None


def setup_logs():
    """
    Configures the logging module to handle our log output based on the information read in from the config file.

    :return: False if the logging utility fails configuration.
             True if the logging utility successfully configures the log file output.
    """

    global logfile
    global pidfile
    global logger

    # Check to confirm that the path to logfile exists.
    if not os.path.isdir(os.path.dirname(logfile)):
        os.makedirs(os.path.dirname(logfile))

    # Creates rotating file handler, with a max size of 10 MB and maximum of 5 backups, if path configured.
    handler = logging.handlers.RotatingFileHandler(logfile, mode='a',
                                                   maxBytes=1048576, backupCount=5)

    if handler is None:
        print("A serious error occurred while attempting to create the logging handler.")
        return False

    # Create formatter...
    formatter = logging.Formatter(fmt='%(levelname)s %(asctime)s: %(name)s - %(module)s.%(funcName)s - %(message)s')
    handler.setFormatter(formatter)
    # now set log level, and configure the loggers for each imported local package (including gnupg.)
    log_level = logging.DEBUG

    # create the logger for main, and set up the loggers for each of the supporting modules:
    logger = logging.getLogger(__name__)
    logger.addHandler(handler)
    logger.setLevel(log_level)

    if logger is None:
        print("A serious error occurred while attempting to generate a logger object.")
        return False

    return True


def print_help():
    """
    Prints the help screen when a startup syntax error occurs.
    :return:
    """

    print("Looms Scheduler Daemon")
    print("Usage: scheduler.py [start|stop|restart]")


def main():
    """
    Standard flow control stuff; daemonizes the process as well.
    :return:
    """

    global logger
    # init the log output.
    setup_logs()

    # Check to see what arguments have been passed - accepts start, stop and restart.  For now assume it's the second
    # item in the passed array.
    try:
        action = sys.argv[1]
    except IndexError:
        print_help()
        sys.exit(1)

    # We know we have an action, so check the action type and proceed once execution is complete.  We know that we're
    # the double child no matter the event because the parent procs all explicitly exited.
    if action.lower() == 'start':
        if start() != 0:
            sys.exit(1)
    elif action.lower() == 'stop':
        stop()
    elif action.lower() == 'restart':
        if restart() != 0:
            sys.exit(1)
    else:
        print_help()
        sys.exit(1)

    # Assume that this is a properly separated daemon (double forked for paranoia.)  Now begin on the scheduling.

    # Normally we'd read in scheduling data and config from files.  This is a trivial scheduler basically designed
    # as a monotasker to run our system environment, so we'll do our config in script and fix everything right later.
    pkg_updater = PackageUpdater.PackageUpdater()
    # Set the timer_base to be today at midnight.
    pkg_updater.timer_base = datetime.datetime.combine(datetime.datetime.today(), datetime.time(0, 0))
    pkg_updater.timer_type = 'absolute'
    pkg_updater.timer = datetime.timedelta(hours=24)
    pkg_updater.update_next_run()

    pkgscan = HostPackageScan.HostPackageScan()
    pkgscan.timer_base = datetime.datetime.now()
    pkgscan.timer_type = 'relative'
    pkgscan.timer = datetime.timedelta(minutes=5)
    pkgscan.update_next_run()

    update_host_db = UpdateHostDBEntry.UpdateHostDBEntry()
    update_host_db.timer_base = datetime.datetime.now()
    update_host_db.timer_type = 'relative'
    update_host_db.timer = datetime.timedelta(minutes=5)
    update_host_db.update_next_run()

    update_hosts = UpdateHosts.UpdateHosts()
    update_hosts.timer_base = datetime.datetime.now()
    update_hosts.timer_type = 'relative'
    update_hosts.timer = datetime.timedelta(minutes=5)
    update_hosts.update_next_run()

    # Load the scheduling list.
    schedule = []

    schedule = insert_job(pkg_updater, schedule)
    schedule = insert_job(pkgscan, schedule)
    schedule = insert_job(update_host_db, schedule)
    schedule = insert_job(update_hosts, schedule)

    # Give us a list of jobs and dates/times, to be sure we have everything recorded.
    for job in schedule:
        logger.debug("Running job {0} at time {1}".format(job.script_path, job.get_next_run_time()))

    while True:
        # Read the topmost job and get the amount of time before it runs.
        next_job_seconds = schedule[0].get_next_run_delta().total_seconds()
        if next_job_seconds > 0:
            time.sleep(next_job_seconds + 1)

        # Pop job off the top of the queue.
        job = schedule.pop(0)
        logger.debug("Running job {0}".format(job.script_path))
        job.run()

        if job.error_state != 0:
            logger.debug("An error occurred during the execution of this job.")
            # Reset the error state for the next round
            job.error_state = 0

        # Regardless of whether or not the job failed, we kinda want to know what happened.
        logger.debug(job.log_string)
        # Empty the job log string.
        job.log_string = ""


        insert_job(job, schedule)


def start():
    """
    When the program is called with the 'start' argument, check to see if the scheduler is already running (pidfile
    check) and start if not.  If it is, then exit out.  Note that we do not clean up the pidfile here - if there is
    another instance of the daemon already running then we don't want to bork everything up.  The daemon is responsible
    for itself, the pre-forks are not.

    :return:
             Nonzero value if the daemon could not be started because it was already running.
             Zero value if the daemon started successfully.
    """

    global pidfile
    global logger

    # Check before anything else to be sure that the path to the pidfile exists.
    pid_dir = os.path.split(pidfile)[0]
    if not os.path.exists(pid_dir):
        os.makedirs(pid_dir)

    if os.path.exists(pidfile):
        # pidfile exists, so either a dirty shutdown occurred and the pidfile was not cleansed, or else a process
        # is already running.  Read the file to get the pid, and then exit out reporting that pid to logger.
        pid_fd = open(pidfile, 'r')
        pid = pid_fd.read()
        logger.error('Unable to start daemon - pidfile {0} exists and contains pid {1}.'.format(pidfile, pid))
        return 1

    # otherwise pidfile does not exist, so we initiate the daemon.
    daemonize()

    return 0


def stop():
    """
    When the program is called with the stop argument, check to see if the scheduler is running.  If so, issue a kill
    command on the pid found in pidfile.
    :return:
    """

    global pidfile
    global logger

    # Check to see if the pidfile exists.
    if os.path.exists(pidfile):
        # file exists, process may.  Get pid and issue halt.
        try:
            pid_fd = open(pidfile, 'r')
            pid = pid_fd.read()
        except OSError as e:
            logger.error("An error occurred while attempting to open pid file "
                         "{0} - {1}: {2}".format(pidfile, e.errno, e.message))
            return 1
        except IOError as e:
            logger.error("An error occurred while attempting to read pid file "
                         "{0} - {1}: {2}".format(pidfile, e.errno, e.message))
            return 1

        # simple sanity check - make sure the pidfile contents aren't borked.
        if not pid.isdigit():
            logger.error('Contents of pidfile {0} are non-numeric: {1}'.format(pidfile, pid))
            return 1

        # pidfile exists, process ID is present.  Attempt to shut down process; try for 15 seconds before calling it.
        timer_start = 0
        while pid in os.listdir('/proc') and timer_start < 15:
            os.kill(int(pid), signal.SIGTERM)
            time.sleep(1)
            timer_start += 1

        if timer_start >= 15:
            logger.error('Could not shut down process {0} - SIGTERM ignored!')
            return 1

        return 0


def restart():
    """
    When the program is started with the restart option, attempt to stop the existing process and then start it again.
    The function shortcuts itself to the end; that is, if the process reports an error when attempting to stop the
    daemon, then no attempt to start the daemon is made.

    :return: 0 if the daemon could be stopped and then started successfully - 1 if the daemon failed either option.
    """

    global logger

    success = stop()

    if not success:
        logger.error('Unable to stop daemon - rejecting start attempt.')
        return 1

    success = start()

    if not success:
        logger.error('Daemon appears to have stopped, but is not starting.')
        return 1


def daemonize():
    """
    Performs the tasks necessary to convert the script into a long running daemon - forks the process (twice), sets
    cwd to root, sets umask to 0, and redirects stdin, stdout and stderr to /dev/null.  The pidfile is written out
    to /var/looms, and atexit.register is called to set automatic deletion of the pidfile in the event of daemon
    shutdown.
    :return:
    """

    global logger

    try:
        pid = os.fork()
    except OSError as e:
        logger.error("Fork attempt 1 failed.  Error code {0}, message {1}".format(e.errno, e.message))
        sys.exit(1)

    # If the returned PID is nonzero, then this is the parent and the PID is that of the child.  Exit out.
    if pid > 0:
        sys.exit(0)

    # Change working directory to / so that, in the event of system shutdown, our daemon is not dwelling on a
    # mounted volume that can't be brought down.
    os.chdir('/')
    # Create new session group, of which this process will be the leader..
    os.setsid()
    # Set umask to 0 so there is no interference should we need to create files.  We must be sure to properly
    # set and secure ALL created files by ourselves after this.
    os.umask(0)

    # Redaemonize to put the child into a leaderless session group (parent was leader and it exited.)
    try:
        pid = os.fork()
    except OSError as e:
        logger.error("Fork attempt 2 failed.  Error code {0}, message {1}".format(e.errno, e.message))
        sys.exit(1)

    if pid > 0:
        sys.exit(0)

    # Flush anything sitting in stdout or stderr pipes.
    sys.stdout.flush()
    sys.stderr.flush()
    # and then redirect them to /dev/null.
    stdin = open('/dev/null', 'r')
    stdout = open('/dev/null', 'a+')
    stderr = open('/dev/null', 'a+', 0)
    os.dup2(stdin.fileno(), sys.stdin.fileno())
    os.dup2(stdout.fileno(), sys.stdout.fileno())
    os.dup2(stderr.fileno(), sys.stderr.fileno())

    # Finally, register a pid deletion routine with atexit, and then create our pidfile
    atexit.register(delete_pidfile)
    pid = str(os.getpid())
    pid_fd = open(pidfile, 'w+')
    pid_fd.write(pid + "\n")
    pid_fd.close()


def delete_pidfile():
    """
    Deletes the pidfile specified by global pidfile, if it exists.
    :return:
    """

    if os.path.exists(pidfile):
        os.remove(pidfile)


def insert_job(job, schedule_list):
    """
    Takes a job object (child of Job) and a list of existing jobs, then attempts to insert the new Job into the
    chronologically correct spot.  If schedule_list is empty, the job is trivially appended.
    :param job: job that is to be inserted into the schedule.
    :param schedule_list: list object of jobs, in chronological order from soonest to be completed to last.
    :return: the modified schedule.
    """

    job_count = len(schedule_list)
    logger.debug("Num jobs in queue: {0}".format(job_count))

    for i in range(0, job_count):
        logger.debug("checking job {0}".format(i))
        logger.debug("new job {0} versus old job {1}".format(job.script_path, schedule_list[i].script_path))
        logger.debug(job.get_next_run_time())
        logger.debug(schedule_list[i].get_next_run_time())
        if job.get_next_run_time() < schedule_list[i].get_next_run_time():
            schedule_list.insert(i, job)
            break
    else:
        schedule_list.append(job)

    return schedule_list


# run on main boilerplate.
if __name__ == "__main__":
    main()
