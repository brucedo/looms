#!/usr/bin/python

"""
Simple scheduler - runs a global queue of jobs, sleeping between separate runs of jobs.  Each time a job is to be
executed, the scheduler wakes up, pulls the job from the queue, runs it, and re-adds it to the queue.

This is a simplified version of a proper scheduler, allowing us to do a bit more than cron or anacron does (jobs
timed to start relative to the end of the last job OR on an absolute schedule, for instance, rather than just
on absolute, and with better time resolution than "hourly".
"""

import os
import os.path
import sys
import subprocess
import shlex
import datetime
import abc
import time
import mysql.connector
import mysql.connector.errors


def main():
    """
    Standard flow control stuff; daemonizes the process as well.
    :return:
    """

    # start daemonizing.

    # Assume that this is a properly separated daemon (double forked for paranoia.)  Now begin on the scheduling.

    # Normally we'd read in scheduling data and config from files.  This is a trivial scheduler basically designed
    # as a monotasker to run our system environment, so we'll do our config in script and fix everything right later.
    pkg_updater = PackageUpdater()
    # Set the timer_base to be today at midnight.
    pkg_updater.timer_base = datetime.datetime.combine(datetime.datetime.today(), datetime.time(0, 0))
    pkg_updater.timer_type = 'absolute'
    pkg_updater.timer = datetime.timedelta(hours=24)
    pkg_updater.update_next_run()

    pkgscan = HostPackageScan()
    pkgscan.timer_base = datetime.datetime.now()
    pkgscan.timer_type = 'relative'
    pkgscan.timer = datetime.timedelta(minutes=5)
    pkgscan.update_next_run()

    update_host_db = UpdateHostDBEntry()
    update_host_db.timer_base = datetime.datetime.now()
    update_host_db.timer_type = 'relative'
    update_host_db.timer = datetime.timedelta(minutes=5)
    update_host_db.update_next_run()

    update_hosts = UpdateHosts()
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

    while True:
        # Read the topmost job and get the amount of time before it runs.
        next_job_seconds = schedule[0].get_next_run_delta().total_seconds()
        if next_job_seconds > 0:
            time.sleep(next_job_seconds + 1)

        # Pop job off the top of the queue.
        job = schedule.pop(0)
        print("Running job {0}".format(job.script_path))
        job.run()
        insert_job(job, schedule)


def insert_job(job, schedule_list):
    """
    Takes a job object (child of Job) and a list of existing jobs, then attempts to insert the new Job into the
    chronologically correct spot.  If schedule_list is empty, the job is trivially appended.
    :param job: job that is to be inserted into the schedule.
    :param schedule_list: list object of jobs, in chronological order from soonest to be completed to last.
    :return: the modified schedule.
    """

    job_count = len(schedule_list)
    print("Num jobs in queue: {0}".format(job_count))

    for i in range(0, job_count):
        print("checking job {0}".format(i))
        print ("new job {0} versus old job {1}".format(job.script_path, schedule_list[i].script_path))
        print job.get_next_run_time()
        print schedule_list[i].get_next_run_time()
        if job.get_next_run_time() < schedule_list[i].get_next_run_time():
            schedule_list.insert(i, job)
            break
    else:
        schedule_list.append(job)

    return schedule_list


class Job(object):
    """
    Job class defines a simple interface in which jobs are stored, and provide simple functionality like providing
    a datetime object marking when the job should next be run (for easy insertion back into the queue to reup the
    task) and time remaining from current time before this job should be run (so the queue manager can easily
    determine how long it needs to sleep for.)
    """

    def __init__(self):
        __metaclass__ = abc.ABCMeta
        """
        Sets up an empty job.
        :return:
        """

        # The type of timer - relative to current time, or absolute.
        self.timer_type = ''
        # The actual timer - a timedelta object indicating the time gap between runs of the job.
        self.timer = None
        # Timer base - with an absolute type, the base never changes.  With relative, the base can (but does not
        # necessarily) change to be the most recent end time of the job.  The base _can_ be used to calculate the
        # next run time by the job.
        self.timer_base = None
        # The datetime object representing when the job will next run.
        self.next_run_time = None

    @abc.abstractmethod
    def run(self):
        """
        Job runner for class.  Abstract, must be instantiated by an actual job in order to handle the necessary
        logic checks for whether a job should go ahead or not.
        :return:
        """

    def update_next_run(self):
        """
        Updates the next run time object to the next absolute time the task should run again (note that this stores
        an absolute time regardless of whether the timer_type is relative or not, but the absolute time that is
        stored IS affected by the timer_type.
        :return:
        """

        if self.timer_type == 'absolute':
            self.timer_base += self.timer
            self.next_run_time = self.timer_base + self.timer
        else:
            self.next_run_time = datetime.datetime.now() + self.timer

    def get_next_run_time(self):
        """
        Returns a datetime object indicating the next time the job should be run, with respect to either it's
        absolute runtime or its relative run time.
        :return:
        """

        return self.next_run_time

    def get_next_run_delta(self):
        """
        Returns a timedelta object relative to current time indicating when the job should be run.
        :return:
        """

        # Take the next run time and subtract the current datetime.  This guarantees a negative timedelta if
        # the current time has surpassed the next_run_time.
        return self.next_run_time - datetime.datetime.now()


class PackageUpdater(Job):
    """
    Subclass of Job; responsible for calling the package update process.  No logic in place prevents the package
    updater from running.
    """

    def __init__(self):
        """
        Initializes PackageUpdater.
        :return:
        """

        super(PackageUpdater, self).__init__()

        self.script_path = '/data/programs/usr/local/bin/pkg_manager/pkg_manager.py'

    def run(self):
        """
        Executes the package manager.
        :return:
        """

        print("Running the Package Updater script at datetime: {0}".format(datetime.datetime.now()))

        subprocess.call(shlex.split(self.script_path))

        self.update_next_run()


class HostPackageScan(Job):
    """
    Subclass of Job; responsible for calling the system package scan whenever there are hosts in the host table
    with no entries in host_update_history.
    """

    def __init__(self):
        """
        Initializes HostPackageScan.
        :return:
        """

        super(HostPackageScan, self).__init__()

        self.query = """SELECT h.name, h.domain FROM host AS h
                   LEFT OUTER JOIN host_update_history AS huh ON h.id = huh.host_id
                   WHERE huh.id IS NULL;"""
        self.script_path = '/data/programs/usr/local/bin/update_linux_hosts/pkgscan_linux_host.py'

    def run(self):
        """
        Checks to see if the database has any systems named in the hosts table that do NOT have any package
        entries in host_update_history, and if so adds those system's names to a list and calls pkg_scan_linux_host.py
        with them as an argument.
        :return:
        """

        print("Running the host package scan at datetime: {0}".format(datetime.datetime.now()))

        connection = mysql.connector.connect(option_files='/etc/update_linux_hosts/db_info/options.cnf')
        cursor = connection.cursor()

        cursor.execute(self.query)

        # Execute the query
        machine_list = []
        for (machine_name, domain) in cursor:
            machine_list.append(machine_name + '.' + domain)

        if len(machine_list) <= 0:
            print("No machines in the database require package scanning.")
            return

        # Remove any extraneous commas.
        machine_str = ','.join(machine_list)

        print("Found machines {0} to scan.".format(machine_str))

        cmd = self.script_path + ' ' + machine_str

        subprocess.call(shlex.split(cmd))

        self.update_next_run()

        cursor.close()
        connection.close()


class UpdateHostDBEntry(Job):
    """
    Subclass of Job class; responsible for calling the update_host_db script whenever there is data to be consumed
    out of the /data/call_home/registered_hosts file.
    """

    def __init__(self):
        """
        Initializes UpdateHostDBEntry class.
        :return:
        """

        super(UpdateHostDBEntry, self).__init__()

        self.script_path = '/data/programs/usr/local/bin/update_linux_hosts/update_host_db.py'
        self.data_file = '/data/call_home/registered_hosts'

    def run(self):
        """
        Checks to see if the /data/call_home/registered_hosts has data in it, and if so calls the
        update_host_db script to add/update systems.
        :return:
        """

        print("Running Update Host DB Entry job at datetime {0}".format(datetime.datetime.now()))

        file_stats = os.stat(self.data_file)

        if file_stats.st_size > 0:
            print("There are records in the php output!")
            subprocess.call(shlex.split(self.script_path))

        # Update the next run time...
        self.update_next_run()


class UpdateHosts(Job):
    """
    Subclass of the Job class, intended to perform a database check for any host systems that are missing updates
    and run the host_update script.
    """

    def __init__(self):
        """
        Initializes the UpdateHosts class.
        :return:
        """

        super(UpdateHosts, self).__init__()

        self.script_path = '/data/programs/usr/local/bin/update_linux_hosts/update_hosts.py'
        self.query = """SELECT COUNT(*) AS count
                        FROM host_package_versions AS hpv
                        LEFT JOIN current_package_versions AS cpv ON cpv.package_id = p.id
                        WHERE cpv.package_history_id != hpv.package_history_id"""

    def run(self):
        """
        Implementation of abstract run method from base.  When called, it attempts to run the job update_hosts job
        iff the query returns a non-zero number of systems that are out of date.
        :return:
        """

        print("Running Update Host with new packages at datetime {0}".format(datetime.datetime.now()))

        # Open connection to database.
        connection = mysql.connector.connect(option_files='/etc/update_linux_hosts/db_info/options.cnf')
        cursor = connection.cursor()

        # Date and time to check - now, but less a day.
        check_date = datetime.datetime.now() - datetime.timedelta(days=1)

        cursor.execute(self.query, (check_date, ))

        exists = cursor.fetchone()[0]

        if exists > 0:
            print("There are out of date systems!!!")
            subprocess.call(shlex.split(self.script_path))

        # Update the next run time.
        self.update_next_run()

        # Close db connection
        cursor.close()
        connection.close()

# run on main boilerplate.
if __name__ == "__main__":
    main()