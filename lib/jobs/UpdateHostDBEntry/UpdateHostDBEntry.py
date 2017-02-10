from lib.jobs.jobs import Job
import os
import subprocess
import shlex
import datetime


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

        self.script_path = '/usr/lib/python2.7/site-packages/looms/sync_host_db/sync_host_db.py'
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
