from lib.jobs.jobs import Job
import subprocess
import shlex
import datetime


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

        self.script_path = '/usr/lib/python2.7/site-packages/looms/pkg_manager/pkg_manager.py'

    def run(self):
        """
        Executes the package manager.
        :return:
        """

        print("Running the Package Updater script at datetime: {0}".format(datetime.datetime.now()))

        subprocess.call(shlex.split(self.script_path))

        self.update_next_run()