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

        self.log_string += "{0} - Running the Package Updater script.\n".format(datetime.datetime.now())

        try:
            subprocess.call(shlex.split(self.script_path))
        except subprocess.CalledProcessError as err:
            time = datetime.datetime.now()
            self.log_string += "{0} - The called program {1} exited with a non-zero " \
                               "return code.\n".format(time, self.script_path)
            self.log_string += "Return Code: {0}\n".format(err.returncode)
            self.log_string += "Error Message: {0}\n".format(err.message)
            self.log_string += "Program output: {0}\n".format(err.output)
            self.error_state = 1

        self.update_next_run()
