from lib.jobs.jobs import Job
import datetime
import mysql.connector
import subprocess
import shlex


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

        self.script_path = '/usr/lib/python2.7/site-packages/looms/host_updater/host_updater.py'
        self.query = """SELECT COUNT(*) AS count
                        FROM host_package_versions AS hpv
                        LEFT JOIN current_package_versions AS cpv ON cpv.package_id = hpv.package_id
                        WHERE cpv.package_history_id != hpv.package_history_id"""

    def run(self):
        """
        Implementation of abstract run method from base.  When called, it attempts to run the job update_hosts job
        iff the query returns a non-zero number of systems that are out of date.
        :return:
        """

        self.log_string += "{0} - Running Update Host with new packages\n".format(datetime.datetime.now())

        # Open connection to database.
        try:
            connection = mysql.connector.connect(option_files='/etc/looms/db_inf/update_host_job.cnf')
        except mysql.connector.Error as err:
            time = datetime.datetime.now()
            self.log_string += "{0} - An error occurred when establishing a connection to the database.\n".format(time)
            self.log_string += "Error Number: {0}\n".format(err.errno)
            self.log_string += "SQLSTATE: {0}\n".format(err.sqlstate)
            self.log_string += "Error Message: {0}\n".format(err.msg)
            self.error_state = 1
            self.update_next_run()
            return

        try:
            cursor = connection.cursor()
        except mysql.connector.Error as err:
            time = datetime.datetime.now()
            self.log_string += "{0} - An error occurred while creating a cursor.\n".format(time)
            self.log_string += "Error Number: {0}\n".format(err.errno)
            self.log_string += "SQLSTATE: {0}\n".format(err.sqlstate)
            self.log_string += "Error Message: {0}\n".format(err.msg)
            self.error_state = 2
            self.update_next_run()
            connection.close()
            return

        # Date and time to check - now, but less a day.
        check_date = datetime.datetime.now() - datetime.timedelta(days=1)

        try:
            cursor.execute(self.query, (check_date, ))
        except mysql.connector.Error as err:
            time = datetime.datetime.now()
            self.log_string += "{0} - An error occurred while executing query \"{1}\".\n".format(time, self.query)
            self.log_string += "Error Number: {0}\n".format(err.errno)
            self.log_string += "SQLSTATE: {0}\n".format(err.sqlstate)
            self.log_string += "Error Message: {0}\n".format(err.msg)
            self.error_state = 3
            self.update_next_run()
            cursor.close()
            connection.close()
            return

        exists = cursor.fetchone()[0]

        if exists > 0:
            time = datetime.datetime.now()
            self.log_string += "{0} - There are out of date systems!!!\n".format(time)

            try:
                subprocess.call(shlex.split(self.script_path))
            except subprocess.CalledProcessError as err:
                time = datetime.datetime.now()
                self.log_string += "{0} - The called program {1} exited with a non-zero " \
                                   "return code.\n".format(time, self.script_path)
                self.log_string += "Return Code: {0}\n".format(err.returncode)
                self.log_string += "Error Message: {0}\n".format(err.message)
                self.log_string += "Program output: {0}\n".format(err.output)
                self.error_state = 4

        # Update the next run time.
        self.update_next_run()

        # Close db connection
        cursor.close()
        connection.close()
