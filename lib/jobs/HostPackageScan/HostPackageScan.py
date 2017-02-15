from lib.jobs.jobs import Job
import mysql.connector
import datetime
import subprocess
import shlex


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
        self.script_path = '/usr/lib/python2.7/site-packages/looms/scan_host_pkgs/scan_host_pkgs.py'

    def run(self):
        """
        Checks to see if the database has any systems named in the hosts table that do NOT have any package
        entries in host_update_history, and if so adds those system's names to a list and calls pkg_scan_linux_host.py
        with them as an argument.
        :return:
        """

        self.log_string += "Running the host package scan at datetime: {0}\n".format(datetime.datetime.now())

        try:
            connection = mysql.connector.connect(option_files='/etc/looms/db_inf/host_package_scan_job.cnf')
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

        try:
            cursor.execute(self.query)
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

        # Execute the query
        machine_list = []
        for (machine_name, domain) in cursor:
            machine_list.append(machine_name + '.' + domain)

        if len(machine_list) <= 0:
            time = datetime.datetime.now()
            self.log_string += "{0} - No machines in the database require package scanning.\n".format(time)
            self.update_next_run()
            cursor.close()
            connection.close()
            return

        # Remove any extraneous commas.
        machine_str = ','.join(machine_list)

        time = datetime.datetime.now()
        self.log_string += "{1} - Found machines {0} to scan.".format(machine_str, time)

        cmd = self.script_path + ' ' + machine_str

        try:
            subprocess.call(shlex.split(cmd))
        except subprocess.CalledProcessError as err:
            time = datetime.datetime.now()
            self.log_string += "{0} - The called program {1} exited with a non-zero " \
                               "return code.\n".format(time, self.script_path)
            self.log_string += "Return Code: {0}\n".format(err.returncode)
            self.log_string += "Error Message: {0}\n".format(err.message)
            self.log_string += "Program output: {0}\n".format(err.output)
            self.error_state = 4

        self.update_next_run()

        cursor.close()
        connection.close()
