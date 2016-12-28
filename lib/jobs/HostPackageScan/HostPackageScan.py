from looms.lib.jobs.jobs import Job
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