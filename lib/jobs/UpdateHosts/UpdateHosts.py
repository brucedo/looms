from looms.lib.jobs.jobs import Job
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
