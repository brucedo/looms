#!/usr/bin/python

"""
Checks the database for a list of hosts who are not fully up to date, and proceeds to poll them via ansible and
then attempt to push the missing software updates to them.
"""

import logging
import logging.handlers
import datetime
import mysql.connector
import mysql.connector.errors
import shlex
import json
import subprocess

config = {}
logger = None
log_level = None

# Create some column ordering here, can modify it in get_outdated_systems if need be.
hostname_index = 0
domain_index = 1
package_name_index = 2
version_index = 3


def setup_logging():
    """
    Configures the logging module to handle our log output based on the information read in from the config file.

    :return: None
    """

    global logger
    global log_level
    global config

    conf_log_level = config['log_level']

    # Creates rotating file handler, with a max size of 10 MB and maximum of 5 backups, if path configured.
    if config['update_hosts_log_path'] != '':
        handler = logging.handlers.RotatingFileHandler(config['update_hosts_log_path'], mode='a',
                                                       maxBytes=10485760, backupCount=5)
    else:
        # if no file or path configured, we just spew to standard err.
        handler = logging.StreamHandler()

    if handler is None:
        print("A serious error occurred while attempting to create the logging handler.")

    # Create formatter...
    formatter = logging.Formatter(fmt='%(levelname)s %(asctime)s: %(name)s - %(module)s.%(funcName)s - %(message)s')
    handler.setFormatter(formatter)
    # now set log level, and configure the loggers for each imported local package (including gnupg.)
    # Y U NO SWITCH PTHON
    if conf_log_level == 'debug':
        log_level = logging.DEBUG
    elif conf_log_level == 'info':
        log_level = logging.INFO
    elif conf_log_level == 'warn':
        log_level = logging.WARN
    elif conf_log_level == 'error':
        log_level = logging.ERROR
    elif conf_log_level == 'critical':
        log_level = logging.CRITICAL
    else:
        log_level = logging.ERROR

    # create the logger for main, and set up the loggers for each of the supporting modules:
    logger = logging.getLogger(__name__)
    logger.addHandler(handler)
    logger.setLevel(log_level)

    if logger is None:
        print("A serious error occurred while attempting to generate a logger object.")


def read_config():
    """
    Reads the config file located in /etc/update_linux_hosts.  Config file currently consists of password related
    options and some logging odds and ends.  Config file is standard ini file style - option = value.  Note that
    the logger is not set up and cannot be until the config file is read, so all errors will be printed.
    :return:
    """

    global config

    # Attempt to open the config file.
    fstream = open('/etc/update_linux_hosts/update_linux_hosts.conf', 'r')

    data = fstream.read()
    fstream.close()

    # Read over each line.
    for line in data.split('\n'):
            # Deal with comments.
            if line.lstrip().startswith('#') or line.lstrip() == '':
                continue
            if line.find('=') < 0:
                print('Invalid line in config file; does not conform to OPTION = VALUE format: {0}'.format(line))
                continue

            pair = line.split('=')
            opts = pair[0].strip().lower()
            value = pair[1].strip()

            # Some option values we expect to be fully lowercase; some can have upper.
            if opts == 'log_level':
                config[opts] = value.lower()
            elif opts == 'update_hosts_log_path':
                config[opts] = value
            elif opts == 'ansible_vault_file':
                config[opts] = value
            elif opts == 'db_pass':
                config[opts] = value
            else:
                print('Unknown option {0} in config file.'.format(line))


def get_outdated_systems():
    """
    Runs a database query to request a list of systems with outdated packages on them.
    :return: List of machine names that are out of date.
    """

    logger.debug('Executing query to get list of systems that have outdated packages...')

    # Here's the query - it's a doozy.
    query = """SELECT DISTINCT hpv.name AS machine_name, hpv.domain AS domain
               FROM host_package_versions AS hpv
               LEFT JOIN current_package_versions AS cpv ON cpv.package_name = hpv.package_name
               AND cpv.package_contents = hpv.contents
               LEFT JOIN host AS h ON hpv.name = h.name
               WHERE cpv.event_date > hpv.event_date AND h.last_checkin > %s
               AND (h.last_update < CURRENT_TIMESTAMP() - INTERVAL 1 DAY)"""

    # Get the date and time of now minus 1 day.
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    logger.debug('Getting datetime value to put into query: {0}'.format(yesterday))

    connection = mysql.connector.connect(option_files='/etc/update_linux_hosts/db_info/options.cnf')
    cursor = connection.cursor()

    cursor.execute(query, (yesterday, ))
    record_set = []

    for (machine_name, domain) in cursor:
        record_set.append((machine_name, domain))

    cursor.close()
    connection.close()

    return record_set


def convert_records(record_set):
    """
    Takes a comma separated string of records produced by the query in get_outdated_systems() and creates an
    association of package_name and version to machine and domain name, ready for injection into ansible commands.
    :param record_set: The list of records in string form, rows separated by newlines, columns by ',' or ', ' that
    we need to translate into a more usable form.
    :return: A dictionary of packages names, where each name references a nested list structure.  The first element
    in the outermost list is the version of that package that we will update to.  The second element is a
    nested list, containing a tuple of machine names and domains that uniquely identify a machine that must be
    updated.
    """

    # Create an empty dictionary.  We use the dictionary because, in most cases, multiple machines are going
    # to need to update the same package, so we need to reference the same package repeatedly to add the new systems
    # to it.
    package_dict = {}

    # Iterate across the rows of the record set.
    for row in record_set.split('\n'):
        # Deal with blank lines from the cursor readout...
        if row.strip() == '':
            continue
        # Break each row into it's component pieces:
        columns = [x.strip() for x in row.split(',')]
        # Now, create my entries for the dictionary.
        package = columns[package_name_index]
        version = columns[version_index]
        host = columns[hostname_index]
        domain = columns[domain_index]

        # Check if the package has been identified already.
        if package in package_dict:
            # if it has, then we're just adding the machine name to the entry.
            package_dict[package][1].append((host, domain))
        else:
            # if not, we're creating a whole new entry.
            package_dict[package] = [version, [(host, domain)]]

    return package_dict


def update_systems(package_name, version, machine_list):
    """
    Takes a package name and its version, and an associated list of machine/domain pairs, and calls ansible
    apt to update that package on all of the systems.
    :param package_name: Name of the package to be updated.
    :param version: Version of the package to update to.
    :param machine_list: indexable list of indexable machine/domain pairs.  The underlying structures do not
    matter, so long as the outer list is iterable and the inner indexable with standard [] notation.
    :return: The text output of the ansible command, usable to identify which machines updated and which did not.
             None in the event that the ansible command could not be run.
    """

    global logger
    global config

    # Compose our package and version string...
    package_str = package_name + '=' + version

    if len(machine_list) == 0:
        logger.error('No machines in the machine_list - nothing to update!')
        return None

    # Compose the machine string.
    machine_str = machine_list[0][0].upper() + '.' + machine_list[0][1].upper()
    for machine in machine_list[1:]:
        # Each machine in the list is a tuple of the form (hostname, domain).  We want the full thing, as all
        # computers are labelled by their FQDN in uppercase.
        machine_str += "," + machine[0].upper() + '.' + machine[1].upper()

    cmd = 'ansible -m apt {0} --vault-password-file "{1}" ' \
          '--args="name={2} state=present"'.format(machine_str, config['ansible_vault_file'], package_str)

    try:
        output = subprocess.check_output(shlex.split(cmd))
    except subprocess.CalledProcessError as err:
        print("Exception occurred during subprocess call - result was {0}.".format(err.output))
        print("Command executed: {0}".format(err.cmd))
        print("Arguments passed: {0}".format(err.args))

    return output


def parse_apt_output(apt_output):
    """
    Takes a string of data from running the ansible apt module in a playbook with verbose, and breaks it up into
    a list of records, where each record specifies a hostname and contains a list of packages and versions that
    the host was updated to.
    :param apt_output: String in format provided by ansible's playbook.
    :return: dictionary keyed by hostname, value is a list of package name, arch, version triples for pushing back to
    the database to indicate which packages the host has been updated with.
    """

    # Break the input down by newline.
    apt_list = apt_output.split('\n')

    update_dict = {}

    mode = ''

    # iterate over list to identify the Task that we care about.
    for line in apt_list:
        if line == '':
            mode = ''
            continue
        elif line.find('TASK [Update the remote system after updating the cache.]') >= 0:
            mode = 'update_task'
            continue

        if mode == 'update_task':
            # update task lines are one single line per host, format "ok <hostname> => {<data>}
            # Get the hostname out first.
            host_start = line.find('[')
            host_end = line.find(']', host_start)
            hostname = line[host_start+1:host_end]

            # Pre-load the update dictionary with the machine.
            update_dict[hostname] = []

            # We should also check the status of the attempt.  If it failed, then the list for the machine will
            # be empty.  The system itself may have received partial updates, but that's too complicated to work
            # out here simply because we don't know exactly what the error messages for each package are going to be.
            # Redoing a full inventory becomes the easiest option.
            if not (line.startswith('ok:') or line.startswith('changed:')):
                logger.error('Unable to properly execute update on system {0} - check manually!'.format(hostname))
                reason = line[host_end + 2:line.find(' =>')]
                logger.error('Reason for failure: {0}'.format(reason))
                continue

            # Now get the JSON component.  JSON does not like newlines included in the string (even escaped ones)
            # so we have to escape the already escaped newlines.  yeeesh.
            json_obj = json.loads(line[line.find('{'):].replace('\n', '\\n'))

            # We can now use the properties of the JSON object.  First check to see if any packages were updated:
            if json_obj['changed']:
                # Go over the lines in json_obj['stdout_lines']
                for element in json_obj['stdout_lines']:
                    # Mostly dross - what we care about right now is the Get: lines.
                    if element.startswith('Get:'):
                        # The line parts are differentiated by whitespace.
                        parts = element.split()
                        # The line format is "Get:<#> <repo_addr> <component/category> package arch version size units
                        # We care about package, arch, and version.
                        update_dict[hostname].append((parts[-5], parts[-4], parts[-3]))

                # We're sort of blindly trusting that all packages install here.  However, if it failed on a package
                # then the status indicator at the start of the line should not have been "ok" or "changed."

    return update_dict


def confirm_package_details(package, arch, version):
    """
    Checks to confirm that a given package exists in the database, and if it does whether or not the specified
    version and architecture exist in the package_history table.  If they do not, then they will be inserted
    as provisional entries until they are validated.
    :param package: Name of package to check.
    :param arch: Architecture of the package.
    :param version: Version of the package.
    :return: True if the package exists or else could be inserted into the update history table; false if the
    package does not exist in the database as we cannot proceed - package may be unallowed.
    """

    # Establish db connection.
    connection = mysql.connector.connect(option_files='/etc/update_linux_hosts/db_info/options.cnf')
    cursor = connection.cursor()

    # First check if the package exists..
    query = """SELECT COUNT(*) FROM package AS p WHERE p.package_name = %s AND p.contents = %s"""

    cursor.execute(query, (package, arch))
    exists = cursor.fetchone()[0]

    if exists <= 0:
        cursor.close()
        connection.close()
        logger.error('Package {0}:{1} does not exist in database - may be unauthorized.'.format(package, arch))
        return False

    # Check to see if there's an entry in the version history table...
    query = """SELECT COUNT(*) FROM package AS p LEFT JOIN package_history AS ph ON p.id = ph.package_id
               WHERE p.package_name = %s AND ph.version = %s AND p.contents = %s"""

    cursor.execute(query, (package, version, arch))
    exists = cursor.fetchone()[0]

    if exists > 0:
        logger.debug('At least one package {0} of version {1} exists in the database.  '
                     'Nothing to add.'.format(package, version))
        cursor.close()
        connection.close()
        return True
    else:
        # No entry - add one.
        query = """INSERT INTO package_history (package_id, version, event_type)
                   SELECT package.id, %s, %s
                   FROM package
                   WHERE package.package_name = %s AND package.contents = %s"""

        cursor.execute(query, (version, 'provisional', package, arch))

        # Commit the update
        connection.commit()

    cursor.close()
    connection.close()

    return True


def insert_host_pkg_update(host_dict):
    """
    Takes a dictionary keyed by host, with the associated value being a list containing a triple of package,
    architecture and version, and inserts updates into the database host_update_history table to denote the update
    we've just made.  If the package version itself does not exist in the database, then a version entry is
    created.
    :param host_dict: Dictionary containing all updated systems and the packages/versions they were updated to.
    :return:
    """

    # Establish db connection.
    connection = mysql.connector.connect(option_files='/etc/update_linux_hosts/db_info/options.cnf')
    cursor = connection.cursor()

    # Iterate across the host package list.
    for fqdn_host, package_data in host_dict.iteritems():
        # And now to iterate across the package information in package_data
        hostname = fqdn_host.split('.', 1)[0]
        domain = fqdn_host.split('.', 1)[1]
        for (package, arch, version) in package_data:
            # translate the arch to what's used in the database:
            if arch == "i386":
                arch = "binary-i386"
            else:
                arch = "binary-amd64"

            exists = confirm_package_details(package, arch, version)

            if not exists:
                logger.error("Package {0} on host {1}, version {2} does not exist in database.  May be unauthorized."
                             "Leaving this package unrecorded...".format(package, fqdn_host, version))
                continue

            # Package exists, version exists in version info.  Add this entry to the host update history...
            query = """INSERT INTO host_update_history (host_id, package_history_id, package_id)
                       SELECT h.id, ph.id, p.id
                       FROM host as h, package_history as ph, package as p
                       WHERE h.name = %s AND h.domain = %s AND p.package_name = %s AND p.contents = %s
                       AND p.package_type = %s AND ph.package_id = p.id AND ph.version = %s
                       AND ph.event_date = (SELECT MAX(ph.event_date) FROM package AS p LEFT JOIN
                       package_history AS ph ON p.id = ph.package_id WHERE p.package_name = %s AND
                       ph.version = %s)"""

            # Extract the specific variable data we need.
            pkg_type = 'debian'

            cursor.execute(query, (hostname, domain, package, arch, pkg_type, version, package, version))
            connection.commit()

        # Update the host table so that the updated field matches the current timestamp.
        query = """UPDATE host SET host.last_update = CURRENT_TIMESTAMP() WHERE host.name = %s AND host.domain = %s"""
        cursor.execute(query, (hostname, domain))
        connection.commit()

    cursor.close()
    connection.close()




def main():
    """
    Entry point into update script.
    :return:
    """

    # Read configs, set up logs etc.
    read_config()
    setup_logging()

    # Get a database list of systems that are not fully up to date, limited by last-contact-date of within same day.
    record_set = get_outdated_systems()

    print('Record set: {0}'.format(record_set))
    print("Done.")
    # Filter through the database results and invert the data retrieved - we want to apply packages one at a time
    # to as many machines as they should be applied to.
    # machine_list = convert_records(record_set)

    # join the machine list/domain name tuples and then string together into comma separated string form.
    env_machine = "hostnames=" + ','.join(['.'.join(x) for x in record_set])
    # Ansible command:
    cmd = 'ansible-playbook /data/ansible/playbooks/test/daily_update.yaml --vault-password-file {1} ' \
          '--verbose --extra-vars="{0}"'.format(env_machine, config['ansible_vault_file'])
    try:
        output = subprocess.check_output(shlex.split(cmd))
    except subprocess.CalledProcessError as err:
        logger.error("One or more systems could not properly perform an update to all of their existing packages.")
        output = err.output

    # Turn the apt output into a reasonable format to work with.
    host_dict = parse_apt_output(output)

    # Time to update the database with the updates made.
    insert_host_pkg_update(host_dict)

    # print("testing conversion:")
    # for package, data in package_dict.iteritems():
    #    print("{0}, version {1} installed on systems: {2}".format(package, data[0], data[1]))

    # Now iterate over packages.
#    for package, data in package_dict.iteritems():
        # Call ansible -m apt, providing the package name to update along with version in the --args option, and
        # be sure to capture the output.
#        ansible_out = update_systems(package, data[0], data[1])
#        print("Package {0} produced: {1}".format(package, ansible_out))

        # Examine the output for each machine, ensuring that a "changed" response has been provided.

        # Update the database for the package, version and each machine that was successfully updated.


if __name__ == "__main__":
    main()