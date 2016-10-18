#!/usr/bin/python2

"""
This is an application designed to update Linux hosts via the Ansible system, and record the package versions
that each remote system was updated to.  These package lists will be used to provide reports back to security
regarding the version of various packages for which extreme security threats have been registered.

The application will also acquire a package "current state" of a system for which it has no existing record, and
record that current state in the database as a starting point for future updates.
"""

import logging
import logging.handlers
import sys
import subprocess
import shlex
import mysql.connector
import mysql.connector.errors
import json

config = {}
logger = None
log_level = None


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
    if config['log_path'] != '':
        handler = logging.handlers.RotatingFileHandler(config['log_path'], mode='a',
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
            elif opts == 'log_path':
                config[opts] = value
            elif opts == 'ansible_vault_file':
                config[opts] = value
            elif opts == 'db_pass':
                config[opts] = value
            else:
                print('Unknown option {0} in config file.'.format(line))


def parse_dpkg_run(dpkg_output):
    """
    Reads through the output of the dpkg run, creating a list of packages for each machine that actively responded,
    and providing error reports for each machine that did NOT respond.
    :param dpkg_output:  The output of the ansible dpkg run to be parsed for package data.
    :return: A dictionary containing system names as keys, and lists of lists as values (where each sublist is
    version and architecture data for a single package.)
    """

    # Dictionary with machine names (fully qualified) as keys and a list of lists as values, where each sublist
    # represents one package, containing package name, version, and architecture.
    dpkg_dict = {}

    # Use a state machine approach to reading in the contents of a return; assume we're starting a new record
    # where each record is the response from ansible for a single machine.
    state = 'new'
    json_str = ''
    brace_count = 0
    system_name = ''
    query_status = ''
    # Iterate over the lines.
    for line in dpkg_output.split('\n'):
        # Because python removes the double newlines that denote end of stdout from ansible's command module,
        # we must explicitly check for the keywords that indicate a new record here.
        if (line.lower().find('agr.gc.ca') >= 0 or line.lower().find('cfia-acia.inspection.gc.ca') >= 0) and\
           (line.find('>>') >= 0 or line.find('=>') >= 0):
            # The JSON and stdout handlers below are intended to be fully independent - there should be zero cleanup
            # to be done here or in the new record handler, so we just set the state to new and move on.
            state = 'new'

        # Check our state and handle the line appropriately.
        if state == 'new':
            # Line appears to match agr or cfia system status report line.  Extract system name, process rest.
            parts = line.split('|')
            # Remove any whitespace from the individual elements of the parts list.
            parts = [x.strip() for x in parts]
            # First element = machine name.  Every part of this system is standardized on upper case machine names.
            system_name = parts[0].upper()
            # Second element = query status plus possibly some other junk.  We only care about "SUCCESS" which
            # should come back clear.
            query_status = parts[1].upper()
            dpkg_dict[system_name] = []
            # Keep the machine name on hand for re-use later.
            # Now process the rest of the line to determine if we're expecting JSON or stdout.
            if line.endswith('=> {'):
                # JSON output!
                state = 'json'
                json_str = '{'
                brace_count = 1
            else:
                # If not JSON, then it ran dpkg and we have stdout to parse.
                state = 'stdout'
        elif state == 'json':
            # Add the line to the json_str regardless of whether it's the final line or not.
            json_str += '\n' + line
            # Count the curly braces in the line.
            brace_count += line.count('{')
            brace_count -= line.count('}')
            if brace_count < 0:
                # Should never be less than.
                logger.error('JSON string from ansible has more closing } than opening {.  There may be a problem.')
                logger.error('{0}'.format(json_str))
            elif brace_count == 0:
                # End of JSON string.  In a more general service we could push the string into a JSON dict and pull
                # structured data out of it, but here we just don't care.  Spit the string to log as an error.
                logger.error('JSON string returned instead of stdout string - an error occurred during read.')
                logger.error('{0}'.format(json_str))
        elif state == 'stdout':
                # Perform the stdout parsing only in the event that the return value is SUCCESS.
                if query_status == 'SUCCESS':
                    # We're only interested in lines starting with ii, for fully installed packages.
                    if line.startswith('ii'):
                        # Split the line up on whitespace.
                        parts = line.split()
                        # Some packages have the package architecture installed in the package name.  Remove it.
                        if parts[1].count(':') > 0:
                            parts[1] = parts[1][0:parts[1].find(':')]
                        # Every package has a name, version and architecture and none of those three may contain
                        # whitespace.  It's safe, therefore, to blindly take parts[1:4].
                        dpkg_dict[system_name].append(parts[1:4])

    return dpkg_dict


def get_target_dpkg(target):
    """
    Calls ansible and instructs it to use the command module to run a remote dpkg -l.  The output is captured
    and returned as a string to the caller for full processing.
    :param target: The Ansible compatible pattern that identifies the set of PCs to run the command against.
    :return:
    """

    global config
    cmd_string = 'ansible -m command {0} --vault-password-file "{1}" ' \
                 '--args="dpkg -l"'.format(target, config['ansible_vault_file'])

    logger.debug('Beginning dpkg list of target {0}.'.format(target))

    try:
        stdout = subprocess.check_output(shlex.split(cmd_string))
    except subprocess.CalledProcessError as cperr:
        logger.error("An error occurred while attempting to run dpkg -l through ansible's command module.")
        logger.error("Process return code: {0}".format(cperr.returncode))
        logger.error("Result may be because one or more machines was unresponsive to ansible - pressing on.")
        stdout = cperr.output

    logger.debug('Subprocess run, output captured.')

    return stdout


def check_db_for_host(machine_name):
    """
    Checks the host table in the database for the system name for a specific system name to see if it has an entry
    in it.  If it does, return True.  If not, return False.
    :param machine_name: The name of the system to search the database for.
    :return: True: The named system exists in the database.
             False: The named system does not exist in the database, or exists multiple times in the database hosts
             table (it should only exist one time; the column itself is unique keyed.)
    """

    # Establish our MySQL db connection.
    connection = mysql.connector.connect(option_files='/etc/update_linux_hosts/db_info/options.cnf')
    cursor = connection.cursor()

    # Check whether we have FQDN or short hostname.
    if machine_name.find('.') >= 0:
        machine_name = machine_name.split('.')[0]

    query = """SELECT COUNT(*) FROM host WHERE host.name = %s"""
    cursor.execute(query, (machine_name, ))

    # Only getting COUNT back, no named column - should be only one row returned and only one column; should be
    # guaranteed to get exactly one row and exactly one column.
    exists = cursor.fetchone()[0]

    # Close the data stream off.
    cursor.close()
    connection.close()

    print("Count of {0}: {1}".format(machine_name, exists))

    if int(exists) == 1:
        return True
    else:
        return False


def check_db_for_host_pkg(machine_name, package_name, package_version):
    """
    Checks the database host_update_history and package tables to determine if a package of the given name exists
    and if it has been associated with the system.
    :param machine_name: Name of the system to check the package against.
    :param package_name: Name of the package we want to check.
    :param package_version: The version of the package that is currently installed on the machine.
    :return: 0 if the package exists in the database and a valid version from the package_history table has been
    associated with the host.
            -1 if the package exists in the database and no version from the package_history table has been
    associated with the host.
            -2 if the package does not exist in the database.
    """

    connection = mysql.connector.connect(option_files='/etc/update_linux_hosts/db_info/options.cnf')
    cursor = connection.cursor()

    query = """SELECT COUNT(*) FROM host AS h LEFT JOIN host_update_history AS huh ON h.id = huh.host_id
               LEFT JOIN package_history AS ph ON huh.package_history_id = ph.id
               LEFT JOIN package AS p ON huh.package_id = p.id
               WHERE h.name = %s AND h.domain = %s AND p.package_name = %s AND ph.version = %s"""

    hostname = machine_name.split('.', 1)[0]
    domain = machine_name.split('.', 1)[1]

    logger.debug("Query: {0}".format(query % (hostname, domain, package_name, package_version)))

    cursor.execute(query, (hostname, domain, package_name, package_version))

    exists = cursor.fetchone()[0]

    # In this scenario, we're not locking down a specific version being associated with the host, so multiple
    # rows could be identified.  This is fine - we want to make sure there's at least one, not that there's only one.
    if exists > 0:
        logger.debug("At least one package and package history row has been associated with host.")
        retval = 0
    else:
        logger.debug("No package and/or package history ahve been associated with host.  Digging...")
        # If no rows are returned, we could have either no version associated, or else no package name in the package
        # table.
        query = """SELECT COUNT(*) FROM package AS p WHERE p.package_name = %s"""
        logger.debug("Query: {0}".format(query % (package_name, )))
        cursor.execute(query, (package_name, ))
        exists = cursor.fetchone()[0]

        if exists > 0:
            logger.debug("The package name {0} exists in package table.  "
                         "No association is the problem.".format(package_name))
            retval = -1
        else:
            logger.debug("The package name {0} does not exist in package table.".format(package_name))
            retval = -2

    cursor.close()
    connection.close()

    return retval


def query_system_setup(machine_name):
    """
    Runs the ansible setup module on a specific remote machine to gather data about that system so that the local
    host table can be updated.
    :param machine_name: Name of the system whose setup data is to be retrieved.
    :return: A json dictionary object if successful; None if not.
    """

    cmd = 'ansible -m setup {0} --vault-password-file "{1}"'.format(machine_name, config['ansible_vault_file'])

    try:
        setup_out = subprocess.check_output(shlex.split(cmd))
    except subprocess.CalledProcessError as err:
        logger.error('The Ansible subprocess call returned a non-zero value.  However, this does not mean'
                     'the lookup was a failure.  Capturing returned data and examining.')
        setup_out = err.output

    # Read through the output.  First line should be status of request with an opening {.
    setup_data = setup_out.split('\n')
    status_line = setup_data[0]

    # Check status.
    status = status_line.split('|')
    status = [x.strip() for x in status]

    if not status[1].startswith('SUCCESS'):
        logger.error('Something went wrong with the Ansible setup command.  No data to parse.')
        logger.error('{0}'.format(setup_out))
        return None

    # Interpret the JSON.  The status line includes the leading '{'.
    json_str = '{'
    brace_count = 1
    for line in setup_data[1:]:
        # simple sanity check - if the brace count is not zero at the end then the data was malformed.
        brace_count += line.count('{')
        brace_count -= line.count('}')

        json_str += '\n' + line

    if brace_count != 0:
        logger.error("The JSON data from the Ansible setup call must be malformed - non-zero {} count at completion.")
        return None

    return json.loads(json_str)


def insert_new_host(machine_name, setup_data):
    """
    Attempts to insert a host into the hosts table if it does not already exist.
    :param machine_name: Fully qualified domain name for a host that we are attempting to insert.
    :param setup_data: Data extracted from the target system via ansible's setup module.  Expected to be dictionary.
    :return: True on successful update
             False on failed update.
    """

    # Open connection to MySQL
    connection = mysql.connector.connect(option_files='/etc/update_linux_hosts/db_info/options.cnf')
    cursor = connection.cursor()

    query = """INSERT INTO host (name, domain, os_name, os_version, dist_name, dist_ver)
                                 VALUES (%s, %s, %s, %s, %s, %s)"""

    # Get the hostname and domain separate.
    hostname = machine_name.split('.', 1)[0]
    domain = machine_name.split('.', 1)[1]
    facts = setup_data['ansible_facts']

    try:
        # Run the insert with the data we have.
        cursor.execute(query, (hostname, domain, facts['ansible_system'], facts['ansible_kernel'],
                               facts['ansible_lsb']['id'], facts['ansible_lsb']['description']))
        # and commit it.
        connection.commit()

    except mysql.connector.errors.IntegrityError as err:
        logger.error('Database integrity constraints violated by insert of host {0}.'.format(machine_name))
        logger.error('Error code: {0}'.format(err.errno))
        logger.error('SQLState: {0}'.format(err.sqlstate))
        logger.error('Error Message: {0}'.format(err.msg))
    except mysql.connector.errors.DataError as err:
        logger.error('Invalid data type error when inserting host {0} into hosts table.'.format(machine_name))
        logger.error('Error code: {0}'.format(err.errno))
        logger.error('SQLState: {0}'.format(err.sqlstate))
        logger.error('Error Message: {0}'.format(err.msg))
    finally:
        logger.error('Unknown exception occurred inserting host {0} into hosts table.'.format(machine_name))

    # close db connection.
    cursor.close()
    connection.close()


def insert_package_version(pkg_data_list, ansible_facts):
    """
    Given a list consisting of package_name, version, and architecture, insert the package version into the package
    history table.  Use a date corresponding to the epoch and a type of "provisional" to ensure that we can
    readily identify odd package versions and so that the new addition does not clobber the most recent package
    added.
    :param pkg_data_list: Package name, version, and architecture so we can add it to the system.
    :param ansible_facts: dictionary of facts provided by ansible about the target system.
    :return:
    """

    logger.debug('Attempting to insert package data {0} into '
                 'package_history table provisionally.'.format(pkg_data_list[0]))

    # Establish db connection.
    connection = mysql.connector.connect(option_files='/etc/update_linux_hosts/db_info/options.cnf')
    cursor = connection.cursor()

    facts = ansible_facts['ansible_facts']

    if facts['ansible_distribution'].lower() == 'ubuntu':
        pkg_type = 'debian'
    else:
        logger.error('Unknown type - using default of debian.')
        pkg_type = 'debian'

    if facts['ansible_machine'].lower() == 'x86_64':
        arch = 'binary-amd64'
    elif facts['ansible_machine.lower()'] == 'i386':
        arch = 'binary-i386'
    else:
        logger.error('Unknown hardware type - using default of amd64.')
        arch = 'binary-amd64'

    # Pick the provisional date of the epoch.
    date = '1970-01-01 00:00:00'

    # And mark out the event type.  'provisional' chosen because this entry may not be confirmed valid by a human
    # user.
    event_type = 'provisional'

    # First check if the package and version exists in package_history and package - if it's there, we don't need
    # to add it!
    query = """SELECT COUNT(*) FROM package AS p LEFT JOIN package_history AS ph ON p.id = ph.package_id
               WHERE p.package_name = %s AND ph.version = %s AND p.contents = %s"""

    cursor.execute(query, (pkg_data_list[0], pkg_data_list[1], arch))
    exists = cursor.fetchone()[0]

    if exists > 0:
        logger.debug('At least one package {0} of version {1} exists in the database.  '
                     'Nothing to add.'.format(pkg_data_list[0], pkg_data_list[1]))
        cursor.close()
        connection.close()
        return

    # If it doesn't exist, we go ahead with our insert.
    query = """INSERT INTO package_history (package_id, event_date, version, event_type)
               SELECT package.id, %s, %s, %s
               FROM package
               WHERE package.package_name = %s AND package.package_type = %s AND package.contents = %s"""

    cursor.execute(query, (date, pkg_data_list[1], event_type, pkg_data_list[0], pkg_type, arch))
    connection.commit()

    cursor.close()
    connection.close()


def insert_pkg_host_association(machine_name, pkg_data_list, machine_data):
    """
    Creates an entry in the host_update_history table, associating a given host with a given package AND a particular
    package update entry.
    :param machine_name: Fully qualified name of the machine we're trying to associate with package data.
    :param pkg_data_list: A list of package name, version, and architecture from the system's dpkg output.
    :param machine_data: Information provided by ansible about the client system.
    :return:
    """

    logger.debug("Inserting pkg/host association.")

    connection = mysql.connector.connect(option_files='/etc/update_linux_hosts/db_info/options.cnf')
    cursor = connection.cursor()

    query = """INSERT INTO host_update_history (host_id, package_history_id, package_id)
               SELECT h.id, ph.id, p.id
               FROM host as h, package_history as ph, package as p
               WHERE h.name = %s AND h.domain = %s AND p.package_name = %s AND p.contents = %s
               AND p.package_type = %s AND ph.package_id = p.id AND ph.version = %s
               AND ph.event_date = (SELECT MAX(ph.event_date) FROM package AS p LEFT JOIN
               package_history AS ph ON p.id = ph.package_id WHERE p.package_name = %s AND
               ph.version = %s)"""

    # Extract the specific variable data we need.
    hostname = machine_name.split('.', 1)[0]
    domain = machine_name.split('.', 1)[1]
    pkg_name = pkg_data_list[0]

    if pkg_data_list[2] == 'i386':
        pkg_contents = 'binary-i386'
    else:
        pkg_contents = 'binary-amd64'

    pkg_type = 'debian'
    pkg_version = pkg_data_list[1]

    logger.debug('Query: {0}'.format(query % (hostname, domain, pkg_name, pkg_contents, pkg_type, pkg_version, pkg_name, pkg_version)))

    cursor.execute(query, (hostname, domain, pkg_name, pkg_contents, pkg_type, pkg_version, pkg_name, pkg_version))

    # We have the host->package version association created in the history table.  We need the same in the
    # host_package_versions table, because that IS the currently known package version associated with this host.
    query = """INSERT INTO host_package_versions (host_id, package_id, package_history_id)
               SELECT h.id, p.id, ph.id
               FROM host AS h, package_history AS ph LEFT JOIN package AS p ON ph.package_id = p.id
               WHERE h.name = %s AND h.domain = %s AND p.package_name = %s AND p.contents = %s
               AND p.package_type = %s AND ph.version = %s
               ON DUPLICATE KEY UPDATE package_history_id = ph.id"""
    cursor.execute(query, (hostname, domain, pkg_name, pkg_contents, pkg_type, pkg_version))

    # Commit the changes made.
    connection.commit()

    cursor.close()
    connection.close()


def main():
    """
    Launches the updater; the updater will read a short configuration file telling it what passwords to use for
    the Ansible vault (STRONGLY PROTECT THIS) and what password to use for the database (STRONGLY PROTECT THIS.)
    The Ansible group(s)/PC(s) to update should be passed on the command line.  No other options are expected
    for the updater.
    :return:
    """

    # Read in the config file.
    read_config()

    # Set up logging.
    setup_logging()

    # And get ansible group(s) and/or PC(s) to execute this across.
    target_list = sys.argv[1:]

    # Iterate over systems in the target_list.
    for target in target_list:
        # Get the text report from the dpkg -l on the target set.
        result = get_target_dpkg(target)

        # Break the text report down into successful runs and failures.  Return a dictionary value, where
        # the keys are the full system names, and the values are lists of package, version, architectures.
        dpkg_dict = parse_dpkg_run(result)
        # Process each machine one at a time
        for machine in dpkg_dict:
            # Check to see if they exist in host table.
            logger.debug("Checking machine {0}".format(machine))
            logger.debug("Number of packages for machine: {0}".format(len(dpkg_dict[machine])))
            if check_db_for_host(machine):
                logger.debug('Machine {0} exists in the database - no need to add it.'.format(machine))
            else:
                # if not, pull down setup information and jsonify it.
                setup_json = query_system_setup(machine)
                if not setup_json:
                    logger.debug("None object was returned, cannot add setup data about system.")
                    continue

                # Add the host + data from setup.
                insert_new_host(machine, setup_json)

            # Check each package for the machine.
            for package_data in dpkg_dict[machine]:
                # Quick check to confirm no empty packages have been included.
                if len(package_data) <= 0:
                    logger.debug('Empty package somehow slipped into package data for {0}'.format(machine))
                    continue
                # If package is not currently associated with machine, add it.
                associated = check_db_for_host_pkg(machine, package_data[0], package_data[1])
                print('Package {0} associated with machine {1}: {2}'.format(package_data[0], machine, associated))
                if associated == -2:
                    # If package does not exist, report it - future check for unauthorized software.
                    print('Package {0} DOES NOT EXIST IN THE DATABASE!  This could be an unauthorized software '
                          'package.  Please investigate.'.format(package_data[0]))
                    logger.warn('Package {0} DOES NOT EXIST IN THE DATABASE!  This could be an unauthorized '
                                'software package.  Please investigate.'.format(package_data[0]))
                elif associated == -1:
                    # Package association does not exist in database.  Add it if we can.  May need to add the package
                    # version - to do this, we need to identify the package and so need machine architecture data.
                    try:
                        # setup_json may not exist yet, so wrap this in try statement to catch that exception.
                        if not setup_json:
                            setup_json = query_system_setup(machine)
                    except NameError:
                        # if setup_json was not assigned above, try it now.
                        setup_json = query_system_setup(machine)

                    if not setup_json:
                        # query_system_setup returned None.  We cannot go forward.
                        logger.error('Attempting to associate package {0} with machine {1}, '
                                     'but cannot gather machine facts.  Cannot uniquely '
                                     'identify package type and architecture.'.format(package_data[0], machine))
                        break
                    # If the package version does not exist, add it - just need event type ('add'), name, version and
                    # architecture.
                    insert_package_version(package_data, setup_json)
                    # Associate the package version and package with the host in host_update_history.
                    insert_pkg_host_association(machine, package_data, setup_json)
                # We're not worrying about performing updates - this is just to ensure that all package data is
                # properly sync'd.  At this point all packages on host systems should be listed in the db.  Done.


# Start the main() function if this module is run directly.
if __name__ == '__main__':
    main()
