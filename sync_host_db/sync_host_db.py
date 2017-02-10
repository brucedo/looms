#!/usr/bin/python
"""
Reads in the contents of the registered hosts file that the reg_host.php script generates, JSON record by JSON record,
and updates the status of the system in the database based on the information it pulls out of the file.
"""

import mysql.connector
import mysql.connector.errors
import json
import logging
import logging.handlers
import os
import os.path
import socket
import subprocess
import shlex
import shutil
import pwd
import grp
import datetime

allowed_opts = ['log_level', 'log_path', 'db_name', 'db_pass',
                'reg_host_file', 'ansible_environment', 'opts_file']
config = {}

logger = ""
log_level = ""


def read_config(path='/etc/looms/sync_host_db.conf'):
    """
    Reads the config file located in /etc/update_linux_hosts.  Config file currently consists of password related
    options and some logging odds and ends.  Config file is standard ini file style - option = value.  Note that
    the logger is not set up and cannot be until the config file is read, so all errors will be printed.
    :return:
    """

    global config
    global allowed_opts

    # Attempt to open the config file.
    fstream = open(path, 'r')

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
            # Get rid of any whitespace or enclosing quotations.
            value = pair[1].strip()

            # Remove quotes if they fully surround the value; leave them in place if they don't in case they are
            # an intended special character.
            if value.startswith('"') and value.endswith('"'):
                # Use slicing in case there is a "" pair at the start or end of the string.
                value = value[1:-1]
            if value.endswith("'") and value.endswith("'"):
                # Use slicing in case there is a '' pair at the start or end of the string.
                value = value[1:-1]

            # Some option values we expect to be fully lowercase; some can have upper.  Take the user's input at their
            # word.
            if opts not in allowed_opts:
                print('Unknown option {0} in config file.'.format(line))
            else:
                config[opts] = value


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
        # Make sure the log path actually exists; if not, create it.
        log_dir = os.path.split(config['log_path'])[0]
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, 0o755)

        # Now set up the rotating file handler.
        handler = logging.handlers.RotatingFileHandler(config['log_path'], mode='a',
                                                       maxBytes=1048576, backupCount=5)
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


def get_record(file_stream):
    """
    Takes a file_stream object, and reads it line by line until it has fully read one record in.  The record
    is then converted into a JSON object via json.loads and returned to the caller.
    :param file_stream: An input stream object.
    :return: a JSON formatted data object containing the data of a single JSON record.
    """

    record_str = ''
    record = None

    # Read over the file line by line.
    for line in file_stream:
        print("reading line {0}".format(line))
        if not line:
            print("Line is not, apparently.")
            # File stream came to an end before hitting '}', or else we started end of line.
            # return the record all the same.
            return record

        if line.startswith('\n'):
            print("line starts with a newline - empty.")
            # Empty line, it might happen.  Skip over and try the next line.
            continue

        # Looks like a basic line, add it to the string.
        record_str += line

        if line == '}\n':
            print("Line starts with a closing } and newline - end of record.")
            break

    print record_str

    if record_str != '':
        try:
            record = json.loads(record_str)
        except ValueError:
            logger.error("A malformed JSON record has been found.  It is being skipped.  "
                         "Record is: {0}".format(record_str))
            record = {"Status": "invalid"}

    return record


def is_present(hostname, domain):
    """
    Takes a hostname and domain, and checks the host table to see if they exist in it.
    :param hostname: shortname of the host to check.
    :param domain: domain of the host.
    :return: True if the system is present in the database.
             False if the system is NOT present in the database (it will have been added.)
    """

    # Establish our MySQL db connection.
    connection = mysql.connector.connect(option_files=config['opts_file'])
    cursor = connection.cursor()

    query = 'SELECT COUNT(*) FROM host WHERE host.name = %s AND host.domain = %s'
    cursor.execute(query, (hostname, domain))

    exists = cursor.fetchone()[0]

    if exists >= 1:
        return True

    cursor.close()
    connection.close()


def insert_host(hostname, domain, os_name, os_ver, dist_name, dist_ver, checkin_timestamp):
    """
    Takes a host system and inserts its details into the host table in the database.
    :param hostname: Short hostname of the system.
    :param domain: Domain the system resides on.
    :param os_name: Name of the OS (Linux, BSD, etc)
    :param os_ver: Version of the OS (linux kernel version, etc)
    :param dist_name: Name of the OS distribution (applies mostly to Linux distros)
    :param dist_ver: Version of the OS distribution.
    :param checkin_timestamp: Timestamp that the system checked in on.
    :return:
    """

    connection = mysql.connector.connect(option_files=config['opts_file'])
    cursor = connection.cursor()

    query = """INSERT INTO host (name, domain, os_name, os_version, dist_name, dist_ver, last_checkin)
            VALUES(%s, %s, %s, %s, %s, %s, %s)"""

    # Convert checkin_timestamp into a datetime object.  Reference: 29/08/2016 11:56:11
    checkin_datetime = datetime.datetime.strptime(checkin_timestamp, '%d/%m/%Y %H:%M:%S')

    cursor.execute(query, (hostname, domain, os_name, os_ver, dist_name, dist_ver, checkin_datetime))
    connection.commit()

    cursor.close()
    connection.close()


def clear_update_history(hostname, domain):
    """
    Removes from the host_update_history table any rows corresponding to host hostname on domain domain,
    effectively wiping clear their existing system update history.
    :param hostname: Name of the host whose history we wish to wipe.
    :param domain: Domain the host resides on.
    :return:
    """

    connection = mysql.connector.connect(option_files=config['opts_file'])
    cursor = connection.cursor()

    # Need to remove both the host_update_history AND the host_package_versions entries.
    query = """DELETE hpv FROM host AS h LEFT JOIN host_package_versions AS hpv ON h.id = hpv.host_id
               WHERE h.name = %s AND h.domain = %s"""

    cursor.execute(query, (hostname, domain))

    query = """DELETE huh FROM host AS h LEFT JOIN host_update_history AS huh ON h.id = huh.host_id
               WHERE h.name = %s and h.domain = %s"""

    cursor.execute(query, (hostname, domain))
    connection.commit()

    cursor.close()
    connection.close()


def update_checkin_datestamp(hostname, domain, datestamp):
    """
    Attempts to update the checkin timestamp on a specific machine.
    :param hostname: hostname (short) of the system.
    :param domain: domain the system is on.
    :param datestamp: The last checkin time of the system according to the php receiver.
    :return:
    """

    connection = mysql.connector.connect(option_files=config['opts_file'])
    cursor = connection.cursor()

    query = """UPDATE host SET last_checkin = %s WHERE name = %s AND domain = %s"""

    checkin_datetime = datetime.datetime.strptime(datestamp, '%d/%m/%Y %H:%M:%S')

    cursor.execute(query, (checkin_datetime, hostname, domain))
    connection.commit()

    cursor.close()
    connection.close()


def fix_known_hosts(hostname, domain):
    """
    Given a hostname and a domain, remove any existing matching hostname or IP in the running user's known_hosts
    directory, then run ssh-keyscan to acquire the remote host's fingerprint.
    :param hostname: Name of the remote host we need to create/remove entries for.
    :param domain: Domain the remote system is on.
    :return:
    """

    home_ssh_dir = os.path.join(os.getenv('HOME'), '.ssh/known_hosts')

    # Remove any existing references.
    fqdn = hostname + '.' + domain

    # If the host does not exist in the DNS and therefore we cannot get any IP address for it, then we should not
    # try to clear it from the ssh keylist.  It may just be the computer is off network for a short period of time
    # but will return with its SSH keys intact.
    try:
        ip = socket.gethostbyname(fqdn)
    except socket.gaierror:
        return

    cmd = 'ssh-keygen -R {0}{1}'

    # Clear any existence of the hostname and ip, if they exist.
    subprocess.call(shlex.split(cmd.format(fqdn.lower(), '')))
    subprocess.call(shlex.split(cmd.format('[' + fqdn.lower() + ']', '')))
    subprocess.call(shlex.split(cmd.format(ip, '')))
    subprocess.call(shlex.split(cmd.format('[' + ip + ']', '')))
    subprocess.call(shlex.split(cmd.format(fqdn.lower(), ip)))
    subprocess.call(shlex.split(cmd.format('[' + fqdn.lower() + ']', ip)))
    subprocess.call(shlex.split(cmd.format(fqdn.lower(), '[' + ip + ']')))
    subprocess.call(shlex.split(cmd.format('[' + fqdn.lower() + ']', '[' + ip + ']')))

    # Create new reference.
    cmd = 'ssh-keyscan -p 34923 {0}'
    print('Attempting to append known host fingerprint to file {0}'.format(home_ssh_dir))
    fd = open(home_ssh_dir, 'a')

    subprocess.call(shlex.split(cmd.format(fqdn)), stdout=fd)

    fd.close()


def fix_ansible_hosts(hostname, domain):
    """
    Attempts to insert the hostname into the ansible hosts file in the correct category.
    :param hostname:  Name of the system we want added.
    :param domain: Domain the system is a memeber of.
    :return: False if a problem occurred and the system could not be inserted
             True if the system was inserted with no issues.
    """

    global config

    if domain.lower() == 'agr.gc.ca':
        group_prefix = 'AAFC'
    elif domain.lower() == 'cfia-acia.inspection.gc.ca':
        group_prefix = 'CFIA'
    else:
        logger.error('Domain {1} from host {0} is not an AAFC or CFIA domain.'.format(hostname, domain))
        return False

    logger.debug('group_prefix is: {0}'.format(group_prefix))

    try:
        group_suffix = config['ansible_environment']
    except KeyError:
        logger.error('Config option ansible_environment is not set in the options file.')
        return False

    group = '[' + group_prefix + group_suffix + ']'
    logger.debug('Group suffix is: {0}'.format(group_suffix))
    logger.debug('Group is: {0}'.format(group))

    try:
        fd = open('/etc/ansible/hosts', 'r+')
    except IOError as err:
        logger.error('Unable to open ansible hosts file.')
        return False

    host_data = fd.read().split('\n')

    for i in range(0, len(host_data)):
        logger.debug('Checking line {0} against group {1}'.format(host_data[i], group))
        if host_data[i] == group:
            host_data.insert(i + 1, hostname.upper() + '.' + domain.upper())
            break
    else:
        logger.error('Could not find group {0}!'.format(group))

    host_out = '\n'.join(host_data) + '\n'

    fd.seek(0)
    fd.write(host_out)
    fd.close()

    return True


def delete_host_vars(hostname, domain):
    """
    Removes the host variables file from /etc/ansible/host_vars.
    :param hostname: Name of the machine whose variables file we're removing.
    :param domain: Domain in which the machine resides.
    :return:
    """

    hostname = hostname.upper()
    domain = domain.upper()

    try:
        os.remove('/etc/ansible/host_vars/{0}.{1}'.format(hostname, domain))
    except IOError as err:
        # No real errors to log...if the file didn't exist, then we're perfectly fine anyways.  Just note it for
        # possible issues.
        logger.error('Attempt to remove host_vars file {0}.{1} failed - '
                     'it may not have existed.'.format(hostname, domain))
    except OSError as err:
        # Not sure if IOError ever gets raised or not, but leaving it in case it does.
        logger.error('Attempt to remove host_vars file {0}.{1} failed - '
                     'it may not have existed.'.format(hostname, domain))


def create_host_vars(hostname, domain):
    """
    Create a host_vars file for the machine in the hosts file, containing ssh credentials, and vault it.
    :param hostname: Name of the system
    :param domain: Domain the system is in.
    :return:
    """

    hostname = hostname.upper()
    domain = domain.upper()

    # The files are all keyed off of the same vault password for now, so we're able to just copy a template
    # over to access.
    shutil.copy('/etc/ansible/host_vars/TEMPLATE.YML', '/etc/ansible/host_vars/{0}.{1}'.format(hostname, domain))

    # But we do want to be sure that the resulting file has the correct permissions.
    os.chmod('/etc/ansible/host_vars/{0}.{1}'.format(hostname, domain), 0o600)

    # and owner/group.
    owner = pwd.getpwnam('root')[2]
    group = pwd.getpwnam('root')[3]

    os.chown('/etc/ansible/host_vars/{0}.{1}'.format(hostname, domain), owner, group)


def main():
    """
    Standard main entry point, flow control yadda yadda
    :return:
    """

    global config

    # Read in the config file and setup logs...
    read_config()
    setup_logging()

    # Confirm the registered hosts file exists.
    if not os.path.exists(config['reg_host_file']):
        # Just exit - the file may not have been created yet.
        exit(0)

    # Check the size of the registered hosts file to ensure there is data stored in it.
    size = os.stat(config['reg_host_file'])
    if size == 0:
        # If the file is empty, move on.
        exit(0)

    # Open the registered hosts file.
    print("Opening the stupid file. {0}".format(config['reg_host_file']))
    fd = open(config['reg_host_file'], 'r+')
    json_record = get_record(fd)

    while json_record:
        # A short sanity check to deal with cases where a broken record has been reported in.
        if ("Status" in json_record) and (json_record["Status"] == "invalid"):
            # Just get the next record, record the broken record, and continue one.
            json_record = get_record(fd)
            logger.error("The record returned by get_record is invalid.")
            continue

        # Check if the system even exists in the db.  Cast hostname and domain to uppercase - it's possible that
        # the reply from the client appears in lower.
        hostname = json_record['Host'].split('.', 1)[0].upper()
        domain = json_record['Host'].split('.', 1)[1].upper()

        # if the system is not at all present in the db, then this is likely a brand new system.
        if not is_present(hostname, domain):
            insert_host(hostname, domain, json_record['OS_NAME'], json_record['OS_VER'],
                        json_record['DIST_NAME'], json_record['DIST_VER'], json_record['Date'])

            # new system must also be added to known_hosts, ansible's hosts file, and a host_vars file must exist.
            fix_known_hosts(hostname, domain)
            fix_ansible_hosts(hostname, domain)
            create_host_vars(hostname, domain)

            # Once the host is inserted, move on.
            continue

        # Check first boot status:
        if json_record['FIRST_BOOT'] == 'YES':
            # First boot of system; and the hostname clearly exists in the db or we wouldn't have even made it here.
            # Remove all values in host_update_history.
            clear_update_history(hostname, domain)

            # Also need to fix hostname and ansible host_vars.  System already exists in the hosts file, however, or
            # at least it should.
            fix_known_hosts(hostname, domain)

            # Remove and readd the host_vars file.
            delete_host_vars(hostname, domain)
            create_host_vars(hostname, domain)

        # If first boot is not set to yes, then we know the system just needs it's current date/time timestamp
        # updated.  This will hold true for both existing, unchanged machines and machines that have first_boot set
        # but existed before (likely rebuild/reimage.)
        update_checkin_datestamp(hostname, domain, json_record['Date'])

        # Get the next record.
        json_record = get_record(fd)

    fd.truncate(0)
    fd.close()


if __name__ == '__main__':
    main()