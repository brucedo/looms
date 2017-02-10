#!/usr/bin/python

"""
Installation program - checks to see if the looms tools currently exist on the system, and removes/replaces them
if they do.  Creates/updates the database, and if necessary config files do not exist, creates them with generic
settings.

Default directory setup:

"Executables"
/usr/local/bin/pkg_manager.py
/usr/local/bin/scan_host_pkgs.py
/usr/local/bin/scheduler.py
/usr/local/bin/host_updater.py
/usr/local/bin/sync_host_db.py

Libraries
/usr/local/lib/python2.7/site-packages/looms/__init__.py
/usr/local/lib/python2.7/site-packages/looms/conf/__init__.py
/usr/local/lib/python2.7/site-packages/looms/conf/pkg_manager.py
/usr/local/lib/python2.7/site-packages/looms/common/__init__.py
/usr/local/lib/python2.7/site-packages/looms/common/common.py
/usr/local/lib/python2.7/site-packages/looms/data_api/__init__.py
/usr/local/lib/python2.7/site-packages/looms/data_api/updated_pkg_data.py

Configuration
/etc/looms/looms.conf

On execution, installer checks the default looms.conf file for specific settings - namely, the bin_root and lib_root
directives.  If they are set, then the bin_ and lib_root variables are changed; if not then the defaults hold.

The following command line parameters can be set:
--bin-root=
Tells the installer what the bin_root value should be; overrides the default and any value set in the config file.

--lib-root=
Tells the installer what the lib_root value should be; overrides the default and any value set in the config file.

--conf-file=
Tells the installer what directory the configuration file is stored in.  Overrides the default of /etc/looms.


Further notes from a test setup:
Looms preparation work:
Need looms directory created under /usr/local/python/site-packages
Need database created, made ready for use
Create /etc/looms
create /etc/looms/db_info

Getting sync_host_db ready:
Need database account created for sync_host_db
need sync_host_db directory created under /usr/local/python/site-packages/sync_host_db
Create /etc/looms/db_info/sync_host.cnf (MySQL opts file)
Set permissions to cnf file to 0600
Create /etc/looms/sync_host_db.conf
    fill with:
        # Level at which we want to record debug data
        log_level
        # Path to the logfile.
        log_path
        # Path to the file in which registered hosts call_home pings are recorded.
        reg_host_file
        # Name given to the ansible environment (eg. test, prod, prodsystems etc.)
        # in which a new computer's name will be recorded as part of the dynamic inventory.
        ansible_environment
        # full path to the MySQL options file for the sync_host_db database user.
        opts_file

Ensure that a DB user account exists for sync_host_db
Grant that user account access to:
    SELECT on host, host_package_versions, host_update_history
    INSERT on host
    DELETE on host, host_package_versions, host_update_history
    UPDATE on host

Create /etc/ansible/hosts
Populate /etc/ansible/hosts with titles for environment groups.


Getting scan_host_pkgs ready:
Need database account created
Need db account granted access to:
    SELECT on host, host_update_history, package_history, package
    INSERT on host, host_update_history, package_history, host_package_versions
Create opts file /etc/looms/scan_host_pkgs.cnf
Set permissions to cnf file to 0600
Create /etc/looms/scan_host_pkgs.conf file
    fill with:
        # Level at which we want to record debug data
        log_level
        # Path to the logfile.
        log_path
        # scan_host_pkgs needs to run ansible jobs, and so needs to be able to unvault host_var files.
        ansible_vault_file
        # full path to the MySQL options file for the scan_host_pkgs user.
        opts_file


Getting pkg_manager ready:
Create /etc/looms/plugin_defs directory
Create /etc/looms/keys
Create /usr/lib/python2.7/site-packages/looms/pkg_manager directory
Create /usr/lib/python2.7/site-packages/looms/pkg_manager/plugins directory
Create /usr/lib/python2.7/site-packages/looms/lib
Create /usr/lib/python2.7/site-packages/looms/data_api

Copy looms/data_api/{__init__.py,updated_pkg_data.py} /usr/lib/python2.7/site-packages/looms/data_api
Copy looms/pkg_manager/{__init__.py,pkg_manager.py} /usr/lib/python2.7/site-packages/looms/pkg_manager
Copy looms/pkg_manager/conf/{__init__.py,pkg_manager.py} /usr/lib/python2.7/site-packages/looms/pkg_manager/conf
Copy looms/pkg_manager/repo_plugins/{__init__.py,debian_pkg_manager.py}
    /usr/lib/python2.7/site-packages/looms/pkg_manager/plugins

Create declarations file for debian_pkg_manager.py called debian_pkg_manager.decl, with contents:
type = deb
plugin_name = debian_pkg_manager
plugin_path = /usr/lib/python2.7/site-packages/looms/pkg_manager/plugins

Create a config file /etc/looms/pkg_manager.conf
Load with all the settings needed to cover all repositories.
Create a data repository
Ensure that the data repository is set in pkg_manager.conf root path.
Each repository must also be configured to point to a valid whitelist file; whitelist file is relative to the specified
root for the repository, which itself is relative to the root specified in the global settings.

Create a whitelist file for each repository (or shared between all).  Whitelist file format depends on the type of the
package repository.  Currently, debs are:
[sub-repo type]
package_name	pkg_type

where sub-repo type is one of the Debian components, and pkg_type is "binary/src/translation" etc.  Virtually all of
our packages are binary right now.

whitelisted packages should be based on the packages provided on the image(s) being offered, and must be kept up to
date or else important updates may be missed.

Create a new key with which to sign packages.  Import a public key with which to verify existing packages.  Long term
plan will be to pull packages from the test server.  For now, just pull from Ubuntu's repos, which means that we
need to include the Ubuntu public keys.

Keyrings and key passfiles should be stored in a secure location, such as /etc/looms/secure.

Create db account pkg_manager
Grant SELECT, INSERT on panopticon.package
Grant SELECT, INSERT, UPDATE on panopticon.package_history
Grant INSERT on panopticon.current_package_versions

Create /etc/looms/db_inf/pkg_manager.cnf db options file for login.
Create


Setting up host_updater
Need config file under /etc/looms/host_updater
Need log file under /var/log/looms

Need db account host_updater
Grant SELECT, INSERT on host_package_versions
Grant SELECT, UPDATE on host
Grant SELECT, INSERT on package_history
Grant SELECT on package
Grant SELECT on current_package_versions
Grant INSERT on host_update_history




Setting up scheduler
There is no config file for the scheduler (yet.)
Create directory /usr/lib/python2.7/looms/scheduler
Create directory /usr/lib/python2.7/looms/lib
Create directory /usr/lib/python2.7/looms/lib/jobs
Create directory /usr/lib/python2.7/looms/lib/jobs/HostPackageScan
Create directory /usr/lib/python2.7/looms/lib/jobs/PackageUpdater
Create directory /usr/lib/python2.7/looms/lib/jobs/UpdateHostDBEntry
Create directory /usr/lib/python2.7/looms/lib/jobs/UpdateHosts

Copy scheduler.py to /usr/lib/python2.7/looms/scheduler
Copy __init__.py to /usr/lib/python2.7/looms/lib
Copy __init__.py to /usr/lib/python2.7/looms/lib/jobs
Copy {__init__.py,HostPackageScan.py} to /usr/lib/python2.7/looms/lib/jobs/HostPackageScan
Copy {__init__.py,PackageUpdater.py} to /usr/lib/python2.7/looms/lib/jobs/PackageUpdater
Copy {__init__.py,UpdateHostDBEntry.py} to /usr/lib/python2.7/looms/lib/jobs/UpdateHostDBEntry
Copy {__init__.py,UpdateHosts.py} to /usr/lib/python2.7/looms/lib/jobs/UpdateHosts

UpdateHosts job and HostPackageScan both need to perform database queries.
HostPackageScan needs:
SELECT on host, host_update_history
UpdateHosts job needs:
SELECT on host_package_versions, current_package_versions

Because we have no config in place for these services, we're forced to hardcode some values in them - specifically, the
location of the actual script that gets run, and the opts files.  This needs to be fixed, but can be done later.  For
now, just correct and then copy.

"""

import sys

bin_root = "/usr/local/bin"
lib_root = "/usr/local/lib/python2.7/site-packages/looms"
conf_file = "/etc/looms/looms.conf"
status = 0


def parse_cmdline():
    """
    Reads the command line variables and alters the expected location of the bin_root, lib_root and conf_dir as
    called for.
    :return:
    """

    global bin_root
    global lib_root
    global conf_file
    global status

    vars = sys.argv

    # Account for the script name or -c appearing in the arg list by starting at 1.
    skip_next = False
    for i in range(1, len(vars)):
        if vars[i].startswith('--bin-root='):
            arg = vars[i].split('=')
            if len(arg) > 1:
                bin_root = arg[1]
            else:
                # Handle the badly formed --bin-root.
                print('--bin-root argument badly formed: {0}'.format(vars[i]))
                print_exec_error()
                status = -1
                break
            continue
        elif vars[i].startswith('--lib-root='):
            arg = vars[i].split('=')
            if len(arg) > 1:
                lib_root = arg[1]
            else:
                # Handle the baldy formed --lib-root.
                print('--lib-root argument badly formed: {0}'.format(vars[i]))
                print_exec_error()
                status = -1
                break
            continue
        elif vars[i].startswith('--conf-file'):
            arg = vars[i].split('=')
            if len(arg) > 1:
                conf_file = arg[1]
            else:
                # Handle the badly formed --conf-file.
                print('--conf-file argument badly formed: {0}'.format(vars[i]))
                print_exec_error()
                status = -1
                break
            continue
        # The arguments passed are not valid, so...
        else:
            print('Invalid command line argument {0} passed.'.format(vars[i]))
            print_exec_error()
            status = -1
            break


def print_exec_error():
    """
    Prints a message indicating that the program was invoked incorrectly and the help info.
    :return:
    """
    print('Valid arguments are:')
    print('')
    print('--bin-root=(path)')
    print('Tells the installer what the bin_root value should be; overrides the default and any value '
          'set in the config file.')
    print('')
    print('--lib-root=(path)')
    print('Tells the installer what the lib_root value should be; overrides the default and any value '
          'set in the config file.')
    print('')
    print('--conf-file=(path)')
    print('Tells the installer what directory the configuration file is stored in.  Overrides the '
          'default of /etc/looms.')


def parse_conf(fname):
    """
    Takes the name of a configuration file and parses it, looking for information that will tell it where existing
    installed files may be found for replacement.  Focus is on finding the
    :param fname:
    :return:
    """




def main():
    """
    Entry point for the installer.
    :return: 0 if the installation completed successfully; non-zero on failure.
    """

    global bin_root
    global lib_root
    global conf_file
    global status

    # Check our command line options.
    parse_cmdline()

    # Make sure the command line options were read in successfully, quit out if not.
    if status != 0:
        exit(status)

    # Check to see if a config file exists:
    try:

        parse
    except IOError:
        # No config file.  This is not the end of the world - it may just mean that this is a first-time install.
        pass


if __name__ == "__main__":
    main()
