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


def parse_conf


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
        conf_fstream = open(conf_file)
        parse
    except IOError:
        # No config file.  This is not the end of the world - it may just mean that this is a first-time install.
        pass


if __name__ == "__main__":
    main()
