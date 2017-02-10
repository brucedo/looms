#!/usr/bin/python

"""
Primary executable for the package manager.  Takes a set of command line arguments to control behavior.  Default
operation is to perform a sync() on all defined package repositories.  Multiple commands can be presented and they
will be executed in the order: add-gpg-key > del-gpg-key > add-package-to-whitelist > del-package-from-whitelist >
add-mirror > del-mirror > sync.

By following this ordering, we guarantee that any packages downloaded will be signed or verified by the newest
available gpg keys, that the packages will be pulled from the most up-to-date whitelist and from the most recently
added mirrors.

The following commands are accepted:

--sync [repo_name, repo_name, ..., repo_name]
Calls the sync operation on any repository specified by name.  If no repository name is present, then the sync
operation will be performed against all repositories.

--add-package-to-whitelist pkg_name [repo_name, repo_name, ..., repo_name]
Adds a package to the whitelist for one or more repos specified by a comma-and-space separated list.  If no one
repository name is provided, then the package name is added to all defined repository whitelists.

--del-package-from-whitelist pkg_name [repo_name, repo_name, ..., repo_name]
Removes a package from the whitelist for one or more repos specified by a comma-and-space separated list.  If no one
repository name is provided, then the package name is added to all defined repository whitelists.

--add-gpg-key ascii_armored_file [repo_name, repo_name, ..., repo_name]
Adds a gpg key to one or more repositories (specified by a comma-and-space separated list).  If no one repository name
is provided, then the package name is added to all defined repository whitelists to which GPG keys can be added.

--del-gpg-key signature [repo_name, repo_name, ..., repo_name]
Removes a gpg key from all repositories specified in the comma-and-space separated list; if no repositories are
specified then the key is removed from all repositories.

--add-mirror uri [repo_name, repo_name, ..., repo_name]
Adds a mirror to the repositories specified by the comma-and-space separated list.  If no repositories are specified
then the mirror is added to all repositories.

--del-mirror uri [repo_name, repo_name, ..., repo_name]
Removes a mirror from the repositories specified by the comma-and-space separated list.  If no repositories are
specified then the mirror is added to all repositories.
"""

import conf.pkg_manager
import mysql.connector
import sys
import importlib
import logging
import logging.handlers
import glob
import os
import os.path


# GLOBALS
add_uri = ''
del_uri = ''
mode = ''
sync_repo_list = []
add_pkg_repo_list = []
del_pkg_repo_list = []
add_gpg_repo_list = []
del_gpg_repo_list = []
add_mirror_repo_list = []
del_mirror_repo_list = []
add_pkg_name = ''
del_pkg_name = ''
add_key_file = ''
del_key_signature = ''
conf_file = '/etc/looms/pkg_manager.conf'

logger = None
log_level = None
plugins = {}


def setup_logs():
    """
    Configures the logging module to handle our log output based on the information read in from the config file.

    :return: None
    """

    global logger
    global log_level

    conf_log_level = conf.pkg_manager.log_level

    # Creates rotating file handler, with a max size of 10 MB and maximum of 5 backups, if path configured.
    if conf.pkg_manager.log_path != '':
        # Ensure that the path to the log file actually exists, and create it if it does not.
        log_dir = os.path.split(conf.pkg_manager.log_path)[0]
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Now point the handler to the log file.
        handler = logging.handlers.RotatingFileHandler(conf.pkg_manager.log_path, mode='a',
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

    logger = logging.getLogger('gnupg.gnupg')
    logger.addHandler(handler)
    logger.setLevel(log_level)

    if logger is None:
        print("A serious error occurred while attempting to generate a logger object.")


def display_help():
    """
    Displays the help menu for the command.
    :return:
    """

    msg_string = "pkg_manager.py is a program designed to manage the automatic updating and provisioning of one \n" \
                 "or more local package repositories.  It is capable of interacting with standard remote \n" \
                 "repositories for rpm, debian and snap packages, and downloading updated or new packages to a \n" \
                 "local repository on request.  It automatically signs packages, index files or any other portion \n" \
                 "of the local repository that the specification for that repository type dictates, and can handle \n" \
                 "multiple remote mirror repositories, switching between them seemlessly should one fail \n" \
                 "connection or security checks.\n" \
                 "\n" \
                 "One primary option (see list below) should be included on the command line; if no primary options\n" \
                 "are provided, then the program will assume that --sync is being called with no repository names\n" \
                 "listed.\n" \
                 "\n" \
                 "Multiple primary options can be provided.  They will not be executed in the order provided on the\n" \
                 "command line; instead, they will be executed in the order:\n" \
                 "1) --add-gpg-key\n" \
                 "2) --del-gpg-key\n" \
                 "3) --add-package-to-whitelist\n" \
                 "4) --del-package-from-whitelist\n" \
                 "5) --add-mirror-uri\n" \
                 "6) --del-mirror-uri\n" \
                 "7) --sync\n" \
                 "\n" \
                 "By following the above ordering, we can guarantee deterministic results; further, we can \n" \
                 "guarantee that the list of packages pulled from will be up to date, as will be the list of \n" \
                 "sources that  the packages can be pulled from, and that all packages will be signed/verified by \n" \
                 "the most up to date keys.\n" \
                 "\n" \
                 "Options:\n" \
                 "\n" \
                 "--conf <path/to/conf.file>\n" \
                 "Specifies an alternative configuration file to use with the package manager.  The default path\n" \
                 "is /etc/pkg_manager/pkg_manager.conf.\n" \
                 "\n" \
                 "--sync [repo_name, repo_name, ..., repo_name] \n" \
                 "Calls the sync operation on any repository specified by name.  If no repository name is present, \n" \
                 "then the sync operation will be performed against all repositories.\n" \
                 "\n" \
                 "--add-package-to-whitelist pkg_name [repo_name, repo_name, ..., repo_name]\n" \
                 "Adds a package to the whitelist for one or more repos specified by a comma-and-space separated \n" \
                 "list.  If no one repository name is provided, then the package name is added to all defined \n" \
                 "repository whitelists.\n" \
                 "--del-package-from-whitelist pkg_name [repo_name, repo_name, ..., repo_name]\n" \
                 "Removes a package from the whitelist for one or more repos specified by a comma-and-space \n" \
                 "separated list.  If no one repository name is provided, then the package name is added to all \n" \
                 "defined repository whitelists.\n" \
                 "\n" \
                 "--add-gpg-key ascii_armored_file [repo_name, repo_name, ..., repo_name]\n" \
                 "Adds a gpg key to one or more repositories (specified by a comma-and-space separated list).  If \n" \
                 "no one repository name is provided, then the package name is added to all defined repository \n" \
                 "whitelists to which GPG keys can be added.\n" \
                 "\n" \
                 "--del-gpg-key signature [repo_name, repo_name, ..., repo_name]\n" \
                 "Removes a gpg key from all repositories specified in the comma-and-space separated list; if no \n" \
                 "repositories are specified then the key is removed from all repositories.\n" \
                 "\n" \
                 "--add-mirror uri [repo_name, repo_name, ..., repo_name]\n" \
                 "Adds a mirror to the repositories specified by the comma-and-space separated list.  If no \n" \
                 "repositories are specified then the mirror is added to all repositories.\n" \
                 "\n" \
                 "--del-mirror uri [repo_name, repo_name, ..., repo_name]\n" \
                 "Removes a mirror from the repositories specified by the comma-and-space separated list.  If no \n" \
                 "repositories arespecified then the mirror is added to all repositories.\n"

    print(msg_string)


def arg_handler():
    """
    Initiates argument parsing.  The argument structure is currently simple enough that it can be handled directly
    through sys.argv - no need to call on argparse.
    :return:
    0 - The command line options were successfully read in and are all valid.
    -1 - One or more of the command line options providied were not valid and could not be read.
    """
    global add_uri
    global del_uri
    global mode
    global sync_repo_list
    global add_pkg_repo_list
    global del_pkg_repo_list
    global add_gpg_repo_list
    global del_gpg_repo_list
    global add_mirror_repo_list
    global del_mirror_repo_list
    global add_pkg_name
    global del_pkg_name
    global add_key_file
    global del_key_signature
    global conf_file

    # Having the allowed list of options here makes it easier to ensure that one and only one correct option is
    # present.
    allowed_options = ['--sync', '--add-package-to-whitelist', '--del-package-from-whitelist',
                       '--add-gpg-key', '--del-gpg-key', '--add-mirror', '--del-mirror', '--conf']

    # Make a copy of the options list.  Exclude the very first "option", as that's either the program name, or -c.
    opts = sys.argv[1:]

    # Formerly we had only single-use options; now we can specify any function AND the conf file.  Split the list
    # of options into dictionary of lists, where each key has as its value a list of the command line settings taht
    # accompany it.
    opts_dict = {}

    # Deal with a special case where no --option is provided, which means that we're doing the default of sync.
    if (len(opts) <= 0) or (not opts[0].startswith('--')):
        mode = 'sync'
        opts_dict[mode] = []

    for option in opts:
        if option.startswith('--'):
            if option not in allowed_options:
                print("Malformed options list - option {0} is not a valid option.".format(option))
                return -1
            mode = option.strip('-')
            opts_dict[mode] = []
        else:
            opt_list = opts_dict[mode]
            opt_list.append(option)
            opts_dict[mode] = opt_list

    # We have a complete dict of all options and their parameters.  Turn those into actual settings for the later
    # parts of the program to rely on.
    for key, value in opts_dict.iteritems():
        if key == 'sync':
            sync_repo_list = value
        if key == 'add-package-to-whitelist':
            add_pkg_repo_list = value[1:]
            add_pkg_name = value[0]
        if key == 'del-package-from-whitelist':
            del_pkg_repo_list = value[1:]
            del_pkg_name = value[0]
        if key == 'add-gpg-key':
            add_gpg_repo_list = value[1:]
            add_key_file = value[0]
        if key == 'del-gpg-key':
            del_gpg_repo_list = value[1:]
            del_key_signature = value[0]
        if key == 'add-mirror':
            add_mirror_repo_list = value[1:]
            add_uri = value[0]
        if key == 'del-mirror':
            del_mirror_repo_list = value[1:]
            del_uri = value[0]
        if key == 'conf':
            conf_file = value[0]

    return 0


def read_config(path='/etc/pkg_manager/pkg_manager.conf'):
    """
    Reads the configuration file /etc/pkg_manager/pkg_manager.conf, and sets the environment up.
    :return:
    0 - the configuration file was loaded successfully.
    -1 - the configuration file could not be opened.
    -2 - The configuration file has errors and could not be successfully loaded.
    """

    return conf.pkg_manager.load_conf(path)


def load_plugins():
    """
    Reads the plugin declaration files stored in the declaration directory and uses those to load the python
    plugin module (just done with import.)  Each plugin will be classified according to the type of package repository
    it's designed to handle.

    :return:
    """

    # need access to the global plugins dictionary, so we can fill it.
    global plugins
    global logger
    global log_level

    # We want to set up logging for each plugin, as well.  Pre-create a logging handler for the logging module.
    # Creates rotating file handler, with a max size of 10 MB and maximum of 5 backups, if path configured.
    if conf.pkg_manager.log_path != '':
        handler = logging.handlers.RotatingFileHandler(conf.pkg_manager.log_path, mode='a',
                                                       maxBytes=10485760, backupCount=5)
    else:
        # if no file or path configured, we just spew to standard err.
        handler = logging.StreamHandler()

    formatter = logging.Formatter(fmt='%(levelname)s %(asctime)s: %(name)s - %(module)s.%(funcName)s - %(message)s')
    handler.setFormatter(formatter)

    # Get a list of files in the declaration directory.
    if conf.pkg_manager.plugin_decl_dir.endswith('/'):
        decl_dir = conf.pkg_manager.plugin_decl_dir + '*'
    else:
        decl_dir = conf.pkg_manager.plugin_decl_dir + '/*'

    print("declaration directory: " + decl_dir)
    decl_files = glob.glob(decl_dir)

    # We could attempt to only load those plugins for which we have a repo, but we don't gain much.

    # declaration data
    repo_type = ''
    plugin_name = ''
    plugin_path = ''
    logger.debug('Attempting to read in all plugin declaration files.')
    for path in decl_files:
        logger.debug('Reading plugin declaration file {0}'.format(path))
        fd = open(path)
        # Read the contents in.
        contents = fd.read()
        fd.close()
        for line in contents.split('\n'):
            # handle the case of empty or whitespace only strings:
            if not line or line.isspace():
                # Go on to the next string.
                continue
            # line should be in form of option = value
            temp = line.split('=')
            option = temp[0].strip().lower()
            value = temp[1].strip()
            if option == 'type':
                logger.debug('Plugin type: {0}'.format(value))
                repo_type = value
            if option == 'plugin_name':
                logger.debug('Plugin name: {0}'.format(value))
                plugin_name = value
            if option == 'plugin_path':
                logger.debug('Plugin path: {0}'.format(value))
                plugin_path = value

        if repo_type == '' or plugin_name == '':
            logger.error('Type ({0}) or name ({0}) left empty - plugin '
                         'cannot be loaded.'.format(repo_type, plugin_name))
            # malformed declaration file.  Move on to next.
            continue

        # if the plugin is not on the standard path
        if (plugin_path != '') and os.path.exists(plugin_path) and (plugin_path not in sys.path):
            logger.debug('Plugin path not empty; plugin in non-standard path {0}'.format(plugin_path))
            sys.path.append(plugin_path)
        elif plugin_path == '' and (conf.pkg_manager.plugins_dir != '') \
                and os.path.exists(conf.pkg_manager.plugins_dir):
            logger.debug('Plugin decl file does not provide plugin path; instead applying '
                         'globally defined plugin path {0}'.format(conf.pkg_manager.plugins_dir))
            sys.path.append(conf.pkg_manager.plugins_dir)
        else:
            logger.debug('Neither plugin declaration file nor master conf file provide a valid plugin path'
                         'string.  Using the python default path list.')
            if conf.pkg_manager.plugins_dir not in sys.path:
                sys.path.append(conf.pkg_manager.plugins_dir)

        # Now attempt to load the plugin.
        logger.debug('Attempting to load plugin module now...')
        plugin = importlib.import_module(plugin_name)
        if plugin is not None:
            logger.debug('Plugin module {0} for package type {1} loaded successfully.'.format(plugin_name, repo_type))
            plugins[repo_type] = plugin

            # Set up this plugins login.
            logger = logging.getLogger(plugin.__name__)
            logger.addHandler(handler)
            logger.setLevel(log_level)

            # clear the plugin var
            plugin = None
        else:
            logger.error('Plugin {0} could not be loaded.'.format(plugin_name))


def update_database(api_pkg_data, repository_name):
    """
    Iterates over the contents of the api_pkg_data object and stores them in the MySQL database.  This is a fast,
    down and dirty implementation - a proper one will come later.
    :param api_pkg_data: A list of packages that have been updated by one of the repository plugins.
    :param repository_name: The name of the repository from which the package update has been identified.
    :return:
    """

    global logger
    # Establish our MySQL db connection.
    connection = mysql.connector.connect(option_files=conf.pkg_manager.opts_file)
    cursor = connection.cursor()
    update_type = ''

    print("Updating database for changes made to {0}".format(repository_name))

    # Iterate over each item in the updated list, checking first if the package name is present and if not
    # adding it, then applying the update data to the package_history table.
    for pkg in api_pkg_data.get_list():
        # First confirm that the package is in the database.
        query = """SELECT COUNT(*) FROM package WHERE package_name = %s AND package_type = %s and contents = %s"""
        cursor.execute(query, (pkg[0], pkg[1], pkg[2]))

        # count returns one row, one column named COUNT(*)
        update_type = pkg[5]
        exists = cursor.fetchone()[0]
        if int(exists) < 1:
            print('Package {0} does not exist in the database as yet.  Adding now.'.format(pkg[0]))
            query = """INSERT INTO package (package_name, package_type, contents)
                    VALUES (%s, %s, %s);"""
            cursor.execute(query, (pkg[0], pkg[1], pkg[2]))
            update_type = 'add'

        # Package is now guaranteed to be in packages table.  Next, we want to see if the package version
        # has already been added INTO the history table, perhaps as part of a scan from another machine with
        # a newer version of the image that was marked as provisional.
        print('Package {0} may already have had version {1} added to the db.  Testing...'.format(pkg[0], pkg[3]))
        query = """SELECT COUNT(*) FROM package AS p LEFT JOIN package_history AS ph
                   ON p.id = ph.package_id
                   WHERE p.package_name = %s AND p.package_type = %s AND p.contents = %s AND ph.version = %s"""

        cursor.execute(query, (pkg[0], pkg[1], pkg[2], pkg[3]))
        exists = cursor.fetchone()[0]

        if int(exists) < 1:
            print('Package {0}, version {1} does NOT exist in the update history table.'.format(pkg[0], pkg[3]))
            # If the result is that 0 rows match, we want to go ahead with the insert event.
            query = """INSERT INTO package_history(package_id, event_date, version, event_type, from_repository)
                    SELECT id, %s, %s, %s, %s
                    FROM package WHERE package_name = %s AND package_type = %s AND contents = %s"""
            print('Query: {0}'.format(query % (pkg[4], pkg[3], update_type, repository_name, pkg[0], pkg[1], pkg[2])))
            cursor.execute(query, (pkg[4], pkg[3], update_type, repository_name, pkg[0], pkg[1], pkg[2]))
        else:
            print('Package {0}, version {1} DOES exist in the update history table.'.format(pkg[0], pkg[3]))
            # If the result is that 1 or more rows match, however, we want to update those rows to the correct
            # event type and event_date.
            query = """UPDATE package_history AS ph LEFT JOIN package AS p ON ph.package_id = p.id
                       SET ph.event_date = %s, ph.event_type = %s, ph.from_repository = %s
                       WHERE p.package_name = %s AND p.package_type = %s AND p.contents = %s AND ph.version = %s"""
            print('Query: {0}'.format(query % (pkg[4], update_type, repository_name, pkg[0], pkg[1], pkg[2], pkg[3])))
            cursor.execute(query, (pkg[4], update_type, repository_name, pkg[0], pkg[1], pkg[2], pkg[3]))

        # MySQL documentation suggests that insert ... select on duplicate update type statements are considered
        # unsafe, as it's not possible to guarantee the order of items pulled from a select and therefore insertions
        # cannot guarantee a deterministic order for unique key constraint violation.  However, I think this will
        # be safe, because there should only ever be ONE result returned from the select subquery.
        logger.debug('Attempting insert/update on duplicate key query for '
                     'package {0}, version {1}'.format(pkg[0], pkg[3]))
        query = """INSERT INTO current_package_versions (package_id, package_history_id)
                    SELECT p.id AS package_id, ph.id AS package_history_id
                    FROM package AS p LEFT JOIN package_history AS ph ON p.id = ph.package_id
                    WHERE p.package_name = %s AND p.package_type = %s AND p.contents = %s AND ph.version = %s
                    ON DUPLICATE KEY UPDATE package_history_id = ph.id"""
        cursor.execute(query, (pkg[0], pkg[1], pkg[2], pkg[3]))

        # commit the package updates and close the connection.
        connection.commit()

    # When done, close our resources.
    cursor.close()
    connection.close()


def main():
    """
    Entry point for the program.  Initiates argument handling, loads the config files for those repositories that
    have been invoked (as well as the plugin handlers) and then initiates execution of the appropriate plugin
    functionality.
    :return:
    """

    global conf_file

    # First step - read the command options.
    opt_results = arg_handler()
    if opt_results != 0:
        print("An error occurred - one or more command line options is invalid.  Please check "
              "your options and try again.\n\n")
        display_help()
        exit(-1)

    # Next, read our own configuration file.
    conf_result = read_config(conf_file)
    if conf_result != 0:
        print("Error reading configuration file {0} - error code {1}".format(conf_file, conf_result))
        exit(conf_result)

    # set our libpath onto the end of the sys libpath and import the looms api modules.
    sys.path.append(conf.pkg_manager.libpath)
    import data_api.updated_pkg_data

    # Configuration file is loaded - get logging configured next.
    setup_logs()

    # Now to load plugins.
    load_plugins()

    # Create a set of repository management objects for each repository we have configured.
    repositories = {}
    for repo_name, repo_value in conf.pkg_manager.repositories.iteritems():
        name = repo_name
        type = repo_value['type']
        repositories[name] = plugins[type].initialize(name, repo_value)

    # Test work in setting up repo.
    for name, repo_mgmt_obj in repositories.iteritems():
        # Try a sync.
        print('Syncing repo {0}.'.format(name))
        updated_data = repo_mgmt_obj.sync()
        if updated_data is None:
            logger.warn('None returned by repo sync for repo {0}.  There may have been no updates, or there may be a '
                        'problem with the repository configuration.'.format(name))
        else:
            # and update the db with results.
            print('Repo {0} returned with {1} updated packages in the output '
                  'list.'.format(name, len(updated_data.get_list())))
            update_database(updated_data, name)


if __name__ == "__main__":
    main()
