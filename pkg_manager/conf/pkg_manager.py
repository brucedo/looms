#!/usr/bin/python

"""
Module used to store the settings of the package manager's front end.  Stores the following values:

# Topmost directory under which all repositories are stored.
root = path
# Sets globally whether the whitelist can be overridden by missing dependencies.
override_whitelist = yes|no
# Path to the public and private key repository.  The path cannot be fully overridden but a repository can request a
# subdirectory of their own.
key_path = <path>
# public keyring name (global, default).  Individual repositories can override this default.
public_keyring = <name>
# private keyring name (global, default).  Individual repositories can override this default.
private_keyring = <name>
# Default user id to assign to every created repo directory.
default_owner = uid
# Default group id to assign to every created repo directory.
default_group = gid
# Default permissions to assign to all repository top-level directories.
default_perms = <octal_permission_list>
# When yes, the group sticky bit will be set for all created repo directories.
default_sticky = yes|no
# Log location:
log_path = <path>
# Log level:
log_level = debug|critical|error|warn|info

# Repository section, one for each repository being hosted.
[Repository_name]
# Need to associate a type for the repository
type = deb|rpm|flatpack|snap|docker
# Need to associate a subdirectory to the repository.
root = <directory_name_under_root>
# Give option to grant access to specific user/group for the repo's directories.
root_owner = uid
root_group = gid
# Give option for specific directory permissions for top-level.
root_perm = 0o###
# Give option for inheritance sticky bit set.
root_sticky = yes|no
# Unique subdirectory in which the repo's keyfiles can be stored.
key_path = <subdir_path>
# public keyring name for the repository, if required.
public_keyring = <name>
# private keyring name for the repository, if required.
private_keyring = <name>
"""

import os
import os.path
import pwd
import grp

root = '/srv/pkg_manager'
repositories = {}
keypath = '/etc/pkg_manager/keys'
public_keyring = 'pubring.gpg'
private_keyring = 'secring.gpg'
key_name = ''
key_password_file = ''
override_whitelist = False
default_owner = 'root'
default_group = 'root'
default_perms = 0o755
default_sticky = False
log_path = '/var/log/pkg_manager/pkg_manager.log'
log_level = 'debug'

# Options for supporting plugins
plugin_decl_dir = '/etc/pkg_manager/plugin_defs'
plugins_dir = '/usr/local/lib/python2.7/site-packages'


def load_conf(path):
    """
    Loads a configuration file into the module namespace.  sub-information regarding repositories is stored
    in the repository dict.  A reference to the repository object that is created when the repository plugin is loaded
    is stored in the dict as well.
    :param path: The full path to the config file we are loading.
    :return:
    0 - the file loaded correctly.
    -1 - the file failed to load due to an exception when opening the file.
    -2 - the file failed to load due to an error in the config file syntax.
    """

    global repositories
    global root
    global keypath
    global public_keyring
    global private_keyring
    global key_name
    global key_password_file
    global override_whitelist
    global default_owner
    global default_group
    global default_perms
    global default_sticky
    global log_path
    global log_level
    global plugin_decl_dir
    global plugins_dir

    retval = 0

    # Open the file.
    try:
        fd = open(path, 'r')
    except IOError as e:
        err_str = "An error has occurred opening file {0}".format(path)
        if os.path.exists(path):
            err_str += "  The file does exist.  Permissions may not be set correctly.\n"
        else:
            err_str += "  The file does not appear to exist.  Please double check the path provided.\n"
        err_str += "The error message is:\n{0}".format(e.message)
        print(err_str)
        return -1

    # File open.  Start reading it line by line.  Asume the opening seciton is the Global one.
    section = 'Global'
    line_no = 1
    for line in fd:
        # New section definition.  Check to see if they're returning to global or not; also, confirm syntax.
        # We don't need keyword and value if this is a section line, but we also don't care if we do the work because
        # it throws no errors if there is no = sign.
        keyword = line.lower().split('=')[0].strip()
        value = line.split('=')[-1].strip()
        if line.startswith('['):
            if line.count('[') > 1 or line.count(']') > 1:
                # Malformed section declaration.
                retval = -2
                print("Malformed section declaration on line {0}".format(line_no))
                break

            # Section seems fine.  get the new section name.
            name = line.strip('[]\n ')
            # If the section is 'global' then set the current section to 'Global' so we know what settings we're in.
            if name.lower() == 'global':
                section = 'Global'
            else:
                # If it's not global, then we're in a repository section.  The repository section is used as the index
                # into the repositories dictionary for easy callups later.
                if name not in repositories:
                    repositories[name] = {}
                section = name
        # Skip comments and blank lines.
        elif line.strip().startswith('#') or line.strip() == '':
            pass
        # This section deals with the actual settings keywords, starting with 'root' (shared between repos and global)
        elif keyword == 'root':
            if section == 'Global':
                root = value
            else:
                repositories[section]['root'] = value
        # Keypath keyword (also shared between repos and global)
        elif keyword == 'key_path':
            if section == 'Global':
                keypath = value
            else:
                repositories[section][keyword] = value
        # public_keyring keyword (shared between repos and global)
        elif keyword == 'public_keyring':
            if section == 'Global':
                if value.lower() == 'default':
                    public_keyring == ''
                else:
                    public_keyring = value
            else:
                if value.lower() == 'default':
                    public_keyring == ''
                else:
                    repositories[section][keyword] = value
        # and private_keyring (shared between repos and global settings.)
        elif keyword == 'private_keyring':
            if section == 'Global':
                if value.lower() == 'default':
                    private_keyring = ''
                else:
                    private_keyring == value
            else:
                if value.lower() == 'default':
                    repositories[section][keyword] = ''
                else:
                    repositories[section][keyword] = value
        # This handle the default_owner setting, which is Global specific.
        elif keyword == 'key_name':
            if section == 'Global':
                if value.lower() == 'default':
                    key_name = ''
                else:
                    key_name = value
            else:
                if value.lower() == 'default':
                    repositories[section][keyword] = ''
                else:
                    repositories[section][keyword] = value
        elif keyword == 'key_password_file':
            if section == 'Global':
                key_password_file = value
            else:
                repositories[section][keyword] = value
        elif keyword == 'default_owner':
            # Make sure we're in a global section.
            if section != 'Global':
                retval = -2
                print("default_owner on line {0} should be in Global section!".format(line_no))
                break
            # and confirm that the user account actually exists.
            try:
                pwd.getpwnam(value)
            except KeyError:
                retval = -2
                print("default_owner on line {0} specifies non-existant username {1}".format(line_no, value))
                break
            default_owner = value
        # Handle the default_group keyword.
        elif keyword == 'default_group':
            # Make sure we're in the global section.
            if section != 'Global':
                retval = -2
                print("default_group on line {0} should be in Global section!".format(line_no))
                break
            # and that the group specified exists:
            try:
                grp.getgrnam(value)
            except KeyError:
                retval = -2
                print("default_group on line {0} specifies non-existant group {1}".format(line_no, value))
                break
            default_group = value
        elif keyword == 'default_perms':
            # default_perms is only applicable to the global settings section.
            if section != 'Global':
                retval = -2
                print("default_perms on line {0} should be in Global section!".format(line_no))
                break
            # Confirm that the permission provided does, in fact, specify an octal value.
            try:
                default_perms = int(value, 8)
            except ValueError:
                print("Non-octal value {0} found in default_perms statement on line {1}".format(value, line_no))
        elif keyword == 'default_sticky':
            # default_sticky is only applicable to the global settings section.
            if section != 'Global':
                retval = -2
                print("default_sticky on line {0} should be in Global section!".format(line_no))
                break
            # Sticky bit is a binary on/off, so we accept yes or no as input.
            if value.lower() == 'yes':
                default_sticky = True
            elif value.lower() == 'no':
                default_sticky = False
            else:
                retval = -2
                print("default_sticky on line {0} should be yes or no, not {1}.".format(line_no, value))
                break
        elif keyword == 'log_path':
            if section == 'Global':
                log_path = value
            else:
                retval = -2
                print("log_path on line {0} should be in Global section.".format(line_no))
                break
        elif keyword == 'log_level':
            if section != 'Global':
                retval = -2
                print("log_level on line {0} should be in Global section.".format(line_no))
                break
            if value.lower() not in ['debug', 'critical', 'error', 'warn', 'info']:
                retval = -2
                print("log_level value {0} on line {1} should be one of "
                      "debug, critical, error, warn or info.".format(value, line_no))
                break
            log_level = value.lower()
        elif keyword == 'type':
            if section == 'Global':
                retval = -2
                print("type on line {0} must be in a repository section, not Global.".format(line_no))
                break
            if value.lower() not in ['deb', 'rpm', 'flatpack', 'snap', 'docker']:
                retval = -2
                print("type on line {0} is not a recognizable type (deb, rpm, flatpack, snap, docker".format(line_no))
                break
            repositories[section][keyword] = value
        elif keyword == 'root_owner':
            if section == 'Global':
                retval = -2
                print("root_owner on line {0} must be in a repository section, not Global.".format(line_no))
                break
            try:
                pwd.getpwnam(value)
            except KeyError:
                retval = -2
                print("root_owner on line {0} specifies non-existant username {1}".format(line_no, value))
                break
            repositories[section][keyword] = value
        elif keyword == 'root_group':
            if section == 'Global':
                retval = -2
                print("root_group on line {0} must be in a repository section, not Global.".format(line_no))
                break
            try:
                grp.getgrnam(value)
            except KeyError:
                retval = -2
                print("default_group on line {0} specifies non-existant group {1}".format(line_no, value))
                break
            repositories[section][keyword] = value
        elif keyword == 'root_perm':
            if section == 'Global':
                retval = -2
                print("root_group on line {0} must be in a repository section, not Global.".format(line_no))
                break
            try:
                repositories[section][keyword] = int(value, 8)
            except ValueError:
                retval = -2
                print("Non-octal value {0} found in root_perm, line {1}".format(value, line_no))
        elif keyword == 'root_sticky':
            if section == 'Global':
                retval = -2
                print("root_sticky on line {0} must be in a repository section, not Global.".format(line_no))
                break
            if value.lower() == 'yes':
                repositories[section][keyword] = True
            elif value.lower() == 'no':
                repositories[section][keyword] = False
            else:
                retval = -2
                print("Invalid value {0} in root_sticky (line {1}).".format(value, line_no))
                break
        elif keyword == 'plugin_decl_dir':
            if section != 'Global':
                retval = -2
                print("plugin_decl_dir on line {0} must be in the Global section, "
                      "not repo {1}".format(line_no, section))
                break
            if os.path.exists(value):
                plugin_decl_dir = value
            else:
                print("Plugin declaration directory {0} specified on line {1} of the configuration file does not"
                      "exist.  Using {2} as default.".format(value, line_no, plugin_decl_dir))
        elif keyword == 'plugin_dir':
            if section != 'Global':
                retval = -2
                print("plugin_dir on line {0} must be in the Global section, not repo.".format(line_no, section))
                break
            if os.path.exists(value):
                plugins_dir = value
            else:
                print("Plugin directory {0} specified on line {1} of the configuration file does not exists."
                      "Using {2} as default.".format(value, line_no, plugins_dir))
        else:
            if section == 'Global':
                retval = -2
                print("Unknown option {0} found in configuration file Global section.".format(keyword))
            else:
                # Other option for plugin.  Accept it and add it to the repositories dictionary.
                repositories[section][keyword] = value

        line_no += 1
    return retval
