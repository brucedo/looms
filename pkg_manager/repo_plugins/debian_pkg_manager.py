"""
Plugin file for the debian package manager that we will be using.  Holds a class definition for the repository manager,
and a module-namespace function called initialize() that takes a single dictionary of options and uses them to create
an instance of the repository manager class.

The repository manager class does not (yet) inherit from a proper base class; instead we use a simpler mechanism of
"just agree to keep a set of method names in the class."
"""

import conf.pkg_manager
import os.path
import glob
import pwd
import grp
import logging
import httplib
import datetime
import gnupg
import hashlib
import StringIO
import gzip
import bz2
import api.updated_pkg_data


logger = None


def initialize(name, opts_dict):
    """
    Uses the opts_dict to create an instance of the debian package management class.  The initialize call exists
    to provide a guaranteed way of creating an instance of any package management class for any plugin without
    requiring that all plugins name their package management classes exactly the same.\
    :param name: The name of the repository as defined in the configuration file.
    :param opts_dict: List of options for the given repository that the package manager is going to work on.
    :return: An instance of the debian package manager class.
    """

    global logger

    logger = logging.getLogger(__name__)
    logger.addHandler(NullHandler())

    return DebianPkgManager(name, opts_dict)


class DebianPkgManager:
    """
    The class responsible for managing the functionality associated with a debian-type package repository.
    """

    def __init__(self, name, opts_dict):
        """
        Class constructor.  Accepts a dictionary of options and uses them to initialize its settings.  The options
        dictionary will be checked for mandatory options and throw an exception ValueError if they are missing.
        :param opts_dict:
        :return:
        """

        # Directory setup.
        root = conf.pkg_manager.root

        # Non-config file specified variables.
        self.whitelist = {}
        self.release_contents = {}

        # Config file specified variables.
        try:
            self.root = os.path.join(root, opts_dict['root'])
        except:
            raise ValueError('Option root not specified in options file for repo {0}.  '
                             'Unable to proceed with initialization.'.format(name))

        try:
            cache = opts_dict['cache_directory']
        except KeyError:
            raise ValueError('Option cache_directory not specified in options file for repo {0}.  '
                             'Unable to proceed with initialization.'.format(name))
        # Set the proper value for cache depending on whether it starts with / or not...
        self.cache_dir = os.path.join(self.root, cache)

        try:
            pool = opts_dict['pool_directory']
        except KeyError:
            raise ValueError('Option pool_directory not specified in options file for repo {0}.  '
                             'Unable to proceed with initialization.'.format(name))
        # Set the proper value for the pool directory depending on whether it starts with / or not...
        self.pool_dir = os.path.join(self.root, pool)

        try:
            web_root = opts_dict['web_root']
        except KeyError:
            raise ValueError('Option web_root not specified in options file for repo {0}.  '
                             'Unable to proceed with initialization.'.format(name))
        self.web_root = os.path.join(self.root, web_root)

        try:
            repo = opts_dict['repository_path']
        except KeyError:
            raise ValueError('Option repository_path not specified in options file for repo {0}.  '
                             'Unable to proceed with initialization.'.format(name))
        # set the proper value for the repository directory depending on whether it starts with / or not.
        self.repo_dir = os.path.join(self.root, repo)

        # Handle the supported architectures next.
        try:
            arch_str = opts_dict['supported_archs'].replace(' ', '')
        except KeyError:
            raise ValueError('Option supported_archs not specified in options file for repo {0}.  '
                             'Unable to proceed with initialization.'.format(name))
        self.arch_list = arch_str.split(',')

        # Repository components:
        try:
            comp_str = opts_dict['supported_components'].replace(' ', '')
        except KeyError:
            raise ValueError('Option supported_components not specified in options file for repo {0}.  '
                             'Unable to proceed with initialization.'.format(name))
        self.component_list = comp_str.split(',')

        # Now to handle the categories per component settings...
        self.component_categories = {}
        for component in self.component_list:
            try:
                category_str = opts_dict[component + '_categories'].replace(' ', '')
            except KeyError:
                raise ValueError('Component {0} specified in supported_components option, but no '
                                 '{0}_categories option present.'.format(component))
            self.component_categories[component] = category_str.split(',')

        # Get the whitelist options.
        try:
            whitelist_file = opts_dict['whitelist_file']
        except KeyError:
            raise ValueError('Option whitelist_file not specified in options file for repo {0}.'
                             '  Unable to proceed with initialization.'.format(name))
        self.whitelist_file = os.path.join(self.root, whitelist_file)

        try:
            wl_override = opts_dict['override_whitelist']
        except KeyError:
            raise ValueError('Option override_whitelist not specified in options file for repo {0}.'
                             '  Unable to proceed with initialization.'.format(name))
        if wl_override.lower() == 'no':
            self.whitelist_override = False
        else:
            self.whitelist_override = True

        # Last thing - need to pull in the url data.
        self.urls = []

        # Pull the primary one first, then search for any mirrors.
        try:
            self.urls.append(opts_dict['url_main'])
        except KeyError:
            raise ValueError('Option url_main not specified in options file for repo {0}.  '
                             'Unable to proceed with initialization.'.format(name))

        # Now try for the remaining mirrors.
        for key in opts_dict:
            if key.startswith('url_mirror'):
                self.urls.append(opts_dict[key])

        # The url is intended to contain the path up to the last non-unique point across mirrors; the path to the
        # Release file is then contained here.
        try:
            self.remote_release_path = opts_dict['release_path']
        except KeyError:
            raise ValueError('Option release_path not specified in options file for repo {0}.  '
                             'Unable to proceed with initialization.'.format(name))

        # Remote pool root is optional - it assists primarily in identifying where the pool path in the Packages
        # Filename field ends and the unique path for that package begins.  '/' is stripped from both ends.
        try:
            self.remote_pool_root = opts_dict['remote_pool_root'].strip('/')
        except KeyError:
            self.remote_pool_root = ''

        # package_field_order is optional - it allows the administrator to write out the field order in a specific
        # order rather than the arbitrary one that dictionaries create.
        try:
            field_order = opts_dict['package_field_order'].replace(' ', '')
            self.package_field_order = field_order.split(',')
        except KeyError:
            self.package_field_order = []

    def sync(self):
        """
        Attempts to update the local repository copy against the contents of the remote repository.  Refreshes the
        local index cache and ensures that the indices are cryptographically valid, then updates the actual package
        store and the local repository index files, signing and getting cryptohashes for each one.
        :return: On successful update, a populated api.updated_pkg_data.UpdatedPackageData() object is returned.
                 On failure, None is returned.
        """

        done = False
        updated_pkg_data = api.updated_pkg_data.UpdatedPackageData()

        # We have to take into account the fact that some mirrors may be compromised and return non-verifiable
        # index files or packages.  We may have to restart the process, so the sync command will loop until all
        # packages have been safely updated or until we have eliminated all mirrors as safe.

        # Make sure the repo directory state is correct and fully present before proceeding.
        self.verify_repo_state()

        # Start by updating the release caches.
        success = self.update_cached_release()

        if not success:
            logger.error("All mirrors failed to return a valid remote Release index, or remote Release file "
                         "has not been updated.  Sync will halt.")
            return None

        success = self.parse_cached_release()

        if not success:
            logger.error("Unable to parse the local cached Release file.  Cannot proceed with sync.")
            return None

        # Release files have been updated - load our whitelist.
        self.read_whitelist()

        # Now to start on the Packages, Source and i18n indices, and the actual package updates.
        for component in self.component_list:
            # get the list of categories in each component, iterate over them...
            category_list = self.component_categories[component]
            for category in category_list:
                # We want to manage each category according to it's specific requirements.  So, binary
                # we're getting the Packages indices and updating the local binary package files; source we get
                # the source indices etc.
                if category == 'binary':
                    # Update the cached Package index files for all binary categories.
                    self.update_cached_pkg_index(component, category)

                    # Now read in the cached Package value against the whitelist contents to get a trimmed list
                    # of packages whose versions we can compare against our local install base.
                    for arch in self.arch_list:
                        full_category = 'binary-' + arch
                        logger.debug('Beginning local Package file update for repo {0}, '
                                     'component {1}, category {2}.'.format(self.root, component, full_category))

                        remote_pkg_filename = self.generate_cache_filename('Packages', component, full_category)
                        remote_pkg_path = os.path.join(self.cache_dir, remote_pkg_filename)
                        remote_pkg_list = self.read_pkg_index_file(remote_pkg_path)

                        # Clear non-whitelisted packages from the remote_pkg_list.
                        remote_pkg_list = self.apply_whitelist(remote_pkg_list, component, category)

                        # Now we need to read in the local repository's Package object, if it exists.
                        local_pkg_glob = os.path.join(self.repo_dir, component, full_category, 'Packages*')
                        pkg_glob = glob.glob(local_pkg_glob)

                        # It doesn't really matter which one we open, but if there's an uncompressed version
                        # available we have so much less to do, so that's our default.
                        if local_pkg_glob[:-1] in pkg_glob:
                            local_pkg_path = local_pkg_glob[:-1]
                        elif local_pkg_glob[:-1] + '.bz2' in pkg_glob:
                            local_pkg_path = local_pkg_glob[:-1] + '.bz2'
                        elif local_pkg_glob[:-1] + '.gz' in pkg_glob:
                            local_pkg_path = local_pkg_glob[:-1] + '.gz'
                        else:
                            logger.error('No Packages file of recognizable compression '
                                         'type in {0}'.format(local_pkg_glob[:-1]))
                            local_pkg_path = ''

                        # Read in the local package cache, if one exists.
                        if local_pkg_path:
                            local_pkg_list = self.read_pkg_index_file(local_pkg_path)
                        else:
                            local_pkg_list = {}

                        # Now compare the whitelisted packages against the local packages, and update the local
                        # packages as required.
                        updated_list = self.compare_pkg_versions(remote_pkg_list, local_pkg_list)

                        # Add the new packages to the updated_pkg_data list for returning to the calling framework.
                        self.record_updates(updated_list, updated_pkg_data, full_category)

                        # Now actually pull down the packages in the updated_list.
                        local_pkg_list = self.update_local_repository(updated_list, local_pkg_list)

                        # And write out the component/binary-arch/Packages files.
                        success = self.write_package_index(local_pkg_list, component, full_category)

                        if not success:
                            logger.error('Attempt to write Packages file for component {0}, category {1} '
                                         'failed.  Please investigate.'.format(component, full_category))
                            # Not much we can do - the original Package file will still be in place and
                            # pointing to old packages, so nothing corrupted.  continue on...

        # After all of the individual index files are created, we need to generate a new Release file.
        self.generate_new_local_release()

        return updated_pkg_data

    def record_updates(self, updated_pkg_list, api_pkg_data, category):
        """
        Takes a dictionary of updated package objects and extracts the information that the api_pkg_data object
        requires for each package.  On completion the api_pkg_data object will have a valid record for every
        updated object in the package dictionary.
        :param updated_pkg_list: A dictionary of package objects which have been updated by the debian plugin.
        :param api_pkg_data: A data storage class provided by the framework API to allow the plugin to return
        structured information about the packages it has updated back to the calling framework.
        :param category: The type of package this is - binary, source, i18n, etc; if binary we use the category
        and architecture designation (binary-amd64)
        :return:
        """

        date = datetime.datetime.now()

        print("Updated package list passed to record_updates with {0} entries in it.".format(len(updated_pkg_list)))

        for package in updated_pkg_list:
            version_string = package['Version']
            name = package['Package']
            print("Adding package {0}, version {1} to the api_pkg_data variable.".format(name, version_string))
            api_pkg_data.add_pkg_modification(name, 'debian', category, version_string, date, 'update')

    def verify_repo_state(self):
        """
        Checks the state of the repository to confirm that it is consistent with the settings created.  If directories
        are missing, they WILL be created.  This function does NOT attempt to create missing indices - those are only
        generated at the end of a successful update.
        :return:
        True if the directory structure was intact.
        False if the directory structure needed to be modified.
        Errors during processing will raise an exception.
        """

        retval = True

        # Create a list of all of the directories we should have.
        dir_structure = [self.root, self.cache_dir, self.pool_dir, self.repo_dir]

        # Cover the components and categories...
        for component in self.component_list:
            component_dir = os.path.join(self.repo_dir, component)
            dir_structure.append(component_dir)
            for category in self.component_categories[component]:
                if category == 'binary':
                    for arch in self.arch_list:
                        dir_structure.append(os.path.join(component_dir, '{1}-{0}'.format(arch, category)))
                else:
                    dir_structure.append(os.path.join(component_dir, category))

        # Check our directory structure.
        for directory in dir_structure:
            # If one of the directories does not exist, create it.
            if not os.path.exists(directory):
                retval = False
                os.makedirs(directory, conf.pkg_manager.default_perms)
                owner = pwd.getpwnam(conf.pkg_manager.default_owner)[2] if conf.pkg_manager.default_owner else -1
                group = grp.getgrnam(conf.pkg_manager.default_group)[2] if conf.pkg_manager.default_group else -1
                os.chown(self.root, owner, group)

        return retval

    def read_whitelist(self):
        """
        When called, opens and reads the whitelist file at the location defined by self.whitelist, and reads it in to
        create the local whitelist data structure.
        :return:
        """

        try:
            fd = open(self.whitelist_file, 'r')
        except IOError as err:
            logger.debug('Unable to open whitelist file {0}.  Error {1} '
                         'occurred.'.format(self.whitelist_file, err.message))
            self.whitelist = {}
            return

        component = ''
        # Read the file line by line, checking for comments, [component] delimiters, and regular package to category
        # lines.
        for line in fd:
            # Ignore blank lines.
            if line.strip() == '' or line.strip == '\n':
                continue

            # Ignore any lines that start with #.  We're not going to support comments midway through a line...
            if line.strip().startswith('#'):
                continue

            # If the first non-whitespace character in a line is [, then we're identifying a component.
            if line.strip().startswith('['):
                component = line.strip('[]\n')
                if component not in self.whitelist:
                    self.whitelist[component] = {}
            else:
                # If it's not a component line, then it must be a regular package to category definition line.
                # The package name is the first item; package names cannot have whitespace according to debian spec so
                # we split the line once from the left on any whitespace and take the first element.
                pkg_name = line.split(None, 1)[0]
                # Categories are all the rest of the line.
                category_str = line.split(None, 1)[1].strip()
                category_list = category_str.replace(' ', '').split(',')

                # Add the pkg_name and category list to the whitelist dictionary for component.
                self.whitelist[component][pkg_name] = category_list

        fd.close()

    def write_whitelist(self):
        """
        Takes the existing whitelist stored in self.whitelist and writes it out to the file pointed to by
        self.whitelist_file.  Note that this file does not guarantee any form of alphabetical output, as we do not
        sorting and dictionaries do not keep a particular order.
        :return:
        """

        try:
            fd = open(self.whitelist_file, 'w+')
        except IOError as err:
            logger.error('Unable to open whitelist file {0} for writing.  '
                         'Error {1} has occurred.'.format(self.whitelist_file, err.message))
            return

        # iterate over the components in the whitelist top level.
        for component in self.whitelist:
            logger.debug('Writing component name {0} out to file {1}.'.format(component, self.whitelist_file))
            output_str = '[' + component + ']\n'
            fd.write(output_str)
            # And then over the packages and category list in each component.
            for pkg_name, category_list in self.whitelist[component].iteritems():
                logger.debug('Writing package {0} and category list {1} to '
                             'file {2}.'.format(pkg_name, category_list, self.whitelist_file))
                output_str = pkg_name + '\t' + ', '.join(category_list) + '\n'
                fd.write(output_str)

        fd.close()

        # Change owner of the file to the default, to be safe.
        owner = pwd.getpwnam(conf.pkg_manager.default_owner)[2] if conf.pkg_manager.default_owner else -1
        group = grp.getgrnam(conf.pkg_manager.default_group)[2] if conf.pkg_manager.default_group else -1
        os.chown(self.whitelist_file, owner, group)

    def add_to_whitelist(self, pkg_name, component_category_dict):
        """
        Given a pkg name, and a list of components and categories the pkg is allowed to be added to/updated from,
        add_to_whitelist will add the package into the whitelist.  Note that the new addition is temporary unless
        write_whitelist() is called.
        :param pkg_name: Name of the package to be added to the whitelist.  This is just the plain name - no version
        data should be added unless it is explicitly included in the package name itself.
        :param component_category_dict: A dictionary mapping component names to categories; the whitelist for each
        pairing of component:category will have the pkg_name added.  The dictionary keys are single strings, while
        the dictionary values are lists of category names.
        :return:
        """

        for component in component_category_dict:
            # Either retrieve or create a dictionary from/in the whitelist.
            try:
                pkg_dict = self.whitelist[component]
            except KeyError:
                logger.error('The component {0} does not exist in the whitelist dictionary.  Adding...')
                self.whitelist[component] = {}
                pkg_dict = self.whitelist[component]

            # Add the package name and the list of packages to the retrieved/new dictionary.
            # We assume that any data here is valid and overrides existing whitelist data, so we don't append the
            # category list if the package is already in the whitelist - we just overwrite it.
            pkg_dict[pkg_name] = component_category_dict[component]

    # def update_cached_package(self, component, category, force=False):
    def update_cached_pkg_index(self, component, force=False):
        """
        Given a component and category name, the method uses the release data to identify the smallest (most highly
        compressed) Packages index with the strongest level of cryptohash available, and if it has been updated
        more recently than the existing local cache, proceeds to download the newest version.
        :param component: The name of the component whose Package index file we want to update.
        :param force: Method will attempt to download Package file regardless of value of local timestamp.
        :return: True if the Package file was updated; False in all other cases.
        """

        # Identify the type of category first.  If binary, we need to examine all of the architectures supported...
        # if category == 'binary':
        category = ['binary-' + arch for arch in self.arch_list]
        # else:
        #    # To simplify our processing strategy, just turn category into a list of length 1.
        #    category = [category]

        # Now identify the strongest hash available in the Release contents.
        if 'SHA256' in self.release_contents:
            hash_type = 'SHA256'
        elif 'SHA1' in self.release_contents:
            hash_type = 'SHA1'
        elif 'MD5Sum' in self.release_contents:
            hash_type = 'MD5Sum'
        else:
            logger.error("No recognized hash function in Release contents.  Plugin cannot handle this repository's "
                         "chain of trust.  Throwing exception.")
            raise NotImplementedError('No recognizable hash type in Release file; no implementation to handle it.')

        logger.debug('Best hash is {0}'.format(hash_type))

        # and now search all of the entries in self.release_contents[hash] to find Packages files for all
        # component:category pairings, check for existence and datetime modified, then update if necessary.
        for item in category:
            # Identify the local Package cache name.
            local_package_name = self.generate_cache_filename('Packages', component, item)
            local_package_path = os.path.join(self.cache_dir, local_package_name)

            logger.debug('Local Packages file given cache name {0}, '
                         'in path {1}'.format(local_package_name, self.cache_dir))

            # Get the timestamp on the local cache.
            if (not force) and os.path.isfile(local_package_path):
                local_ts = datetime.datetime.fromtimestamp(os.path.getmtime(local_package_path))
                logger.debug('Cached Packages file exists with timestamp {0}'.format(local_ts))
            else:
                local_ts = None

            # All of the following work needs to be done in the anticipation of a failed download from the remote
            # source.  So we loop over this until one of the URLs has a working entry.
            for url in self.urls[:]:
                # and the start of the path to the packages file.
                path_prefix = component + '/' + item + '/Packages'
                logger.debug('Path prefix for search is: {0}'.format(path_prefix))

                # Initially we were keeping only the best matching Package file.  However, it appears that some
                # repositories play fast and loose with the Release file, and do not necessarily HAVE every file
                # in the repository that Release says should be there.  Therefore, we're going to keep a list of
                # matches, ordered from the smallest matching file to the largest.
                best_pkg_index = []

                # Now find Packages entries for the remote index...
                for index in self.release_contents[hash_type]:
                    # file indices are 3 item lists - hash value, size, and full path.
                    logger.debug('Examining release_contents key {0}, index entry {1}'.format(hash_type, index))

                    if index[2].startswith(path_prefix) and \
                            (index[2].endswith('.bz2') or index[2].endswith('.gz') or index[2].endswith('Packages')):
                        logger.debug('Index points to Packages file for this component:category.  Testing size.')
                        for i in range(0, len(best_pkg_index)):
                            if int(index[1]) < int(best_pkg_index[i][1]):
                                logger.debug('Index size smaller than previously found: '
                                             '{0} vs {1} - inserting.'.format(index[1], best_pkg_index[i][1]))
                                best_pkg_index.insert(i, index)
                                break
                        else:
                            logger.debug('best_pkg_index either empty or all entries '
                                         'smaller than selected.  Appending.')
                            best_pkg_index.append(index)

                if len(best_pkg_index) < 1:
                    logger.error('No Packages file of type bz2, gzip or uncompressed listed in Release file.  Cannot '
                                 'continue with Package file update.')
                    return False

                # The best_pkg_index will now point to a list of index files.  This one we want to download.  We need
                # to form the full remote path; all files are relative to the path that the Release file is in, so
                # we can knock Release off the end and stick them together.
                hash_value = ''
                file_type = ''
                for package in best_pkg_index:
                    pkg_path = self.remote_release_path.strip('Release') + package[2]
                    logger.debug('Attempting to download {0}'.format(pkg_path))
                    try:
                        raw_pkg_data = download_file(url, pkg_path, local_ts)
                    except httplib.HTTPException:
                        logger.error('Unable to download file {1} from {0}.  Trying next...'.format(pkg_path, url))
                        continue

                    # So we don't have to loop ALL the rest of the method, get the successfully downloaded file
                    # type now.
                    filename_bits = package[2].split('.')
                    if len(filename_bits) <= 1:
                        logger.debug('Remote file {0} appears to be uncompressed.'.format(filename_bits))
                        file_type = 'uncompressed'
                    else:
                        logger.debug('Remote file {0} is of type {1}.'.format(package[2], filename_bits[1]))
                        file_type = filename_bits[1]
                    hash_value = package[0]
                    break
                else:
                    logger.error('None of the Packages files listed in Release for component {0}, category {1}'
                                 'exist in the remote repository.  Exiting with error.'.format(component, item))
                    return False

                # Now to check the signature on the downloaded file and ensure it matches what our verified Release
                # entry says it should be.
                if hash_type == 'SHA256':
                    hash_obj = hashlib.sha256()
                elif hash_type == 'SHA1':
                    hash_obj = hashlib.sha1()
                else:
                    hash_obj = hashlib.md5()

                hash_obj.update(raw_pkg_data)

                # If the provided hash does not match the hash from downloading,
                if hash_obj.hexdigest() != hash_value:
                    logger.error('Package file {0} at URL {1} hash value {2} did not match the hash value {3}'
                                 'in the Release file.  Removing URL as potentially tainted.'
                                 ''.format(best_pkg_index[2], url, hash_obj.hexdigest, best_pkg_index[0]))
                    self.urls.remove(url)
                    # Try again with the next URL.
                    continue
                else:
                    break
            else:
                logger.error('Tried all URLs in url list; none had a valid Packages file present.  Exiting...')
                return False

            # Otherwise, write out the contents of the file (note that if it is zipped, we want to unzip it.
            # Decompress data before writing out, if necessary.
            if file_type == 'bz2':
                pkg_data = bz2.decompress(raw_pkg_data)
            elif file_type == 'gz':
                fd = gzip.GzipFile(fileobj=StringIO.StringIO(raw_pkg_data))
                pkg_data = fd.read()
            elif file_type == 'uncompressed':
                pkg_data = raw_pkg_data
            else:
                logger.error('Remote Package file is unknown type.  Not writing, returning failure result.')
                return False

            # Write out the updated, decompressed Packages data.
            fd = open(local_package_path, 'w+')
            fd.write(pkg_data)
            fd.close()

            # Change the owner of the file to the default.
            owner = pwd.getpwnam(conf.pkg_manager.default_owner)[2] if conf.pkg_manager.default_owner else -1
            group = grp.getgrnam(conf.pkg_manager.default_group)[2] if conf.pkg_manager.default_group else -1
            os.chown(local_package_path, owner, group)

    def update_cached_release(self, force=False):
        """
        Looks for a local copy of the Release file and, if present, extracts the files last modified date and time.
        Will do the same thing for the Release.gpg file.  Calls download_file with the remote repository path and
        the last modified time, then saves the file into the cache directory with a unique filename.
        :param force: If it is necessary to redownload the Release file for some reason (e.g. a later step finds one
        of the Package or source index files to be compromised, meaning the mirror cannot be trusted) then setting
        force to True will make the method download the Release file regardless of the state of the cached version.
        :return: True if the Release file was updated and validated; False otherwise.
        """

        cached_release = self.generate_cache_filename('Release')
        logger.debug('Cached Release file will be named {0}'.format(cached_release))
        cached_release_sig = self.generate_cache_filename('Release.gpg')
        logger.debug('Cached Release signature file will be named {0}'.format(cached_release_sig))
        local_ts = None
        retval = False

        # Check if the local cached copies exist and get the timestamp if they do, or if force is enabled then
        # set up for required download.
        if (not force) and os.path.isfile(os.path.join(self.cache_dir, cached_release)):
            local_ts = datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(self.cache_dir, cached_release)))
            logger.debug("Release file has modified date and time of {0}".format(local_ts))

        # Iterate over our URLs, trying each one in turn until either a) we have determined the Release file has not
        # changed, or b) it has changed, and we have downloaded and cryptographically verified the validity of the
        # downloaded Release file.
        for url in self.urls[:]:
            logger.debug('Trying url {0}.'.format(url))
            # Make a shot at pulling down the Release file.
            release_data = download_file(url, self.remote_release_path, local_ts)

            # If the remote copy is not newer than the local copy, there is nothing for us to do.
            if release_data is None:
                logger.debug('None returned for release_data - either the download failed or the remote file has'
                             'not been updated.')
                return False

            logger.debug('Data returned for Release file - acquiring signature file.')
            # If release_data is not None, then Release has been updated and we have pulled down the file.  Get
            # the signature too.  Don't bother with the timestamp - we need the newest copy.
            release_sig_data = download_file(url, self.remote_release_path + '.gpg')

            # Write both out to cache.
            logger.debug('Writing file {0} to disk.'.format(os.path.join(self.cache_dir, cached_release)))
            fd = open(os.path.join(self.cache_dir, cached_release), 'w+')
            fd.write(release_data)
            fd.close()

            # Change release file owner to default.
            owner = pwd.getpwnam(conf.pkg_manager.default_owner)[2] if conf.pkg_manager.default_owner else -1
            group = grp.getgrnam(conf.pkg_manager.default_group)[2] if conf.pkg_manager.default_group else -1
            os.chown(os.path.join(self.cache_dir, cached_release), owner, group)

            logger.debug('Writing file {0} to disk.'.format(os.path.join(self.cache_dir, cached_release_sig)))
            fd = open(os.path.join(self.cache_dir, cached_release_sig), 'w+')
            fd.write(release_sig_data)
            fd.close()

            owner = pwd.getpwnam(conf.pkg_manager.default_owner)[2] if conf.pkg_manager.default_owner else -1
            group = grp.getgrnam(conf.pkg_manager.default_group)[2] if conf.pkg_manager.default_group else -1
            os.chown(os.path.join(self.cache_dir, cached_release_sig), owner, group)

            # Now to verify the signature with one of the keys in the public keyring.
            pubring = conf.pkg_manager.public_keyring if conf.pkg_manager.public_keyring != '' else None

            gpg = gnupg.GPG(gnupghome=conf.pkg_manager.keypath, keyring=pubring)

            logger.debug('Attempting to verify Release file {0} with signature {1}...'.format(
                os.path.join(self.cache_dir, cached_release_sig),
                os.path.join(self.cache_dir, cached_release)
            ))

            # The verify_data method requires a path to the sig file, but takes data in memory...
            verification = gpg.verify_data(os.path.join(self.cache_dir, cached_release_sig),
                                           release_data)

            if not verification:
                logger.error('The Release file {0} failed verification against '
                             'gpg checksum {1}'.format(cached_release, cached_release_sig))
                os.remove(os.path.join(self.cache_dir, cached_release_sig))
                os.remove(os.path.join(self.cache_dir, cached_release))
                logger.error('Deleting URL from list as potentially tainted.')
                self.urls.remove(url)
            else:
                logger.debug('The Release file {0} has been successfully verified '
                             'by gpg_checksum {1}.'.format(cached_release, cached_release_sig))
                retval = True
                break
        else:
            logger.error('All downloaded Release files from all URLs provided have failed verification.  Unable to '
                         'continue.')

        return retval

    def generate_new_local_release(self):
        """
        Creates a new local release file and signs it; all directories under the Release file path are searched for
        files.  Each found file is hashed (MD5, SHA1 and SHA256) and the hashes, file size, and path relative to Release
        file directory are stored in the Release file.  The Release file is then signed by the private key specified
        in the configuration.
        :return:
        """

        logger.debug('Generating a new local Release file and signing it.')

        # Release file is saved under the top of the repo_dir.  Get an initial list of all items under self.repo_dir.
        search_paths = glob.glob(os.path.join(self.repo_dir, '*'))
        logger.debug('Top level search paths: {0}'.format(search_paths))

        # Getting the glob of the repo directory means we're including the Release file - get rid of that.
        try:
            search_paths.remove(os.path.join(self.repo_dir, 'Release'))
        except ValueError:
            # The possibility exists that, in a brand new repository, Release won't yet exist.  If it's not there,
            # that's fine - continue on.
            logger.warn('Release file does not exist in {0} - possible problem if '
                        'this is not a new repository.'.format(self.repo_dir))
            pass

        try:
            search_paths.remove(os.path.join(self.repo_dir, 'Release.gpg'))
        except ValueError:
            # Just like there may not be a Release, there may also not be a Release.gpg.
            logger.warn('Release.gpg file does ont exist in {0} - possible problem if '
                        'this is not a new repository.'.format(self.repo_dir))

        # Now recurse through subdirectories and identify all files for the Release output.
        files = []
        while len(search_paths) > 0:
            # Set up our hash libraries.
            md5_hasher = hashlib.md5()
            sha1_hasher = hashlib.sha1()
            sha256_hasher = hashlib.sha256()

            # Pull the first item off the list.
            file_object = search_paths.pop(0)

            # Check if file or directory.
            if os.path.isfile(file_object):
                logger.debug('{0} is a file.'.format(file_object))
                stream = open(file_object, 'r')
                data = stream.read()
                stream.close()

                md5_hasher.update(data)
                md5_hash = md5_hasher.hexdigest()
                sha1_hasher.update(data)
                sha1_hash = sha1_hasher.hexdigest()
                sha256_hasher.update(data)
                sha256_hash = sha256_hasher.hexdigest()
                logger.debug('Hashes for {0} have been generated.'.format(file_object))

                size = os.stat(file_object).st_size
                logger.debug('File size of {0} is {1}.'.format(file_object, size))

                # Store our data in a list, data order md5, sha1, sha256, file size, and file path.
                # we need the file path relative to the Release file, NOT the whole thing!!
                files.append([md5_hash, sha1_hash, sha256_hash, size, os.path.relpath(file_object, self.repo_dir)])
            elif os.path.isdir(file_object):
                sub_items = glob.glob(os.path.join(file_object, '*'))
                logger.debug('{0} is a directory; the following items are contained '
                             'in it: {1}'.format(file_object, sub_items))
                search_paths.extend(sub_items)

        # All files have been identified and secure hashed; results are stored in files list.
        # Now we need to write out a modified selection of the Release data.  Specifically, the Date,
        # Architectures, Components, MD5Sum, SHA1, and SHA256 fields need to be updated with our data.
        release_str = ''

        release_str += 'Origin: ' + '\n '.join(self.release_contents['Origin']) + '\n'
        release_str += 'Label: ' + '\n '.join(self.release_contents['Label']) + '\n'
        release_str += 'Suite: ' + '\n '.join(self.release_contents['Suite']) + '\n'
        release_str += 'Version: ' + '\n '.join(self.release_contents['Version']) + '\n'
        release_str += 'Codename: ' + '\n '.join(self.release_contents['Codename']) + '\n'

        dt = datetime.datetime.utcnow()
        release_str += 'Date: ' + dt.strftime('%a, %d %b %Y %H:%M:%S %Z') + '\n'

        arch_string = ' '.join(self.arch_list)
        release_str += 'Architectures: ' + arch_string + '\n'

        cmp_string = ' '.join(self.component_list)
        release_str += 'Components: ' + cmp_string + '\n'

        release_str += 'Description: ' + '\n '.join(self.release_contents['Description']) + '\n'

        # Now to form the MD5Sum, SHA1, and SHA256 strings.
        md5_str = 'MD5Sum: \n'
        sha1_str = 'SHA1: \n'
        sha256_str = 'SHA256: \n'
        for file_stats in files:
            md5_str += ' {0}{1:17} {2}\n'.format(file_stats[0], file_stats[3], file_stats[4])
            sha1_str += ' {0}{1:17} {2}\n'.format(file_stats[1], file_stats[3], file_stats[4])
            sha256_str += ' {0}{1:17} {2}\n'.format(file_stats[2], file_stats[3], file_stats[4])

        # Each string should end with a newline, so we can just stack them up.
        release_str += md5_str
        release_str += sha1_str
        release_str += sha256_str

        # Write it out.
        release_file = os.path.join(self.repo_dir, 'Release')
        release_gpg_file = os.path.join(self.repo_dir, 'Release.gpg')

        try:
            stream = open(release_file, 'w+')
            stream.write(release_str)
            stream.close()
        except IOError:
            logger.error('Unable to open or save data to Release file {0}.  Unable to complete processing'
                         'updates to the repository.'.format(release_file))
            return False

        # Change the owner of the release file.
        owner = pwd.getpwnam(conf.pkg_manager.default_owner)[2] if conf.pkg_manager.default_owner else -1
        group = grp.getgrnam(conf.pkg_manager.default_group)[2] if conf.pkg_manager.default_group else -1
        os.chown(release_file, owner, group)

        # Get the Release file signed.  Get the default signing key data from the global config.
        pubring = conf.pkg_manager.public_keyring if conf.pkg_manager.public_keyring != '' else None
        secring = conf.pkg_manager.private_keyring if conf.pkg_manager.private_keyring != '' else None
        homedir = conf.pkg_manager.keypath
        keyname = conf.pkg_manager.key_name

        # We also need to have the passphrase ready for use with signing.  We assume there's no special formatting
        # around the password.
        stream = open(conf.pkg_manager.key_password_file, 'r')
        password = stream.readline()
        stream.close()
        if password.endswith('\n'):
            password = password.strip()

        gpg = gnupg.GPG(gnupghome=homedir, keyring=pubring, secret_keyring=secring)

        signature_data = gpg.sign(release_str, keyid=keyname, passphrase=password, detach=True)

        # And write the signature file.
        try:
            stream = open(release_gpg_file, 'w+')
            stream.write(str(signature_data))
            stream.close()
        except IOError:
            logger.error('Unable to open or save data to Release signature file {0}.  Unable to complete'
                         'processing updates to the repository.'.format(release_gpg_file))
            return False

        # Change the owner of the Release signature.
        owner = pwd.getpwnam(conf.pkg_manager.default_owner)[2] if conf.pkg_manager.default_owner else -1
        group = grp.getgrnam(conf.pkg_manager.default_group)[2] if conf.pkg_manager.default_group else -1
        os.chown(release_gpg_file, owner, group)

        return True

    def write_package_index(self, pkg_index, component, category):
        """
        Given a dictionary representation of a package index file, write the package index out into the correct
        subdirectory (determined by the component and category parameters.)
        :param pkg_index: dictionary of packages, key is package name, value is the dictionary of the actual package
        fields.
        :param component: The name of the component that the Packages file is a part of.
        :param category: The category that the Packages file is a part of.  Note this must be the complete
        category name (e.g. binary-amd64) and not just the binary- prefix, as the Packages file is unique to each
        such category.
        :return: True if the Packages file could be written in at least one form (uncompressed, compressed with gzip,
        or compressed with bzip2.)
                 False if the Packages file could not be written at all.
        """

        # Piece together the output path.
        packages_path = os.path.join(self.repo_dir, component, category)
        # Make sure the path does not end in /, it just makes for more confusing cases later.
        packages_path = packages_path.rstrip('/')

        # Test that the path exists.
        if not os.path.exists(packages_path):
            # Create the path.
            os.makedirs(packages_path, conf.pkg_manager.default_perms)
            # Change ownership of directories.
            owner = pwd.getpwnam(conf.pkg_manager.default_owner)[2] if conf.pkg_manager.default_owner else -1
            group = grp.getgrnam(conf.pkg_manager.default_group)[2] if conf.pkg_manager.default_group else -1

            temp = packages_path
            while temp != self.repo_dir:
                # Walk backwards from the lowest directory to the pool root.
                os.chown(temp, owner, group)
                temp = os.path.split(temp)[0]

        # We want to write out uncompressed, gzip and bzip2 so that client software has tons of flexibility.
        # Do uncompressed first.
        try:
            unc_stream = open(os.path.join(packages_path, 'Packages'), 'w+')
        except IOError:
            logger.error('Unable to open file {0} for uncompressed writing.'.format(packages_path + '/Packages'))
            unc_stream = None
        try:
            gzip_stream = gzip.GzipFile(os.path.join(packages_path, 'Packages.gz'), 'w')
        except IOError:
            logger.error('Unable to open file {0} for gzip writing.'.format(packages_path + '/Packages.gz'))
            gzip_stream = None
        try:
            bz2_stream = bz2.BZ2File(os.path.join(packages_path, 'Packages.bz2'), 'w')
        except IOError:
            logger.error('Unable to open file {0} for bzip2 writing.'.format(packages_path + '/Packages.bz2'))
            bz2_stream = None

        if unc_stream is None and gzip_stream is None and bz2_stream is None:
            logger.error('No output stream could be opened to write the Packages file out.  Terminating attempt.')
            return False

        # All three file streams created.  Now to turn pkg_index into text and output records.
        # Might as well go ahead and sort our lookups by package name.
        pkg_names = sorted(pkg_index.keys())

        # iterate over the pkg_index keys (e.g. package names.)
        for name in pkg_names:
            logger.debug('Writing record for package {0} out.'.format(name))
            package = pkg_index[name]

            # Use the package-field-list to create our output string, if it exists.
            out_str = ''
            for field in self.package_field_order:
                try:
                    out_str += field + ': ' + package[field] + '\n'
                # Remove the field from the package on completion.
                    package.pop(field)
                except KeyError:
                    logger.debug('Package {0} missing user-specified field {1}.  Leaving out.'.format(name, field))

            logger.debug('Outputting any remaining fields in arbitrary order.')
            # Now, in case there are still fields in the package that have not been output:
            for field in package:
                out_str += field + ': ' + package[field] + '\n'
                # Above we removed the field, but we don't need to do that here.

            logger.debug('Finished creating output string.  Writing out to file now...')
            # Add a closing \n to the out_str so the next record written is double spaced, and write it.
            out_str += '\n'

            if unc_stream:
                logger.debug('Writing uncompressed...')
                unc_stream.write(out_str)
            if gzip_stream:
                logger.debug('Writing gzip compressed...')
                gzip_stream.write(out_str)
            if bz2_stream:
                logger.debug('Writing bzip2 compressed...')
                bz2_stream.write(out_str)

        if unc_stream:
            unc_stream.close()
        if gzip_stream:
            gzip_stream.close()
        if bz2_stream:
            bz2_stream.close()

        owner = pwd.getpwnam(conf.pkg_manager.default_owner)[2] if conf.pkg_manager.default_owner else -1
        group = grp.getgrnam(conf.pkg_manager.default_group)[2] if conf.pkg_manager.default_group else -1
        os.chown(os.path.join(packages_path, 'Packages'), owner, group)
        os.chown(os.path.join(packages_path, 'Packages.gz'), owner, group)
        os.chown(os.path.join(packages_path, 'Packages.bz2'), owner, group)

        return True

    def update_local_repository(self, update_list, local_pkg_index):
        """
        Reads each Package record out of the update_list one at a time, identifies the remote path information and
        pulls the file down from the remote mirror and stores it in the local pool (using the tail of the remote
        path, if not the entire thing.)  The saved data is hashed by md5, sha1 and sha256 and then the new path
        and hashes are updated in the package object.  The modified package object is saved into the local_pkg_index.
        :param update_list: List of packages to update.
        :param local_pkg_index: Dictionary of all packages that the local repository houses.
        :return: Returns the local_pkg_index on success.
                 Returns None if an error occurs during processing.
        """

        logger.debug('Updating local repository; {0} packages have changed or been added.'.format(len(update_list)))

        for package in update_list:
            # The package Filename attribute is relative to the top of the remote repository - we can just pass
            # the two parts to download_file.
            # Iterate over the URLs
            logger.debug('Attempting to update package {0}'.format(package['Filename']))
            for url in self.urls[:]:
                # Try to download the remote package.
                pkg_data = download_file(url, package['Filename'])
                if 'SHA256' in package:
                    logger.debug('Selected SHA256 as hash verification function.')
                    hash_func = hashlib.sha256()
                    hash_value = package['SHA256']
                elif 'SHA1' in package:
                    logger.debug('Selected SHA1 as hash verification function')
                    hash_func = hashlib.sha1()
                    hash_value = package['SHA1']
                elif 'MD5sum' in package:
                    logger.debug('Selected MD5 as hash verification function.')
                    hash_func = hashlib.md5()
                    hash_value = package['MD5sum']
                else:
                    logger.debug('Unknown secure hash associated with package {0}.  '
                                 'Rejecting package..'.format(package['Package']))
                    continue

                hash_func.update(pkg_data)
                dl_hash_value = hash_func.hexdigest()

                # If the hash value of the downloaded file does not match the expected value from the package record,
                # reject this package and try the next URL.
                if hash_value != dl_hash_value:
                    logger.error('Downloaded copy of package {0} does not produce correct hash value.  '
                                 'Expected {1}, produced {2}'.format(package['Filename'], hash_value, dl_hash_value))
                    continue

                # If we get here, then hash_value must equal dl_hash_value and we move on.
                break
            else:
                logger.error('No URL was able to provide a valid copy of package '
                             '{0}.  Skipping.'.format(package['Filename']))
                continue
                # If the hash value of the downloaded file match the expected value from the update list, save it
                # to the local pool.

            # Need to remove the remote pool path from the package, and ensure the path has no leading '/'
            path = package['Filename']

            # If the path in the package has the remote pool root at the base, then we want to yank that out.
            if path.startswith(self.remote_pool_root):
                local_path = path[len(self.remote_pool_root) + 1:]
            else:
                local_path = path
            # and add our own local pool path to the package file path.
            full_path = os.path.join(self.pool_dir, local_path)

            # Check to make sure we actually have a local directory path to the file's resting place:
            temp = os.path.split(full_path)[0]
            if not os.path.exists(temp):
                os.makedirs(temp, conf.pkg_manager.default_perms)

                # Change ownership of directories.
                owner = pwd.getpwnam(conf.pkg_manager.default_owner)[2] \
                    if conf.pkg_manager.default_owner else -1
                group = grp.getgrnam(conf.pkg_manager.default_group)[2] \
                    if conf.pkg_manager.default_group else -1
                while temp != self.pool_dir:
                    # Walk backwards from the lowest directory to the pool root.
                    os.chown(temp, owner, group)
                    temp = os.path.split(temp)[0]

            # Directories are now properly owned and secured.  Go ahead with saving the data.
            file_stream = open(full_path, 'w+')
            file_stream.write(pkg_data)
            file_stream.close()

            owner = pwd.getpwnam(conf.pkg_manager.default_owner)[2] if conf.pkg_manager.default_owner else -1
            group = grp.getgrnam(conf.pkg_manager.default_group)[2] if conf.pkg_manager.default_group else -1
            os.chown(full_path, owner, group)

            # We want the top of the pool directory relative to the root of the web-exposed directory.  The pool
            # directory must sit below the web-exposed root, so if we remove that we'll be left only with the path
            # to the pool directory.

            logger.debug('Building relative path to package {0} from web_root {1}:'.format(full_path, self.web_root))
            relative_path = os.path.relpath(full_path, self.web_root)
            logger.debug('Relative path is: {0}'.format(relative_path))

            # if self.web_root.endswith('/'):
            #    relative_path = full_path[len(self.web_root):]
            # else:
            #    relative_path = full_path[len(self.web_root) + 1:]

            # update the package object with the new filename, and then update the dictionary with the new
            # package.
            package['Filename'] = relative_path
            local_pkg_index[package['Package']] = package

        # Return the updated local_pkg_index object.
        return local_pkg_index

    def compare_pkg_versions(self, new_pkg_cont, old_pkg_cont):
        """
        Checks every entry in new_pkg_cont against the contents of old_pkg_cont.  Every package record in new_pkg_cont
        that has a newer version string than what is in old_pkg_cont (or that does not exist in old_pkg_cont) is
        marked and the package object is added to an updated_pkg list.
        :param new_pkg_cont: The contents of a newer Packages file; must be dictionary with package names as keys
        and the package data (also a dictionary) as the value.
        :param old_pkg_cont:  The contents of an older Packages file; must be dictionary with package names as keys
        and the package data (also a dictionary) as the value.
        :return: A list object of packages from new_pkg_cont whose version is newer than (or for which no equivalent
        package exists in) the old_pkg_cont.
        """

        updated_list = []

        logger.debug('Comparing package versions between the new Packages list and the old one.')
        logger.debug('New packages list has {0} packages in it.'.format(len(new_pkg_cont)))
        logger.debug('Old packages list has {0} packages in it.'.format(len(old_pkg_cont)))

        # iterate over all objects in new_pkg_cont
        for pkg_name, package in new_pkg_cont.iteritems():
            new_version = package['Version']
            try:
                old_version = old_pkg_cont[pkg_name]['Version']
            except:
                # Trivial case - the package does not exist in old_pkg_cont.  Add it to updated list and move on.
                logger.debug('Package {0} does not exist in old_pkg_cont - adding to update list.'.format(pkg_name))
                updated_list.append(package)
                continue

            if pkg_name == 'libcurl3-gnutls':
                print('Comparing package {0}: new versions {1}, old version {2}'.format(pkg_name, new_version, old_version))

            # Have the new version and the old version strings ready.
            logger.debug("Comparing against new package %s, version %s.", pkg_name, new_version)

            # if newest_ver.version.find(':') > 0:
            new_epoch_end = new_version.find(':')
            old_epoch_end = old_version.find(':')
            new_epoch = int(new_version[:new_epoch_end]) if new_epoch_end >= 0 else 0
            old_epoch = int(old_version[:old_epoch_end]) if old_epoch_end >= 0 else 0

            logger.debug("New package version epoch is %d", new_epoch)
            logger.debug("Old package version epoch is %d", old_epoch)

            # debian spec dictates epoch is a single unsigned integer.  Convert to int to ensure numerical comparison.
            if int(old_epoch) > int(new_epoch):
                logger.debug("Old package epoch is larger; new package not newer version.  Checking next...")
                continue
            elif int(old_epoch) < int(new_epoch):
                logger.debug("Old package epoch is smaller, so new package is newer version.  Marking package...")
                updated_list.append(package)
                continue

            logger.debug("Epochs are equal.  Checking upstream version next.")
            # Epoch comparison fails, move on to upstream component.
            new_upstr_end = new_version.find('-', max(new_epoch_end, 0))
            old_upstr_end = old_version.find('-', max(old_epoch_end, 0))
            new_upstr_ver = new_version[new_epoch_end + 1:new_upstr_end] if new_upstr_end >= 0 \
                else new_version[new_epoch_end + 1:]
            old_upstr_ver = old_version[old_epoch_end + 1:old_upstr_end] if old_upstr_end >= 0 \
                else old_version[old_epoch_end + 1:]

            logger.debug("Newest package upstream version is %s.", new_upstr_ver)
            logger.debug("Challenging package upstream version is %s.", old_upstr_ver)

            # Test the package versions...
            out = self.debian_version_compare(new_upstr_ver, old_upstr_ver)
            logger.debug("Result of debian_compare: %s is the higher version.", out)

            if out == new_upstr_ver:
                updated_list.append(package)
                logger.debug("New version is higher; replace newest and check next package.")
                if pkg_name == 'libcurl3-gnutls':
                    print('New version is higher; the new version has been added to the updated_list of package objects.')
                continue

            # If there are debian versions, test those.
            new_deb_ver = new_version[new_upstr_end + 1:] if new_upstr_end > 0 else '0'
            old_deb_ver = old_version[old_upstr_end + 1:] if old_upstr_end > 0 else '0'
            logger.debug("Upstream versions are equal.  Checking debian version string.")

            out = self.debian_version_compare(new_deb_ver, old_deb_ver)
            logger.debug("Result of debian_compare: %s is the higher version.", out)

            if out == new_deb_ver:
                updated_list.append(package)
                logger.debug("New version is higher; replace newest and check next package.")
                if pkg_name == 'libcurl3-gnutls':
                    print('New version debian section is higher; the new package object has been added to updated_list.')
                continue

        return updated_list

    def debian_version_compare(self, versiona, versionb):
        """
        Compares the string/digit/string/digit... groupings of versiona and versionb.  If versiona lexically or
        numerically is greater than versionb, then the function returns versiona.  If versionb is lexically or
        numerically greater than versiona, then the function returns versionb.  If both strings are exactly equal,
        the function returns None.
        :param versiona: The first version string to compare according to the debian rules.
        :param versionb: The second version string to compare against the first according to debian rules.
        """

        # Debian assumes an alternating pattern of <string><digit><string><digit>... in its versioning scheme.

        # Start assuming that versiona is less than or equal to versionb.
        larger = False

        # Building a regular expression to pull out the alternating character types is probably doable, but it will
        # be faster to just iterate the strings for realz.
        a_index = 0
        b_index = 0

        logger.debug("Comparing versiona %s against versionb %s", versiona, versionb)
        while a_index < len(versiona) and b_index < len(versionb):
            logger.debug("a_index: %d", a_index)
            logger.debug("b_index: %d", b_index)
            # Start of the text substring (the next character following the end of the digit sequence.)
            a_start = a_index
            b_start = b_index
            logger.debug("a_start: %d", a_start)
            logger.debug("b_start: %d", b_start)

            while a_index < len(versiona) and not versiona[a_index].isdigit():
                logger.debug("Character %s in versiona is not a digit.", versiona[a_index])
                a_index += 1
            while b_index < len(versionb) and not versionb[b_index].isdigit():
                logger.debug("Character %s in versionb is not a digit.", versionb[b_index])
                b_index += 1

            logger.debug("Non-digit segment for versiona computed, a_index now %d", a_index)
            logger.debug("Non-digit segment for versionb computed, b_index now %d", b_index)

            a_str = versiona[a_start:a_index] if a_index <= len(versiona) else ''
            b_str = versionb[b_start:b_index] if b_index <= len(versionb) else ''

            logger.debug("Non-digit segment for versiona is string '%s'", a_str)
            logger.debug("Non-digit segment for versionb is string '%s'", b_str)

            if a_str < b_str:
                logger.debug("String comparison shows that a_str is smaller than b_str; return versionb.")
                return versionb
            elif b_str < a_str:
                logger.debug("String comparison shows that b_str is smaller than a_str; return versiona.")
                return versiona

            # Start of the digit substring (the next character following the end of the numerical sequence.)
            a_start = a_index
            b_start = b_index

            logger.debug("String segments are equal.  Compare subsequent digit sequence.")
            logger.debug("a_start: %d", a_start)
            logger.debug("b_start: %d", b_start)

            while a_index < len(versiona) and versiona[a_index].isdigit():
                logger.debug("Character %s in versiona is a digit.", versiona[a_index])
                a_index += 1
            while b_index < len(versionb) and versionb[b_index].isdigit():
                logger.debug("Character %s in versionb is a digit.", versionb[b_index])
                b_index += 1

            a_int = int(versiona[a_start:a_index]) if len(versiona) >= a_index > a_start else 0
            b_int = int(versionb[b_start:b_index]) if len(versionb) >= b_index > b_start else 0

            logger.debug("Integer segment for versiona is %d", a_int)
            logger.debug("Integer segment for versionb is %d", b_int)

            if a_int < b_int:
                logger.debug("Integer segment for versiona is smaller than versionb, return versionb.")
                return versionb
            elif b_int < a_int:
                logger.debug("Integer segment for versionb is smaller than versiona, return versiona.")
                return versiona

        logger.debug("Exhausted one of versiona or versionb; length check is last.  Shorter string loses,"
                     "longer string is returned.")
        # Match the behavior of python lexical compare.  Note that this will probably cause issues if a package
        # version increments out of alpha, e.g goes from 9.50a to 9.50, as now the new value is shorter and therefore
        # lexicographically smaller....
        if a_index < len(versiona):
            logger.debug("a_index is less than length of versiona, so versiona is longer.  Returning versiona.")
            return versiona
        if b_index < len(versionb):
            logger.debug("b_index is less than length of versionb, so versionb is longer.  Returning versionb.")
            return versionb

        logger.debug("Both strings are identical.  Returning None.")

        return None

    def read_pkg_index_file(self, path):
        """
        Given a path, reads in the contents of the entire Packages index file for that
        component and binary-<arch>.  Returns a dictionary object of format {pkg_name:{record}}.
        :param path: Takes a single argument - the path to the Packages file.  We take a path so that this
        method can be called for both the cached Packages files and the local repo Package files.
        :return: Dictionary object of Packages file records keyed to their respective names.
                 None if the path could not be opened or the contents wholly unreadable as a Package index file.
        """

        retval = {}

        # The Packages path that is passed in may be a cached file, which means it has additional name data
        # appended to it.  We remove that data here to test the actual file type.
        temp = os.path.split(path)[1]

        if not temp.find('Packages') >= 0:
            logger.error('Filename {0} from path {1} does not contain "Packages".  This may be an invalid'
                         'file - terminating processing.'.format(temp, path))
            return None

        # valid Package filenames should be Packages<.ext>, so the file parts that are unique and attached with _
        # should all come at the end of that.  We'll check all parts of the filename just to be sure, in case
        # the cachename generator gets changed but this method gets forgotten.
        if temp.find('_') >= 0:
            # Extract the substring corresponding from the start of Packages to the first _.
            logger.debug('Found _ in filename - attempting to retrieve just the basic Packages filename from cache.')
            start = temp.find('Packages')
            end = temp.find('_', start)
            filename = temp[start:end]
        else:
            logger.debug('No _ found in filename - could be standard Packages file with no blandishments.')
            filename = temp

        logger.debug('Filename {0} extracted from full path.'.format(filename))

        try:
            # handle the different compression types we might be asked to manage.
            logger.debug('Checking for the file type of file {0}'.format(filename))
            if filename.endswith('Packages'):
                stream = open(path, 'r')
            elif filename.endswith('.bz2'):
                stream = bz2.BZ2File(path, 'r')
            elif filename.endswith('.gz'):
                stream = gzip.GzipFile(path, 'r')
            else:
                # Unknown file extension.  Return with error.
                logger.error('The file features a file with an unknown extension.'.format(filename))
                raise IOError('Path {0} does not point to a bz2, gzip or uncompressed Packages file.'.format(path))
        except IOError:
            logger.error('The path {0} does not point to a valid file.  '
                         'Cannot read contents of Packages file.'.format(path))
            return None

        record = self.read_package_record(stream)

        while record != {}:
            pkg_name = record['Package']
            logger.debug('Package {0} has been read in from file {1}.'.format(pkg_name, path))
            retval[pkg_name] = record

            record = self.read_package_record(stream)

        # Close the read stream when done.
        stream.close()

        logger.debug('{0} package records were read in from file {1}.'.format(len(retval), path))

        return retval

    def apply_whitelist(self, pkg_dict, component, category):
        """
        Checks the packages stored within the package dictionary and removes them if they are not held in the
        whitelist, or if they are not a dependency of a whitelisted package and override_whitelist is enabled.
        :param pkg_dict: The dictionary containing the package records.  Should be in form of {pkg_name: {record}}
        :param component: The component that the pkg_dict belongs to.
        :param category: The category of the package dictionary.
        :return: Dictionary: a cleaned up copy of the pkg_dict dictionary if the whitelist can be applied
                 None: returned if the package list is malformed or there are problems with the whitelist that
                 prevent the whitelist cleanup from being applied.
        """

        logger.debug('Applying the whitelist to reduce the package count.  '
                     'Incoming package count: {0}'.format(len(pkg_dict)))

        logger.debug('Looking at whitelist for component {0}, category {1}'.format(component, category))
        logger.debug('Whitelisted packages: {0}'.format(self.whitelist[component]))

        if len(pkg_dict) > 0:
            logger.debug('Some entries from pkg_dict:')

        # Build up the list of dependencies of whitelisted package names, if needed.
        override_list = []
        if self.whitelist_override:
            for name in self.whitelist[component]:
                logger.debug('Looking at package {0} in the whitelist for component {1}.'.format(name, component))
                logger.debug('Category list associated with package: {0}'.format(self.whitelist[component][name]))

                # if the category is in the category list for component and name, then this package is whitelisted.
                # if further the package name is in the pkg_dict, then we need to get this package's dependencies.
                logger.debug('Category in self.whitelist[component][name]? '
                             '{0}'.format(category in self.whitelist[component][name]))
                logger.debug('Package name in pkg_dict? {0}'.format(name in pkg_dict))

                if category in self.whitelist[component][name] and name in pkg_dict:
                    logger.debug('Category {0} is in whitelist for package {1}.'.format(category, name))
                    # Add the dependencies to the override list.
                    dependencies = self.format_dependance_strings(pkg_dict[name])
                    # We need to also check the dependencies of the dependencies recursively...
                    while len(dependencies) > 0:
                        pkg_name = dependencies.pop(0)

                        try:
                            pkg = pkg_dict[pkg_name]
                        except KeyError:
                            logger.error('Dependency {0} of package {1} is not present in the package dictionary.'
                                         'We cannot add the dependency to the override list.'.format(pkg_name, name))
                            continue

                        # Make sure the dependent package is not already in the whitelist.
                        if pkg_name not in self.whitelist[component]:
                            logger.debug('Dependency {0} is not in the whitelist.  Adding to override, if not'
                                         'already present.'.format(pkg_name))
                            if pkg_name not in override_list:
                                logger.debug('Dependency {0} not in override list.'.format(pkg_name))
                                override_list.append(pkg_name)
                        else:
                            logger.debug('Dependency {0} has already been whitelisted.'.format(pkg_name))
                            continue

                        # Don't blanket extend dependencies; confirm that nothing we're adding already exists.
                        logger.debug("Expanding existing dependency list with {0}'s dependencies.".format(pkg_name))
                        temp = self.format_dependance_strings(pkg)
                        logger.debug("Retrieved dependencies are: {0}".format(temp))
                        for dependency in self.format_dependance_strings(pkg):
                            logger.debug('Checking if package {0} already exists in lists.'.format(dependency))
                            if dependency in override_list:
                                logger.debug('{0} exists in override list.'.format(dependency))
                                continue
                            if dependency in dependencies:
                                logger.debug('{0} exists in dependency list.'.format(dependency))
                                continue

                            # otherwise add it to the unchecked dependency list.
                            logger.debug('Dependency {0} not in dependency or override lists; '
                                         'adding here.'.format(dependency))
                            dependencies.append(dependency)

        logger.debug('{0} packages identified in the override_list.'.format(len(override_list)))
        logger.debug('Checking override and whitelist against {0} records in pkg_dict.'.format(len(pkg_dict)))
        # We've built the override_list.  Now to actually clear unwanted packages from pkg_dict.
        for pkg_name in pkg_dict.keys():
            # if the package name is in the whitelist under component and the category list contains the category
            # name, then we leave the pkg_dict entry alone.
            if pkg_name in self.whitelist[component] and category in self.whitelist[component][pkg_name]:
                logger.debug('{0} is whitelisted for this component and category.'.format(pkg_name))
                continue
            # Or if the package name has been added to the override list, leave it alone.
            elif pkg_name in override_list:
                logger.debug('{0} is in the override list - leaving in.'.format(pkg_name))
                continue

            # In the event that the package name is not in either the whitelist or the override list, we remove it.
            logger.debug('{0} is not in whitelist over override - ignoring.'.format(pkg_name))
            pkg_dict.pop(pkg_name)

        logger.debug('{0} packages left after trimming out non-whitelisted items.'.format(len(pkg_dict)))
        return pkg_dict

    def read_cached_pkg_index(self, component):
        """
        Read the Package file for each component and category (for binary, include all architectures).  For each
        package, check if it is in the whitelist and, if so, add it to a whitelisted packages list.  If it is not,
        add it to a blacklisted package list. If override_whitelist is enabled, each package in the depends and
        recommends fields needs to be examined and either added to the whitelist, or the package record pulled off the
        blacklist (and that record's depends/recommends values processed and added to the search list.)
        :param component: Component whose packages we're attempting to update.
        :return: Dictionary: A dictionary with one entry for each architecture, each of whose values is a dictionary of
        approved (whitelisted) packages for that architecture.
                None: If the method is unable to open or read the Packages file due to permissions or file corruption,
                None is returned.
        """

        category = ['binary-' + arch for arch in self.arch_list]

        retval = {}
        for item in category:
            approved_pkgs = {}
            blacklisted_pkgs = {}

            pkg_cachefile = self.generate_cache_filename('Packages', component, item)
            pkg_cachefile = os.path.join(self.cache_dir, pkg_cachefile)
            try:
                fd = open(pkg_cachefile, 'r')
            except IOError:
                logger.error('An error occurred while trying to open the cached Package file {0}.  Returning'
                             'unsuccessful attempt.'.format(pkg_cachefile))
                return None

            # pre-read in a record.
            record = self.read_package_record(fd)
            while len(record) > 0:
                # Check the record against the whitelist and confirm that it's whitelisted for binary categories.
                name = record['Package']
                if name in self.whitelist[component] and 'binary' in self.whitelist[component][name]:
                    approved_pkgs[name] = record
                    # Handle whitelist override.
                    if self.whitelist_override:
                        # Depends and Recommends are not required fields.  We want them combined into one string
                        # if they exist.
                        pkg_list = self.format_dependance_strings(record)
                        while len(pkg_list) > 0:
                            # If package is already in whitelist, we don't care.  We can ignore that condition.
                            if pkg_list[0] in self.whitelist[component] and \
                              'binary' in self.whitelist[component][pkg_list[0]]:
                                pkg_list.pop(0)
                            # if the package is in the blacklist, move it to whitelist and extend the pkg_list with
                            # its dependencies.
                            elif pkg_list[0] in blacklisted_pkgs:
                                pkg_record = blacklisted_pkgs.pop(pkg_list[0])
                                approved_pkgs[pkg_list[0]] = pkg_record
                                pkg_list.extend(self.format_dependance_strings(pkg_record))
                                pkg_list.pop(0)
                            # If the package not in the whitelist or blacklist, it hasn't been read yet and can simply
                            # be added to the whitelist.
                            else:
                                self.whitelist[component][pkg_list[0]] = ['binary']
                else:
                    blacklisted_pkgs[name] = record

                # Read the next record.
                record = self.read_package_record(fd)

            # Add the approved packages dictionary to the return value under the binary-<component> key.
            retval[item] = approved_pkgs

            # Close the package file.
            fd.close()

        return retval

    def format_dependance_strings(self, pkg_record):
        """
        Given a package record, extract the depends and recommends keys and turn the values into a properly
        formatted package list.
        :param pkg_record: The package record whose depends and recommends fields we want to use.
        :return: A list of package names that are present in the package record.
        """

        # The string that will store the temporare package lists.
        extra_pkgs = ''

        # Neither Depends nor Recommends are required fields in a Package record.
        if 'Depends' in pkg_record:
            extra_pkgs = pkg_record['Depends'].replace('|', ',') + ','
        if 'Recommends' in pkg_record:
            extra_pkgs += pkg_record['Recommends'].replace('|', ',')
        else:
            if extra_pkgs.endswith(','):
                extra_pkgs = extra_pkgs.rstrip(',')

        # Depends and Recommends can specify versions.  We don't care about them.
        start = extra_pkgs.find('(')
        while start >= 0:
            end = extra_pkgs.find(')', start)
            extra_pkgs = extra_pkgs[:start] + extra_pkgs[end + 1:]
            start = extra_pkgs.find('(')

        logger.debug('Completed dependency and recommends package string breakdown.')

        # and finally, split on comma's, removing extra spaces and returning the result.
        return [x.strip() for x in extra_pkgs.split(',') if x != '']

    def read_package_record(self, iostream):
        """
        Reads a single package record from a stream object.
        :param iostream: data stream object holding Package record set.
        :return: A dictionary object containing the key:value pairs comprising a Package record.
        """

        record = {}
        key = ''
        value = ''
        for line in iostream:
            if line == '\n':
                record[key] = value.strip()
                break
            else:
                if line.startswith(' '):
                    value += line.strip()
                else:
                    if key:
                        record[key] = value.strip()

                    pair = line.split(':', 1)
                    if len(pair) != 2:
                        logger.warn('Line in package file may have improper key:value pair. Line: {0}'.format(line))
                        value = ''
                    else:
                        value = pair[1].strip()
                    key = pair[0].strip()

        return record

    def parse_cached_release(self):
        """
        Reads in the cached Release file line by line, and stores the resulting data in the self.release_contents
        member.
        :return: True if the Release file could be opened and read; False if not.
        """

        cached_release = self.generate_cache_filename('Release')

        try:
            stream = open(os.path.join(self.cache_dir, cached_release), 'r')
        except IOError as err:
            logger.error('An error occurred while trying to open the cached Release file {0}, '
                         'directory {1}.'.format(cached_release, self.cache_dir))
            logger.error('Error # {0}, message: {1}'.format(err.errno, err.message))
            return False

        key = ''
        value = []
        # Read the file in line by line.
        for line in stream:
            # Check for multiline.
            if line.startswith(' '):
                logger.debug('Whitespace prefixed line - continuation of previous entry.')
                if (key.lower() == 'md5sum') or (key.lower() == 'sha1') or (key.lower() == 'sha256'):
                    logger.debug('Current key is a hash key - {0}.'.format(key))
                    if line.strip():
                        logger.debug('hash line has contents - appending split line to value.')
                        value.append(line.split())
                else:
                    logger.debug('Current key is not hash, but long line.  Adding to value.')
                    value.append(line.strip())
                continue
            else:
                # Split the line into key and value pair in preparation for storage in the dictionary.
                pair = line.split(':', 1)
                logger.debug("Line has been split at the ':', giving us {0} and {1}".format(pair[0], pair[1]))

                # Confirm that the line is not malformed.
                if len(pair) < 2 or len(pair) > 2:
                    # if it is, error out and move onto the next line.
                    logger.error("Malformed line in Release file - no leading whitespace, no ':' separator "
                                 "present: {0}".format(line))
                    continue

                # If the key value is not empty, then we have a key/value pair from the last iteration.  Store them.
                if key:
                    logger.debug("Key value is not empty - storing previous iteration's key {0} "
                                 "with value {1}".format(key, value))
                    self.release_contents[key] = value
                    # Clear value here.
                    value = []

                # Put the new key/value pair into holding, so that if this is a multiline value we will be able to
                # include all of it.
                logger.debug("New key/value being kept for next iteration.")
                key = pair[0]
                if pair[1].strip():
                    value.append(pair[1].strip())

        # Because we store completed key/value pairs only after starting a new one, it's possible (likely, even)
        # that the last kvp at the end of the file will not be stored.  We take care of that here.
        if key:
            logger.debug("Storing final key {0} with value {1}".format(key, value))
            self.release_contents[key] = value

        stream.close()

        return True

    def generate_cache_filename(self, index_file, component='', category=''):
        """
        When passed the name of one of the primary repository Index files, generates a standardized unique name for
        that particular file so it can be stored in cache without clobbering any other cached files.  The name is
        generated deterministically from repo, component and category information so that it can always be found again
        on successive runs of the program.
        :param index_file: Name of the file we want to generate a unique cache name for.
        :param component: The component that the index_file is intended to provide information for, if any.
        :param category: The category that the index_file is intended to provide informaiton for, if any.
        :return: The unique cached index filename.
        """

        # The repository directory itself is guaranteed to be unique to this repository.  We use the -1 index to ensure
        # that in edge cases (such as the repo_dir only having one directory component on it's path) we still get
        # the repo directory name instead of an error.
        unique_suffix = '_' + self.repo_dir.rsplit(os.sep, 1)[-1]

        # If the component value is present, then we append it to the unique suffix.
        if component != '':
            unique_suffix = unique_suffix + '_' + component

            # and if the category value is non-empty, then we append it to the unique suffix as well.
            if category != '':
                unique_suffix = unique_suffix + '_' + category

        # Our unique suffix is now '_repo-directory[_component-name[_category-name]].
        return index_file + unique_suffix


def download_file(uri, path, cache_ts=None):
    """
    Given a uri and path including file, download_file will attempt to download that file.  If cache_date is
    set to a datetime object, download_file will first attempt to confirm that the remote file is newer than
    the date, so that callers can make use of cached local copies of the file.
    :param uri: The url string to the file we are attempting to download.  Must contain at least the fqdn, may
    contain some of the remote host path to the file.
    :param path: The path on the remote webserver including the filename that we want to download.  The path
    should be relative to the end of the domain and path in the uri.
    :param cache_ts: A timestamp to compare the remote file time to.  If None, then the remote file will be
    downloaded regardless of its last update time; if present (must be a datetime object) then the modified date
    and time of the remote file will be compared against the datetime value, and only downloaded if it is newer.
    """
    global logger

    logger.debug("Attempting to download file %s from uri %s.", path, uri)

    # httplib wants the domain and the path to be completely separate, and not to include the http protocol portion.
    # Test the uri here, split it apart if need be, and then join any path bits to the path bit that was passed to us.
    # Check for the '://' that indicates the presence of the protocol specifier in a url.
    if uri.find('://') >= 0:
        logger.debug('uri contains protocol specifier - removing.')
        partial_split = uri.split('://')
        protocol = partial_split[0]
        uri = partial_split[1]

    # uri either had no protocol or has been overwritten such that no protocol component exists.  Check for path
    # separator.
    if uri.find('/') >= 0:
        logger.debug('uri contains partial path - splitting apart.')
        # split ONE time, at the first separator.
        partial_split = uri.split('/', 1)
        domain = partial_split[0]
        uri_path = partial_split[1]
    else:
        domain = uri
        uri_path = ''

    # uri_path may be '' or it may be non-empty string.  It will not start with a / in either case.  Join it to the
    # value of the path that was passed to us, if it is non-empty.
    if uri_path.endswith('/') and path.startswith('/'):
        full_path = uri_path[:-1] + path
    elif not uri_path.endswith('/') and not path.startswith('/'):
        full_path = uri_path + '/' + path
    else:
        full_path = uri_path + path

    if not full_path.startswith('/'):
        full_path = '/' + full_path

    logger.debug("Opening http connection to remote server.")
    if cache_ts is not None and isinstance(cache_ts, datetime.datetime):
        # create a separate connection here - httplib is supposedly finicky about
        # reusing connections.
        logger.debug("Getting state information about remote file.")
        conn = httplib.HTTPConnection(domain)
        conn.request("HEAD", full_path)
        reply = conn.getresponse()
        # httplib does not like when reply is not read.  We don't need it, though, since this is just a header check.
        dump = reply.read()
        status = reply.status
        # done with the connection here
        conn.close()

        if status != 200:
            logger.error("URL %s, path %s were not found by httplib.", domain, full_path)
            raise httplib.InvalidURL, domain + full_path + ' was not found.'

        datestring = reply.getheader('Last-Modified')
        url_ts = datetime.datetime.strptime(datestring, '%a, %d %b %Y %H:%M:%S %Z')

        # if the url's date and time is older or equal to the local cache, just
        # return None and perform no download.
        if url_ts <= cache_ts:
            logger.debug("provided timestamp is at least as new as the remote host, if not newer.")
            return None

    # if the cache_ts is not set, or else it is older than the url_ts, go ahead
    # and download.

    logger.debug("Downloading file...")
    conn = httplib.HTTPConnection(domain)
    conn.request("GET", full_path)
    reply = conn.getresponse()
    content_type = reply.getheader("Content-Type")
    if reply.status != 200:
        logger.debug("URL %s, path %s were not found by httplib.", domain, full_path)
        raise httplib.InvalidURL, domain + full_path + ' was not found.'

    file_data = reply.read()

    logger.debug("File downloaded, returning acquired data to caller.")
    return file_data


class NullHandler(logging.Handler):
    """
    Logging Handler class for initializing the logger for this plugin.  The actual pkg_manager framework will properly
    tie the logger for this plugin into the rest of the logging system.
    """

    def emit(self, record):
        pass
