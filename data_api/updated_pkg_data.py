"""
Class provides an interface for plugins to return data about which packages have been modified to the framework.
The framework will use the data to update a database with the details and status' of the packages.
"""

import datetime

# The date_fmt string is provided for plugins and frameworks to standardize their date handling; plugins can
# use the date format string to extract current date and time strings in the format the framework handles, for
# instance.
date_fmt = '%d-%m-%Y %H:%M:%S %z'


class UpdatedPackageData:
    """
    The purpose of this class has more or less been described above.  The class does store all updated
    packages - only one instance of the class needs to be returned for an entire repository's update, rather
    than a list of UpdatedPackageData objects.

    Each package is stored as a simple list object, format:
    Index     Information
    0         package name
    1         package type (debian, rpm, etc)
    2         contents (source, translation, binary (if binary, architecture type is required.)
    3         current package version
    4         date at which modification event happened
    5         type of modification event (add, delete, update)

    The package data itself is stored in a list object in the class, as specific package indexing is not expected
    to be needed - the repo plugins are expected to correctly load the data for each package one time, and the
    framework is expected to read through all returned package modification updates, not pick individuals from the mix.
    """

    def __init__(self):
        """
        Simple constructor.
        """

        # All packages are stored in an internal list object.  The data format is a simple list because it is expected
        # that plugins need only write to it once with correct values and not perform searches to fix, and
        # that the framework need only iterate over the entire list, not cherry pick specific updates over others.
        self._packages = []

    def add_pkg_modification(self, name, pkg_type, contents, version, date, event):
        """
        Adds a new entry to the updated _packages list.
        :param name: Name of the modified package.
        :param pkg_type: Type of the package - debian, rpm, snap, docker, etc
        :param contents: whether the package is a binary, source, or some other form.  If binary, the architecture
        should appended to the contents string following a dash (e.g binary-arm, binary-ppc, binary-amd64)
        :param version: The package version string.
        :param date: The date and time at which the package was updated.  This should either be a datetime object, or
        a string that is compatible with the datetime format.  String must be in format defined by the format string
        updated_pkg_data.date_fmt (dd-mm-yyyy hh:mm:ss), where hour is 24 hour clock (not 12.)
        :param event: The type of modification event - update, delete, modify.  Must be one of those three strings.
        :return: Return value is an or'd number, depending on whether:
                 0 - data fields are formatted correctly
                 1 - event is formatted incorrectly
                 2 - date is formatted incorrectly
                 4 - contents is formatted incorrectly
                 If return value is non-zero, then the record was not stored in the updated package list.
        """
        pkg_data = []
        retval = 0
        global date_fmt

        # If the event object does not match update, delete or modify - fail.
        if event != 'update' and event != 'delete' and event != 'modify':
            retval |= 1

        # If the date object is something other than a string or a datetime object (have not tested string format yet)
        # then fail.
        if type(date) is not str and type(date) is not datetime.datetime:
            retval |= 2

        # If the contents string is for a binary object but does not include a '-' after 'binary' to indicate the
        # binary architecture type, fail.
        if contents.startswith('binary') and not contents.startswith('binary-'):
            retval |= 4

        # Try to create the datetime object.
        try:
            if type(date) is str:
                datestamp = datetime.datetime.strptime(date, date_fmt)
            else:
                datestamp = date
        except ValueError:
            # ValueError is thrown if the datetime string is in an invalid format.
            retval |= 2

        # If the return value is non-zero, a data format error occurred and we cannot use/add the record.
        if retval != 0:
            return retval

        pkg_data.append(name)
        pkg_data.append(pkg_type)
        pkg_data.append(contents)
        pkg_data.append(version)
        pkg_data.append(datestamp)
        pkg_data.append(event)

        self._packages.append(pkg_data)

        return retval

    def get_list(self):
        """
        Technically self._packages is not a private variable, and it could be accessed easily by the framework.
        However, we'll preserve the illusion that it is private and should be accessed only indirectly, in case
        the internal storage format of self._packages changes and it needs to be translated into a list before
        being returned to the caller.
        :return: The list of packages for which updates have been recorded into this class.
        """

        return self._packages
