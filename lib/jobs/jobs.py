"""
Library file for jobs python packages.  Contains the general definition of the Jobs abstract class that all job objects
intended to be run by the scheduler must conform to (either by inheritance or duck typing.)  Any job object that is
intended to be loaded for use with the Scheduler must inherit from the Jobs class contained here.
"""

import abc
import datetime


class Job(object):
    """
    Job class defines a simple interface in which jobs are stored, and provide simple functionality like providing
    a datetime object marking when the job should next be run (for easy insertion back into the queue to reup the
    task) and time remaining from current time before this job should be run (so the queue manager can easily
    determine how long it needs to sleep for.)
    """

    def __init__(self):
        __metaclass__ = abc.ABCMeta
        """
        Sets up an empty job.
        :return:
        """

        # The type of timer - relative to current time, or absolute.
        self.timer_type = ''
        # The actual timer - a timedelta object indicating the time gap between runs of the job.
        self.timer = None
        # Timer base - with an absolute type, the base never changes.  With relative, the base can (but does not
        # necessarily) change to be the most recent end time of the job.  The base _can_ be used to calculate the
        # next run time by the job.
        self.timer_base = None
        # The datetime object representing when the job will next run.
        self.next_run_time = None

        # Error handling for each job, allowing any instantiator of a subclass to get the error state and a log of
        # job behavior without each job needing to set up its own logging.
        self.error_state = 0
        self.log_string = ""

    @abc.abstractmethod
    def run(self):
        """
        Job runner for class.  Abstract, must be instantiated by an actual job in order to handle the necessary
        logic checks for whether a job should go ahead or not.
        :return:
        """

    def update_next_run(self):
        """
        Updates the next run time object to the next absolute time the task should run again (note that this stores
        an absolute time regardless of whether the timer_type is relative or not, but the absolute time that is
        stored IS affected by the timer_type.
        :return:
        """

        if self.timer_type == 'absolute':
            self.next_run_time = self.timer_base + self.timer
            self.timer_base += self.timer
        else:
            self.next_run_time = datetime.datetime.now() + self.timer

    def get_next_run_time(self):
        """
        Returns a datetime object indicating the next time the job should be run, with respect to either it's
        absolute runtime or its relative run time.
        :return:
        """

        return self.next_run_time

    def get_next_run_delta(self):
        """
        Returns a timedelta object relative to current time indicating when the job should be run.
        :return:
        """

        # Take the next run time and subtract the current datetime.  This guarantees a negative timedelta if
        # the current time has surpassed the next_run_time.
        return self.next_run_time - datetime.datetime.now()
