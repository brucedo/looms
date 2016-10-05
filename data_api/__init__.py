"""
The api package provides a programming interface between the plugins and the update framework.  Classes and code to
pass data from the plugins to the framework will be placed here, along with any classes or code that is intended
to pass data from the framework to the plugins.  Note that the framework will always operate on the assumption of a
standardized set of methods to call from within a class; for time and speed's sake we're not building a virtualized
superclass but instead relying on duck typing.  In the future, however, should a virtualized superclass be built
from which all repository plugins are expected to inherit, it will be placed here.
"""