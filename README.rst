======================
Django Background Task
======================

Django Background Task is a databased-backed work queue for Django_, loosely based around Ruby's DelayedJob_ library.

In Django Background Task, all tasks are implemented as functions (or any other callable).

There are two parts to using background tasks:

* creating the task functions and registering them with the scheduler
* setup a cron task (or long running process) to execute the tasks

.. _Django: http://www.djangoproject.com/
.. _DelayedJob: http://github.com/tobi/delayed_job
