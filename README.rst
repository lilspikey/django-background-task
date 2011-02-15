======================
Django Background Task
======================

Django Background Task is a databased-backed work queue for Django_, loosely based around Ruby's DelayedJob_ library.

In Django Background Task, all tasks are implemented as functions (or any other callable).

There are two parts to using background tasks:

* creating the task functions and registering them with the scheduler
* setup a cron task (or long running process) to execute the tasks

Creating and registering tasks
==============================

To register a task use the background decorator::

    from background_task import background
    from django.contrib.auth.models import User
    
    @background(schedule=60)
    def notify_user(user_id):
        # lookup user by id and send them a message
        user = User.objects.get(pk=user_id)
        user.email_user('Here is a notification', 'You have been notified')

This will convert the notify_user into a background task function.  When you call it from regular code it will actually create a Task object and stores it in the database.  The database then contains serialised information about which function actually needs running later on.  This does place limits on the parameters that can be passed when calling the function - they must all be serializable as JSON.  Hence why in the example above a user_id is passed rather than a User object.

Calling notify_user as normal will schedule the original function to be run 60 seconds from now::

    notify_user(user.id)

This is the default schedule time (as set in the decorator), but it can be overridden::

    notify_user(user.id, schedule=90) # 90 seconds from now
    notify_user(user.id, schedule=timedelta(minutes=20)) # 20 minutes from now
    notify_user(user.id, schedule=datetime.now()) # at a specific time

Running tasks
=============

There is a management command to run tasks that have been scheduled::

    python manage.py process_tasks

This will simply poll the database queue every few seconds to see if there is a new task to run.

NB: to aid the management task in finding the registered tasks it is best to put them in a file called 'tasks.py'.  You can put them elsewhere, but you have to ensure that they will be imported so the decorator can register them with the scheduler.  By putting them in tasks.py they will be auto-discovered and the file automatically imported by the management command.

.. _Django: http://www.djangoproject.com/
.. _DelayedJob: http://github.com/tobi/delayed_job
