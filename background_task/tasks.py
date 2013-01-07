from models import Task, datetime_now

import os
import logging
import sys
from datetime import datetime, timedelta
from django.db import transaction
from django.utils.importlib import import_module


class Tasks(object):
    def __init__(self):
        self._tasks = {}
        self._runner = DBTaskRunner()

    def background(self, name=None, schedule=None):
        '''
        decorator to turn a regular function into
        something that gets run asynchronously in
        the background, at a later time
        '''
        
        # see if used as simple decorator
        # where first arg is the function to be decorated
        fn = None
        if name and callable(name):
            fn = name
            name = None
        
        def _decorator(fn):
            _name = name
            if not _name:
                _name = '%s.%s' % (fn.__module__, fn.__name__)
            proxy = TaskProxy(_name, fn, schedule, self._runner)
            self._tasks[_name] = proxy
            return proxy
        
        if fn:
            return _decorator(fn)

        return _decorator

    def run_task(self, task_name, args, kwargs):
        task = self._tasks[task_name]
        task.task_function(*args, **kwargs)

    def run_next_task(self):
        return self._runner.run_next_task(self)


class TaskSchedule(object):
    SCHEDULE = 0
    RESCHEDULE_EXISTING = 1
    CHECK_EXISTING = 2

    def __init__(self, run_at=None, priority=None, action=None):
        self._run_at = run_at
        self._priority = priority
        self._action = action

    @classmethod
    def create(self, schedule):
        if isinstance(schedule, TaskSchedule):
            return schedule
        priority = None
        run_at = None
        action = None
        if schedule:
            if isinstance(schedule, (int, timedelta, datetime)):
                run_at = schedule
            else:
                run_at = schedule.get('run_at', None)
                priority = schedule.get('priority', None)
                action = schedule.get('action', None)
        return TaskSchedule(run_at=run_at, priority=priority, action=action)

    def merge(self, schedule):
        params = {}
        for name in ['run_at', 'priority', 'action']:
            attr_name = '_%s' % name
            value = getattr(self, attr_name, None)
            if value is None:
                params[name] = getattr(schedule, attr_name, None)
            else:
                params[name] = value
        return TaskSchedule(**params)

    @property
    def run_at(self):
        run_at = self._run_at or datetime_now()
        if isinstance(run_at, int):
            run_at = datetime_now() + timedelta(seconds=run_at)
        if isinstance(run_at, timedelta):
            run_at = datetime_now() + run_at
        return run_at

    @property
    def priority(self):
        return self._priority or 0

    @property
    def action(self):
        return self._action or TaskSchedule.SCHEDULE

    def __repr__(self):
        return 'TaskSchedule(run_at=%s, priority=%s)' % (self._run_at,
                                                         self._priority)

    def __eq__(self, other):
        return self._run_at == other._run_at \
           and self._priority == other._priority \
           and self._action == other._action


class DBTaskRunner(object):
    '''
    Encapsulate the model related logic in here, in case
    we want to support different queues in the future
    '''

    def __init__(self):
        self.worker_name = str(os.getpid())

    def schedule(self, task_name, args, kwargs, run_at=None,
                       priority=0, action=TaskSchedule.SCHEDULE):
        '''Simply create a task object in the database'''

        task = Task.objects.new_task(task_name, args, kwargs,
                                     run_at, priority)

        if action != TaskSchedule.SCHEDULE:
            task_hash = task.task_hash
            unlocked = Task.objects.unlocked(datetime_now())
            existing = unlocked.filter(task_hash=task_hash)
            if action == TaskSchedule.RESCHEDULE_EXISTING:
                updated = existing.update(run_at=run_at, priority=priority)
                if updated:
                    return
            elif action == TaskSchedule.CHECK_EXISTING:
                if existing.count():
                    return

        task.save()

    @transaction.autocommit
    def get_task_to_run(self):
        tasks = Task.objects.find_available()[:5]
        for task in tasks:
            # try to lock task
            locked_task = task.lock(self.worker_name)
            if locked_task:
                return locked_task
        return None

    @transaction.autocommit
    def run_task(self, tasks, task):
        try:
            logging.info('Running %s', task)
            args, kwargs = task.params()
            tasks.run_task(task.task_name, args, kwargs)
            # task done, so can delete it
            task.delete()
            logging.info('Ran task and deleting %s', task)
        except Exception:
            t, e, traceback = sys.exc_info()
            logging.warn('Rescheduling %s', task, exc_info=(t, e, traceback))
            task.reschedule(t, e, traceback)
            del traceback

    def run_next_task(self, tasks):
        # we need to commit to make sure
        # we can see new tasks as they arrive
        task = self.get_task_to_run()
        transaction.commit()
        if task:
            self.run_task(tasks, task)
            transaction.commit()
            return True
        else:
            return False


class TaskProxy(object):
    def __init__(self, name, task_function, schedule, runner):
        self.name = name
        self.task_function = task_function
        self.runner = runner
        self.schedule = TaskSchedule.create(schedule)

    def __call__(self, *args, **kwargs):
        schedule = kwargs.pop('schedule', None)
        schedule = TaskSchedule.create(schedule).merge(self.schedule)
        run_at = schedule.run_at
        priority = schedule.priority
        action = schedule.action
        self.runner.schedule(self.name, args, kwargs, run_at, priority, action)

    def __unicode__(self):
        return u'TaskProxy(%s)' % self.name

tasks = Tasks()


def autodiscover():
    '''autodiscover tasks.py files in much the same way as admin app'''
    import imp
    from django.conf import settings

    for app in settings.INSTALLED_APPS:
        try:
            app_path = import_module(app).__path__
        except AttributeError:
            continue
        try:
            imp.find_module('tasks', app_path)
        except ImportError:
            continue

        import_module("%s.tasks" % app)
