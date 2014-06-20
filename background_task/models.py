from django.db import models
from django.db.models import Q
from django.conf import settings

from datetime import timedelta
from hashlib import sha1
import traceback
from StringIO import StringIO
import logging

try:
    import json  # Django >= 1.6
except ImportError:
    from django.utils import simplejson as json  # Django <= 1.5

try:
    from django.utils import timezone
    datetime_now = timezone.now
except ImportError:
    from datetime import datetime
    datetime_now = datetime.now


# inspired by http://github.com/tobi/delayed_job


class TaskManager(models.Manager):

    def find_available(self):
        now = datetime_now()
        qs = self.unlocked(now)
        ready = qs.filter(run_at__lte=now, failed_at=None)
        return ready.order_by('-priority', 'run_at')

    def unlocked(self, now):
        max_run_time = getattr(settings, 'MAX_RUN_TIME', 3600)
        qs = self.get_query_set()
        expires_at = now - timedelta(seconds=max_run_time)
        unlocked = Q(locked_by=None) | Q(locked_at__lt=expires_at)
        return qs.filter(unlocked)

    def new_task(self, task_name, args=None, kwargs=None,
                 run_at=None, priority=0):
        args = args or ()
        kwargs = kwargs or {}
        if run_at is None:
            run_at = datetime_now()

        task_params = json.dumps((args, kwargs))
        task_hash = sha1(task_name + task_params).hexdigest()

        return Task(task_name=task_name,
                    task_params=task_params,
                    task_hash=task_hash,
                    priority=priority,
                    run_at=run_at)


class Task(models.Model):
    # the "name" of the task/function to be run
    task_name = models.CharField(max_length=255, db_index=True)
    # the json encoded parameters to pass to the task
    task_params = models.TextField()
    # a sha1 hash of the name and params, to lookup already scheduled tasks
    task_hash = models.CharField(max_length=40, db_index=True)

    # what priority the task has
    priority = models.IntegerField(default=0, db_index=True)
    # when the task should be run
    run_at = models.DateTimeField(db_index=True)

    # how many times the task has been tried
    attempts = models.IntegerField(default=0, db_index=True)
    # when the task last failed
    failed_at = models.DateTimeField(db_index=True, null=True, blank=True)
    # details of the error that occurred
    last_error = models.TextField(blank=True)

    # details of who's trying to run the task at the moment
    locked_by = models.CharField(max_length=64, db_index=True,
                                 null=True, blank=True)
    locked_at = models.DateTimeField(db_index=True, null=True, blank=True)

    objects = TaskManager()

    def params(self):
        args, kwargs = json.loads(self.task_params)
        # need to coerce kwargs keys to str
        kwargs = dict((str(k), v) for k, v in kwargs.items())
        return args, kwargs

    def lock(self, locked_by):
        now = datetime_now()
        unlocked = Task.objects.unlocked(now).filter(pk=self.pk)
        updated = unlocked.update(locked_by=locked_by, locked_at=now)
        if updated:
            return Task.objects.get(pk=self.pk)
        return None
    
    def _extract_error(self, type, err, tb):
        file = StringIO()
        traceback.print_exception(type, err, tb, None, file)
        return file.getvalue()
    
    def reschedule(self, type, err, traceback):
        self.last_error = self._extract_error(type, err, traceback)
        max_attempts = getattr(settings, 'MAX_ATTEMPTS', 25)

        if self.attempts >= max_attempts:
            self.failed_at = datetime_now()
            logging.warn('Marking task %s as failed', self)
        else:
            self.attempts += 1
            backoff = timedelta(seconds=(self.attempts ** 4) + 5)
            self.run_at = datetime_now() + backoff
            logging.warn('Rescheduling task %s for %s later at %s', self,
                backoff, self.run_at)

        # and unlock
        self.locked_by = None
        self.locked_at = None

        self.save()
    
    def save(self, *arg, **kw):
        # force NULL rather than empty string
        self.locked_by = self.locked_by or None
        return super(Task, self).save(*arg, **kw)

    def __unicode__(self):
        return u'Task(%s)' % self.task_name

    class Meta:
        db_table = 'background_task'
