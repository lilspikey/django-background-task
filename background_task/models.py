from django.db import models
from django.db.models import Q

# inspired by http://github.com/tobi/delayed_job

from django.utils import simplejson
from datetime import datetime, timedelta
from hashlib import sha1

class TaskManager(models.Manager):
    
    def find_available(self):
        qs = self.get_query_set()
        
        now = datetime.now()
        return qs.filter(run_at__lte=now, locked_by=None).order_by('-priority', 'run_at')
    
    def create_task(self, task_name, args=None, kwargs=None, run_at=None, priority=0):
        args   = args or ()
        kwargs = kwargs or {}
        if run_at is None:
            run_at = datetime.now()
        
        task_params = simplejson.dumps((args, kwargs))
        task_hash   = sha1(task_name + task_params).hexdigest()
        
        return self.create(task_name=task_name, \
                           task_params=task_params, \
                           task_hash=task_hash, \
                           priority=priority, \
                           run_at=run_at)

class Task(models.Model):    
    # the "name" of the task/function to be run
    task_name   = models.CharField(max_length=255, db_index=True)
    # the json encoded parameters to pass to the task
    task_params = models.TextField()
    # a sha1 hash of the name and params, to lookup already scheduled tasks
    task_hash   = models.CharField(max_length=40, db_index=True)
    
    # what priority the task has
    priority    = models.IntegerField(default=0, db_index=True)
    # when the task should be run
    run_at      = models.DateTimeField(db_index=True)
    
    # how many times the task has been tried
    attempts    = models.IntegerField(default=0, db_index=True)
    # when the task last failed
    failed_at   = models.DateTimeField(db_index=True, null=True, blank=True)
    # details of the error that occurred
    last_error  = models.TextField(blank=True)
    
    # details of who's trying to run the task at the moment
    locked_by   = models.CharField(max_length=64, db_index=True, null=True, blank=True)
    locked_at   = models.DateTimeField(db_index=True, null=True, blank=True)
    
    objects = TaskManager()
    
    def params(self):
        args, kwargs = simplejson.loads(self.task_params)
        # need to coerce kwargs keys to str
        kwargs = dict((str(k), v) for k, v in kwargs.items())
        return args, kwargs
    
    def lock(self, locked_by):
        unlocked = Task.objects.filter(pk=self.pk, locked_by=None)
        updated  = unlocked.update(locked_by=locked_by, locked_at=datetime.now())
        if updated:
            return Task.objects.get(pk=self.pk)
        return None
    
    def __unicode__(self):
        return u'Task(%s)' % self.task_name