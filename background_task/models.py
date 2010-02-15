from django.db import models

# inspired by http://github.com/tobi/delayed_job

class Task(object):
    priority    = models.IntegerField(default=0, db_index=True)
    attempts    = models.IntegerField(default=0, db_index=True)
    task_name   = models.CharField(max_length=255, db_index=True)
    task_params = models.TextField()
    last_error  = models.TextField()
    run_at      = models.DateTimeField(db_index=True)
    locked_at   = models.DateTimeField(db_index=True)
    failed_at   = models.DateTimeField(db_index=True)
    locked_by   = models.CharField(max_length=64, db_index=True)
    
    def __unicode__(self):
        return u'Task(%s)' % self.task_name