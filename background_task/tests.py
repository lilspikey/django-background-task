import unittest
from django.test import TransactionTestCase

from background_task.tasks import tasks
from background_task.models import Task

def empty_task():
    pass

_recorded = []
def record_task(*arg, **kw):
    _recorded.append((arg, kw))

class TestBackgroundDecorator(unittest.TestCase):
    
    def test_get_proxy(self):
        proxy = tasks.background()(empty_task)
        self.failIfEqual(proxy, empty_task)
    
    def test_default_name(self):
        proxy = tasks.background()(empty_task)
        self.failUnlessEqual(proxy.name, 'background_task.tests.empty_task')
        
        proxy = tasks.background()(record_task)
        self.failUnlessEqual(proxy.name, 'background_task.tests.record_task')
    
    def test_specified_name(self):
        proxy = tasks.background(name='mytask')(empty_task)
        self.failUnlessEqual(proxy.name, 'mytask')
    
    def test_task_function(self):
        proxy = tasks.background()(empty_task)
        self.failUnlessEqual(proxy.task_function, empty_task)
        
        proxy = tasks.background()(record_task)
        self.failUnlessEqual(proxy.task_function, record_task)
    
    def test__unicode__(self):
        proxy = tasks.background()(empty_task)
        self.failUnlessEqual(u'TaskProxy(background_task.tests.empty_task)', unicode(proxy))

class TestRunTask(unittest.TestCase):
    
    def test_run_task(self):
        proxy = tasks.background()(record_task)
        
        tasks.run_task(proxy.name, [], {})
        self.failUnlessEqual(((), {}), _recorded.pop())
        
        tasks.run_task(proxy.name, ['hi'], {})
        self.failUnlessEqual((('hi',), {}), _recorded.pop())
        
        tasks.run_task(proxy.name, [], {'kw': 1})
        self.failUnlessEqual(((), {'kw': 1}), _recorded.pop())

class TestSchedulingTasks(TransactionTestCase):
    
    def test_background_gets_scheduled(self):
        self.result = None
        @tasks.background(name='test_background_gets_scheduled')
        def set_result(result):
            self.result = result
        
        # calling set_result should now actually create a record in the db
        set_result(1)
        
        all_tasks = Task.objects.all()
        self.failUnlessEqual(1, all_tasks.count())
        task = all_tasks[0]
        self.failUnlessEqual('test_background_gets_scheduled', task.task_name)
        self.failUnlessEqual('[[1], {}]', task.task_params)

class TestTaskRunner(TransactionTestCase):
    
    def setUp(self):
        super(TestTaskRunner, self).setUp()
        self.runner = tasks._runner
    
    def test_get_task_to_run_no_tasks(self):
        self.failIf(self.runner.get_task_to_run())
    
    def test_get_task_to_run(self):
        task = Task.objects.create_task('mytask', (1), {})
        self.failUnless(task.locked_by is None)
        self.failUnless(task.locked_at is None)
        
        locked_task = self.runner.get_task_to_run()
        self.failIf(locked_task is None)
        self.failIf(locked_task.locked_by is None)
        self.failUnlessEqual(self.runner.worker_name, locked_task.locked_by)
        self.failIf(locked_task.locked_at is None)
        self.failUnlessEqual('mytask', locked_task.task_name)

class TestTaskModel(TransactionTestCase):
    
    def test_lock_uncontested(self):
        task = Task.objects.create_task('mytask')
        self.failUnless(task.locked_by is None)
        self.failUnless(task.locked_at is None)
        
        locked_task = task.lock('mylock')
        self.failUnlessEqual('mylock', locked_task.locked_by)
        self.failIf(locked_task.locked_at is None)
        self.failUnlessEqual(task.pk, locked_task.pk)
    
    def test_lock_contested(self):
        # locking should actually look at db, not object
        # in memory
        task = Task.objects.create_task('mytask')
        self.failIf(task.lock('mylock') is None)
        
        self.failUnless(task.lock('otherlock') is None)
    
    def test__unicode__(self):
        task = Task.objects.create_task('mytask')
        self.failUnlessEqual(u'Task(mytask)', unicode(task))

class TestTasks(TransactionTestCase):
    
    def setUp(self):
        super(TestTasks, self).setUp()
        
        @tasks.background(name='set_fields')
        def set_fields(**fields):
            for key, value in fields.items():
                setattr(self, key, value)
        
        @tasks.background(name='throws_error')
        def throws_error():
            raise RuntimeError("an error")
        
        self.set_fields = set_fields
        self.throws_error = throws_error
    
    def test_run_next_task_nothing_scheduled(self):
        self.failIf(tasks.run_next_task())
    
    def test_run_next_task_one_task_scheduled(self):
        self.set_fields(worked=True)
        self.failIf(hasattr(self, 'worked'))
        
        self.failUnless(tasks.run_next_task())
        
        self.failUnless(hasattr(self, 'worked'))
        self.failUnless(self.worked)
    
    def test_run_next_task_several_tasks_scheduled(self):
        self.set_fields(one='1')
        self.set_fields(two='2')
        self.set_fields(three='3')
        
        for i in range(3):
            self.failUnless(tasks.run_next_task())
        
        self.failIf(tasks.run_next_task()) # everything should have been run
        
        for field, value in [('one', '1'), ('two', '2'), ('three', '3')]:
            self.failUnless(hasattr(self, field))
            self.failUnlessEqual(value, getattr(self, field))
    
    def test_run_next_task_error_handling(self):
        self.throws_error()
        
        all_tasks = Task.objects.all()
        self.failUnlessEqual(1, all_tasks.count())
        original_task = all_tasks[0]
        
        # should run, but trigger error
        self.failUnless(tasks.run_next_task())
        
        all_tasks = Task.objects.all()
        self.failUnlessEqual(1, all_tasks.count())
        
        failed_task = all_tasks[0]
        # should have an error recorded
        self.failIfEqual('', failed_task.last_error)
        self.failIf(failed_task.failed_at is None)
        self.failUnlessEqual(1, failed_task.attempts)
        
        # should have been rescheduled for the future
        # and no longer locked
        self.failUnless(failed_task.run_at > original_task.run_at)
        self.failUnless(failed_task.locked_by is None)
        self.failUnless(failed_task.locked_at is None)
        
        