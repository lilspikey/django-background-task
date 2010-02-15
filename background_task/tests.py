import unittest

from background_task.tasks import tasks

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