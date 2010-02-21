import unittest
from django.test import TransactionTestCase
from django.conf import settings

from datetime import timedelta, datetime

from background_task.tasks import tasks, TaskSchedule
from background_task.models import Task

_recorded = []


def empty_task():
    pass


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
        self.failUnlessEqual(u'TaskProxy(background_task.tests.empty_task)',
                             unicode(proxy))


class TestTaskProxy(unittest.TestCase):

    def setUp(self):
        super(TestTaskProxy, self).setUp()
        self.proxy = tasks.background()(record_task)

    def test_run_task(self):
        tasks.run_task(self.proxy.name, [], {})
        self.failUnlessEqual(((), {}), _recorded.pop())

        tasks.run_task(self.proxy.name, ['hi'], {})
        self.failUnlessEqual((('hi',), {}), _recorded.pop())

        tasks.run_task(self.proxy.name, [], {'kw': 1})
        self.failUnlessEqual(((), {'kw': 1}), _recorded.pop())


class TestTaskSchedule(unittest.TestCase):

    def test_priority(self):
        self.failUnlessEqual(0, TaskSchedule().priority)
        self.failUnlessEqual(0, TaskSchedule(priority=0).priority)
        self.failUnlessEqual(1, TaskSchedule(priority=1).priority)
        self.failUnlessEqual(2, TaskSchedule(priority=2).priority)

    def _within_one_second(self, d1, d2):
        self.failUnless(isinstance(d1, datetime))
        self.failUnless(isinstance(d2, datetime))
        self.failUnless(abs(d1 - d2) <= timedelta(seconds=1))

    def test_run_at(self):
        for schedule in [None, 0, timedelta(seconds=0)]:
            now = datetime.now()
            run_at = TaskSchedule(run_at=schedule).run_at
            self._within_one_second(run_at, now)

        now = datetime.now()
        run_at = TaskSchedule(run_at=now).run_at
        self._within_one_second(run_at, now)

        fixed_dt = datetime.now() + timedelta(seconds=60)
        run_at = TaskSchedule(run_at=fixed_dt).run_at
        self._within_one_second(run_at, fixed_dt)

        run_at = TaskSchedule(run_at=90).run_at
        self._within_one_second(run_at, datetime.now() + timedelta(seconds=90))

        run_at = TaskSchedule(run_at=timedelta(seconds=35)).run_at
        self._within_one_second(run_at, datetime.now() + timedelta(seconds=35))

    def test_create(self):
        fixed_dt = datetime.now() + timedelta(seconds=10)
        schedule = TaskSchedule.create({'run_at': fixed_dt})
        self.failUnlessEqual(schedule.run_at, fixed_dt)
        self.failUnlessEqual(0, schedule.priority)

        schedule = TaskSchedule.create({'run_at': fixed_dt, 'priority': 2})
        self.failUnlessEqual(schedule.run_at, fixed_dt)
        self.failUnlessEqual(2, schedule.priority)

        schedule = TaskSchedule.create(0)
        self._within_one_second(schedule.run_at, datetime.now())

        schedule = TaskSchedule.create(10)
        self._within_one_second(schedule.run_at,
                                datetime.now() + timedelta(seconds=10))

        schedule = TaskSchedule.create(TaskSchedule(run_at=fixed_dt))
        self.failUnlessEqual(schedule.run_at, fixed_dt)
        self.failUnlessEqual(0, schedule.priority)

    def test_merge(self):
        default = TaskSchedule(run_at=10, priority=2)
        schedule = TaskSchedule.create(20).merge(default)

        self._within_one_second(datetime.now() + timedelta(seconds=20),
                                schedule.run_at)
        self.failUnlessEqual(2, schedule.priority)
        
        schedule = TaskSchedule.create({'priority': 0}).merge(default)
        self._within_one_second(datetime.now() + timedelta(seconds=10),
                                schedule.run_at)
        self.failUnlessEqual(0, schedule.priority)

    def test_repr(self):
        self.failUnlessEqual('TaskSchedule(run_at=10, priority=0)',
                             repr(TaskSchedule(run_at=10, priority=0)))


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

    def test_lock_expired(self):
        settings.MAX_RUN_TIME = 60
        task = Task.objects.create_task('mytask')
        locked_task = task.lock('mylock')

        # force expire the lock
        expire_by = timedelta(seconds=(settings.MAX_RUN_TIME + 2))
        locked_task.locked_at = locked_task.locked_at - expire_by
        locked_task.save()

        # now try to get the lock again
        self.failIf(task.lock('otherlock') is None)

    def test__unicode__(self):
        task = Task.objects.create_task('mytask')
        self.failUnlessEqual(u'Task(mytask)', unicode(task))


class TestTasks(TransactionTestCase):

    def setUp(self):
        super(TestTasks, self).setUp()

        settings.MAX_RUN_TIME = 60

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

        self.failIf(tasks.run_next_task())  # everything should have been run

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

    def test_run_next_task_does_not_run_locked(self):
        self.set_fields(locked=True)
        self.failIf(hasattr(self, 'locked'))

        all_tasks = Task.objects.all()
        self.failUnlessEqual(1, all_tasks.count())
        original_task = all_tasks[0]
        original_task.lock('lockname')

        self.failIf(tasks.run_next_task())

        self.failIf(hasattr(self, 'locked'))
        all_tasks = Task.objects.all()
        self.failUnlessEqual(1, all_tasks.count())

    def test_run_next_task_unlocks_after_MAX_RUN_TIME(self):
        self.set_fields(lock_overridden=True)

        all_tasks = Task.objects.all()
        self.failUnlessEqual(1, all_tasks.count())
        original_task = all_tasks[0]
        locked_task = original_task.lock('lockname')

        self.failIf(tasks.run_next_task())

        self.failIf(hasattr(self, 'lock_overridden'))

        # put lot time into past
        expire_by = timedelta(seconds=(settings.MAX_RUN_TIME + 2))
        locked_task.locked_at = locked_task.locked_at - expire_by
        locked_task.save()

        # so now we should be able to override the lock
        # and run the task
        self.failUnless(tasks.run_next_task())
        self.failUnlessEqual(0, Task.objects.count())

        self.failUnless(hasattr(self, 'lock_overridden'))
        self.failUnless(self.lock_overridden)

    def test_default_schedule_used_for_run_at(self):

        @tasks.background(name='default_schedule_used_for_run_at', schedule=60)
        def default_schedule_used_for_time():
            pass

        now = datetime.now()
        default_schedule_used_for_time()

        all_tasks = Task.objects.all()
        self.failUnlessEqual(1, all_tasks.count())
        task = all_tasks[0]

        self.failUnless(now < task.run_at)
        self.failUnless((task.run_at - now) <= timedelta(seconds=61))
        self.failUnless((task.run_at - now) >= timedelta(seconds=59))

    def test_default_schedule_used_for_priority(self):

        @tasks.background(name='default_schedule_used_for_priority',
                          schedule={'priority': 2})
        def default_schedule_used_for_priority():
            pass

        now = datetime.now()
        default_schedule_used_for_priority()

        all_tasks = Task.objects.all()
        self.failUnlessEqual(1, all_tasks.count())
        task = all_tasks[0]
        self.failUnlessEqual(2, task.priority)

    def test_non_default_schedule_used(self):
        default_run_at = datetime.now() + timedelta(seconds=90)

        @tasks.background(name='non_default_schedule_used',
                          schedule={'run_at': default_run_at, 'priority': 2})
        def default_schedule_used_for_priority():
            pass

        run_at = datetime.now() + timedelta(seconds=60)
        default_schedule_used_for_priority(schedule=run_at)

        all_tasks = Task.objects.all()
        self.failUnlessEqual(1, all_tasks.count())
        task = all_tasks[0]
        self.failUnlessEqual(run_at, task.run_at)
