
DEBUG = True
TEMPLATE_DEBUG = DEBUG
DATABASE_ENGINE = 'sqlite3'
DATABASE_NAME = ':memory:'
INSTALLED_APPS = ( 'background_task', )


# enable this for coverage (using django test coverage 
# http://pypi.python.org/pypi/django-test-coverage )
TEST_RUNNER = 'django-test-coverage.runner.run_tests'
COVERAGE_MODULES = ('background_task', 'background_task.tasks', 'background_task.models')