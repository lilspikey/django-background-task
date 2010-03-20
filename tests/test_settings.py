import sys

DEBUG = True
TEMPLATE_DEBUG = DEBUG
DATABASE_ENGINE = 'sqlite3'
DATABASE_NAME = ':memory:'
INSTALLED_APPS = [ 'background_task' ]


# enable this for coverage (using django coverage)
# http://pypi.python.org/pypi/django-coverage/1.0.1
RUN_COVERAGE = 'test_coverage' in sys.argv

if RUN_COVERAGE:
    INSTALLED_APPS.append('django_coverage')
    COVERAGE_REPORT_HTML_OUTPUT_DIR = 'html_coverage'
