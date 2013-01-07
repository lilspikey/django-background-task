import sys

DEBUG = True
TEMPLATE_DEBUG = DEBUG
DATABASE_ENGINE = 'sqlite3'
DATABASE_NAME = ':memory:'

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.%s' % DATABASE_ENGINE,
        'NAME': DATABASE_NAME,
        'USER': '',                      # Not used with sqlite3.
        'PASSWORD': '',                  # Not used with sqlite3.
    }
}


INSTALLED_APPS = [ 'background_task' ]


if 'test_coverage' in sys.argv:
    # http://pypi.python.org/pypi/django-coverage
    INSTALLED_APPS.append('django_coverage')
    COVERAGE_REPORT_HTML_OUTPUT_DIR = 'html_coverage'
    COVERAGE_MODULE_EXCLUDES = []

