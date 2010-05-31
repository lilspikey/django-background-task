from django.core.management.base import BaseCommand
import time
from optparse import make_option
import logging

from background_task.tasks import tasks, autodiscover

class Command(BaseCommand):
    LOG_LEVELS = ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']
    
    help = 'Run tasks that are scheduled to run on the queue'
    option_list = BaseCommand.option_list + (
            make_option('--duration',
                action='store',
                dest='duration',
                type='int',
                default=0,
                help='Run task for this many seconds (0 or less to run forever) - default is 0'),
            make_option('--sleep',
                action='store',
                dest='sleep',
                type='float',
                default=5.0,
                help='Sleep for this many seconds before checking for new tasks (if none were found) - default is 5'),
            make_option('--log-file',
                action='store',
                dest='log_file',
                help='Log file destination'),
            make_option('--log-level',
                action='store',
                type='choice',
                choices=LOG_LEVELS,
                dest='log_level',
                help='Set logging level (%s)' % ', '.join(LOG_LEVELS)),
            )
    
    def _configure_logging(self, log_level, log_file):
        if log_level:
            log_level = getattr(logging, log_level)
        
        config = {}
        if log_level:
            config['level'] = log_level
        if log_file:
            config['filename'] = log_file
        
        if config:
            logging.basicConfig(**config)
    
    def handle(self, *args, **options):
        log_level = options.pop('log_level', None)
        log_file = options.pop('log_file', None)
        duration = options.pop('duration', 0)
        sleep = options.pop('sleep', 5.0)
        
        self._configure_logging(log_level, log_file)
        
        autodiscover()
        
        start_time = time.time()
        
        while (duration <= 0) or (time.time() - start_time) <= duration:
            if not tasks.run_next_task():
                logging.debug('waiting for tasks')
                time.sleep(sleep)