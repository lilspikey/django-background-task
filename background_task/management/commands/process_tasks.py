from django.core.management.base import BaseCommand
import time

from background_task.tasks import tasks

class Command(BaseCommand):
    help = 'Run tasks that are scheduled to run on the queue'
    
    def handle(self, *args, **options):
        while True:
            if not tasks.run_next_task():
                time.sleep(5.0)