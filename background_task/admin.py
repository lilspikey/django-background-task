from django.contrib import admin

from models import Task

class TaskAdmin(admin.ModelAdmin):
    display_filter = ['task_name']
    list_display = ['task_name', 'task_params', 'run_at', 'priority', 'attempts']

admin.site.register(Task, TaskAdmin)