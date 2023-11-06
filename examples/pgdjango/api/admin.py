from django.contrib import admin

from .models import PgcronnerJobs

@admin.register(PgcronnerJobs)
class PgCronnerJobsAdmin(admin.ModelAdmin):
    
    list_display = ('name', 'command', 'schedule')
    search_fields = ('name', 'command', 'schedule')
    list_filter = ('name', 'command', 'schedule')
    ordering = ('name', 'command', 'schedule')

