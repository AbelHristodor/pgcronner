# from .models import PgcronnerJobs


# def validate_command(value: str) -> None:
#     if "CALL" in value:
#         if "();" not in value:
#             raise ValidationError(
#                 "CALL must be followed by a function call, e.g. CALL my_function();"
#             )


# def validate_schedule(value: str) -> None:
#     if not value:
#         raise ValidationError("Schedule cannot be empty")
#     if not croniter.is_valid(value):
#         raise ValidationError("Schedule is not valid")


# class PgCronnerJobsForm(forms.ModelForm):
#     command = forms.CharField(widget=forms.Textarea, validators=[validate_command])
#     schedule = forms.CharField(validators=[validate_schedule])

#     class Meta:
#         model = PgcronnerJobs
#         fields = "__all__"


# @admin.register(PgcronnerJobs)
# class PgCronnerJobsAdmin(admin.ModelAdmin):
#     form = PgCronnerJobsForm

#     list_display = ("name", "command", "schedule")
#     search_fields = ("name", "command", "schedule")
#     list_filter = ("name", "command", "schedule")
#     ordering = ("name", "command", "schedule")

#     def save_model(self, request, obj: PgcronnerJobs, form: forms.Form, change):
#         if pgcron := getattr(settings, "PGCRON", None):
#             if form.changed_data and change:
#                 pgcron.remove(form.initial["name"])

#             job = Job(obj.name, obj.schedule, obj.command, obj.source)
#             pgcron.add(job)
#             pgcron.sync()
