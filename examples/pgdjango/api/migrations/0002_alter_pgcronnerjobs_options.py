# Generated by Django 3.2.23 on 2023-11-06 09:06

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('cronjobs', '0001_initial'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='pgcronnerjobs',
            options={'verbose_name': 'PgCronner Job', 'verbose_name_plural': 'PgCronner Jobs'},
        ),
    ]

