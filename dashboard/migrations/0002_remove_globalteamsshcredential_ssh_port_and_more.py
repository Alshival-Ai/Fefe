from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('dashboard', '0001_initial'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='globalteamsshcredential',
            name='ssh_port',
        ),
        migrations.RemoveField(
            model_name='globalteamsshcredential',
            name='ssh_username',
        ),
    ]
