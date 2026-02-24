#!/usr/bin/env python
import os
import sys
from pathlib import Path

def load_env_file(path: Path) -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        if not path.exists():
            return
        for line in path.read_text().splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith('#') or '=' not in stripped:
                continue
            key, value = stripped.split('=', 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
        return

    load_dotenv(path)


def main():
    base_dir = Path(__file__).resolve().parent
    load_env_file(base_dir / '.env')

    if len(sys.argv) >= 2 and sys.argv[1] == 'runserver':
        has_addrport = any(not arg.startswith('-') for arg in sys.argv[2:])
        if not has_addrport:
            host = os.getenv('HOST') or os.getenv('DJANGO_HOST') or '127.0.0.1'
            port = os.getenv('PORT') or os.getenv('DJANGO_PORT') or '8000'
            sys.argv.append(f'{host}:{port}')

    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'fefe.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and available on your PYTHONPATH environment variable?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
