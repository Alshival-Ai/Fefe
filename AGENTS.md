# Fefe App Technical Details

## Stack Overview
- Backend: Django (project `fefe`, app `dashboard`)
- Auth: django-allauth (login at `/accounts/login/`)
- Database: SQLite (default Django DB: `db.sqlite3`)
- User data storage: per-user SQLite database (`member.db`) under `user_data/<slug-username>-<id>/`
- Frontend (optional): Vite + React embedded into Django templates

## Backend Details
- Project settings: `fefe/settings.py`
- URLs: `fefe/urls.py`
- App: `dashboard/`
- Login required globally via `fefe/middleware.py`
- User folder creation on login via `dashboard/signals.py`
- Watchlist storage: `dashboard/watchlist.py`

## Frontend Details
- Base templates: `dashboard/templates/base.html` and `dashboard/templates/vertical.html`
- Sidebar/topbar partials in `dashboard/templates/partials/`
- Matrix background effect scoped to sidebar
- Vite React app (optional) in `frontend/`
  - Vite dev server: `http://localhost:5173`
  - Build outputs to `dashboard/static/frontend/`
  - Django template tags for Vite: `dashboard/templatetags/vite.py`

## Running
- Django: `python manage.py runserver`
- React (dev): `cd frontend && npm install && npm run dev`
- React (build): `cd frontend && npm run build`

## Key Routes
- `/` Overview
- `/resources/` Resource monitor and watchlist
- `/resources/watchlist/add/` POST add to watchlist
- `/accounts/login/` Login (Allauth)

## Notes
- `USER_DATA_ROOT` in settings controls where per-user data lives (default `user_data/`).
- `STATIC_ROOT` is set to `staticfiles/` for `collectstatic`.
