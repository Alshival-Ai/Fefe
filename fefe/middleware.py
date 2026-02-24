from django.conf import settings
from django.shortcuts import redirect

from dashboard.request_context import clear_current_user, set_current_user


class LoginRequiredMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        set_current_user(getattr(request, "user", None))
        try:
            if request.user.is_authenticated:
                return self.get_response(request)

            path = request.path
            if path.startswith(settings.LOGIN_URL):
                return self.get_response(request)
            if path.startswith('/admin/'):
                return self.get_response(request)

            static_prefix = f"/{settings.STATIC_URL.lstrip('/')}"
            if path.startswith(static_prefix):
                return self.get_response(request)

            return redirect(f"{settings.LOGIN_URL}?next={path}")
        finally:
            clear_current_user()
