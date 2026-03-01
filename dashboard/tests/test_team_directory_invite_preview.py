from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse


class TeamDirectoryInvitePreviewTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.admin = User.objects.create_superuser(
            username="invite_preview_admin",
            email="invite-preview-admin@example.com",
            password="pass1234",
        )
        self.client.force_login(self.admin)
        self.preview_url = reverse("team_directory_invite_preview")

    @patch("dashboard.views._generate_invite_delivery_message_with_agent")
    def test_email_preview_returns_branded_html_payload(self, mock_generate):
        mock_generate.return_value = "<p>Welcome aboard.</p><script>alert('xss')</script>"
        response = self.client.post(
            self.preview_url,
            {
                "invite_channel": "email",
                "email": "new-user@example.com",
                "signup_methods": ["local"],
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(bool(payload.get("ok")))
        self.assertEqual(payload.get("channel"), "email")
        self.assertEqual(payload.get("subject"), "You are invited to Alshival")
        self.assertTrue(bool(payload.get("message_is_html")))
        self.assertIn("<!doctype html>", str(payload.get("message_html") or "").lower())
        self.assertNotIn("<script", str(payload.get("message_html") or "").lower())
        self.assertNotIn("Additional sign-up options", str(payload.get("message_text") or ""))
        self.assertEqual(payload.get("message"), payload.get("message_html"))

    @patch("dashboard.views._generate_invite_delivery_message_with_agent")
    def test_sms_preview_returns_plain_text_payload(self, mock_generate):
        mock_generate.return_value = "Plain SMS invite message."
        response = self.client.post(
            self.preview_url,
            {
                "invite_channel": "sms",
                "phone_number": "+15551234567",
                "signup_methods": ["local"],
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(bool(payload.get("ok")))
        self.assertEqual(payload.get("channel"), "sms")
        self.assertEqual(payload.get("subject"), "")
        self.assertEqual(str(payload.get("message_html") or ""), "")
        self.assertFalse(bool(payload.get("message_is_html")))
        self.assertIn("Plain SMS invite message.", str(payload.get("message_text") or ""))
        self.assertEqual(payload.get("message"), payload.get("message_text"))
