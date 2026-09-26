from django.apps import AppConfig


class ChannelsConfig(AppConfig):
    name = "channels"
    verbose_name = "Channels"

    def ready(self):
        from django.contrib.auth.signals import user_logged_out

        from channels.auth import revoke_session_on_logout

        # Close connections holding a session when it is logged out of.
        user_logged_out.connect(
            revoke_session_on_logout,
            dispatch_uid="channels.auth.revoke_session_on_logout",
        )
