import logging
from django.apps import AppConfig


logger = logging.getLogger(__name__)

class ApiConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "api"

    def ready(self):
        
        try:
            import .receivers  # noqa F401
        except ImportError:
            logger.error("Could not import receivers")
