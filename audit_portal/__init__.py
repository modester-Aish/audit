from __future__ import annotations

from flask import Flask

from .config import Settings
from .routes import bp as routes_bp
from .service import ensure_auto_recrawl_scheduler
from .storage import ensure_storage


def create_app() -> Flask:
    app = Flask(__name__)
    # When the portal is reverse-proxied under /audit/, Next.js may add/remove
    # trailing slashes. Allow both forms to prevent 405/redirect issues.
    app.url_map.strict_slashes = False
    settings = Settings.from_env()

    app.config["SECRET_KEY"] = settings.secret_key
    app.config["AUDIT_SETTINGS"] = settings

    with app.app_context():
        ensure_storage()
        ensure_auto_recrawl_scheduler()

    app.register_blueprint(routes_bp)
    return app

