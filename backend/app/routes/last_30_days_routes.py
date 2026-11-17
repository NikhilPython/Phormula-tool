from flask import Blueprint

from app.routes.live_data_bi_routes import live_mtd_vs_previous


last_30_days_bp = Blueprint("last_30_days_bp", __name__)


@last_30_days_bp.route("/live_last_30_days", methods=["GET"])
def live_last_30_days():
    return live_mtd_vs_previous(period_mode="last_30_days")
