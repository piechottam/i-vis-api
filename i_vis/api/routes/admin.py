"""Admin specific endpoints."""

from typing import Any, Mapping

from flask import abort, jsonify, Blueprint


from .. import session
from ..models import Request as RequestModel, User, TableUpdate


bp = Blueprint("admin_api", __name__, url_prefix="/admin")


@bp.route("/table", methods=["GET"])
def list_tables() -> Any:
    def helper(table: TableUpdate) -> Mapping[str, Any]:
        return {
            "id": table.id,
            "tname": table.name,
            "row_count": table.row_count,
            "updated_at": table.updated_at,
        }

    tables = [helper(table) for table in session.query(TableUpdate).all()]
    return jsonify(tables)


@bp.route("/request", methods=["GET"])
def list_requests() -> Any:
    requests = session.query(RequestModel).all()
    return jsonify(request.in_rid for request in requests)


@bp.route("/request/<int:request_id>", methods=["GET"])
def request_info(request_id: int) -> Any:
    request_data = session.query(RequestModel).filter_by(id=request_id).first()
    if request_data is None:
        return abort(404)

    return jsonify(request_data)


@bp.route("/user", methods=["GET"])
def list_users() -> Any:
    users = User.query.all()
    return jsonify(users)


@bp.route("/user/<int:user_id>", methods=["GET"])
def user(user_id: int) -> Any:
    user_data = User.load_by_id(user_id)
    if user_data is None:
        return abort(404)

    return jsonify(user_data)
