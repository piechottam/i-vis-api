from typing import Sequence, TYPE_CHECKING, Any, Mapping

from sqlalchemy.orm import declared_attr, declarative_mixin, Mapped
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

from . import TNAME, NAMES_RAW_TNAME, meta
from ... import ma, db
from ..utils import get_links
from ...db_utils import ResDescMixin
from ...resource import ResourceDesc
from ...fields import StrDictMethod

if TYPE_CHECKING:
    pass

DOID_MAX_LENGTH = 12
DO_ID = "do_id"
CANCER_TYPE_MAX_LENGTH = 255
DOID_PREFIX = "DOID:"


class CancerType(db.Model, ResDescMixin):
    __tablename__ = TNAME

    id = db.Column(db.Integer, autoincrement=True)
    do_id = db.Column(db.String(DOID_MAX_LENGTH), primary_key=True)

    raw_names: Mapped[Sequence["CancerTypeNameRaw"]] = db.relationship(
        "CancerTypeNameRaw"
    )


@declarative_mixin
class CancerTypeName:
    @declared_attr
    # pylint: disable=invalid-name
    def id(self) -> Mapped[int]:
        return db.Column(db.Integer, primary_key=True, autoincrement=True)

    @declared_attr
    def name(self) -> Mapped[str]:
        return db.Column(db.String(255), nullable=False, index=True)

    @declared_attr
    def do_id(self) -> Mapped[str]:
        return db.Column(db.ForeignKey(CancerType.do_id), nullable=False)


class CancerTypeNameRaw(db.Model, CancerTypeName, ResDescMixin):
    __tablename__ = NAMES_RAW_TNAME


@declarative_mixin
class CancerTypeMixin:
    @declared_attr
    def do_id(self) -> Mapped[str]:
        return db.Column(
            db.ForeignKey(CancerType.do_id),
            nullable=True,
            name=DO_ID,
        )

    @declared_attr
    def cancer_type(self) -> Mapped[CancerType]:
        return db.relationship(CancerType)


cancer_type_name_res_desc = ResourceDesc(["do_id", "name", "plugins"])


class CancerTypeNameSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = CancerTypeNameRaw
        load_instance = True  # Optional: deserialize to model instances
        include_relationships = True
        include_fk = True


class CancerTypeSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = CancerType
        load_instance = True  # Optional: deserialize to model instances
        include_relationships = True

    raw_names = ma.Nested(
        CancerTypeNameSchema(
            many=True,
            exclude=(DO_ID,),
        ),
    )
    links = StrDictMethod("_get_links")

    def _get_links(self, obj: Any) -> Mapping[str, str]:
        return get_links(self, meta.name, obj)
