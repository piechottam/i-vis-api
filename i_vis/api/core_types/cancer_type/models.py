from typing import Sequence, TYPE_CHECKING, Any, Mapping

from sqlalchemy.orm import declared_attr, declarative_mixin, Mapped
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

from . import meta
from ... import ma, db
from ..utils import get_links
from ...db_utils import ResDescMixin, CoreTypeMixin
from ...resource import ResourceDesc
from ...fields import StrDictMethod

if TYPE_CHECKING:
    pass

DOID_MAX_LENGTH = 12
DO_ID = "do_id"
CANCER_TYPE_NAME_MAX_LENGTH = 255
DOID_PREFIX = "DOID:"


class CancerType(db.Model, CoreTypeMixin, ResDescMixin):
    __tablename__ = "cancer_types"
    __table_args__ = (db.UniqueConstraint(DO_ID),)

    id = db.Column(db.Integer, primary_key=True)
    do_id = db.Column(db.VARBINARY(DOID_MAX_LENGTH), nullable=False, index=True)
    names: Mapped[Sequence["CancerTypeName"]] = db.relationship(
        "CancerTypeName", back_populates="cancer_type"
    )


@declarative_mixin
class CancerTypeMixin:
    do_id_nullable = True

    @declared_attr
    def do_id(self) -> Mapped[str]:
        return db.Column(
            db.ForeignKey(CancerType.do_id),
            nullable=self.do_id_nullable,
            name=DO_ID,
            index=True,
        )

    @declared_attr
    def cancer_type(self) -> Mapped[CancerType]:
        return db.relationship(CancerType)


class CancerTypeName(db.Model, CancerTypeMixin, ResDescMixin):
    __tablename__ = "cancer_type_names"
    __table_args__ = (db.UniqueConstraint(DO_ID, "name"),)
    do_id_nullable = False

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(CANCER_TYPE_NAME_MAX_LENGTH), nullable=False)
    data_sources = db.Column(db.Text, nullable=False)


cancer_type_name_res_desc = ResourceDesc([DO_ID, "name", "data_sources"])


class CancerTypeNameSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = CancerTypeName
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
