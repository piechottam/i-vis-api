from typing import Sequence, TYPE_CHECKING, Any, Mapping

from sqlalchemy.orm import declared_attr, declarative_mixin, Mapped
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

from . import meta
from ..utils import get_links
from ... import db, ma
from ...db_utils import ResDescMixin, CoreTypeMixin
from ...fields import StrDictMethod
from ...resource import ResourceDesc

if TYPE_CHECKING:
    pass

CHEMBL_MAX_LENGTH = 13
CHEMBL_ID = "chembl_id"
DRUG_NAME_MAX_LENGTH = 255
CHEMBL_PREFIX = "CHEMBL"


class Drug(db.Model, CoreTypeMixin, ResDescMixin):
    __tablename__ = "drugs"
    __table_args__ = (db.UniqueConstraint(CHEMBL_ID),)

    chembl_id = db.Column(db.VARBINARY(CHEMBL_MAX_LENGTH), nullable=False, index=True)
    names: Mapped[Sequence["DrugName"]] = db.relationship(
        "DrugName", back_populates="drug"
    )


@declarative_mixin
class DrugMixin:
    chembl_id_nullable = True

    @declared_attr
    def chembl_id(self) -> Mapped[str]:
        return db.Column(
            db.ForeignKey(Drug.chembl_id),
            nullable=self.chembl_id_nullable,
            name=CHEMBL_ID,
            index=True,
        )

    @declared_attr
    def drug(self) -> Mapped[Drug]:
        return db.relationship(Drug)


class DrugName(db.Model, DrugMixin, ResDescMixin):
    __tablename__ = "drug_names"
    __table_args__ = (db.UniqueConstraint(CHEMBL_ID, "name"),)
    chembl_id_nullable = False

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.VARBINARY(DRUG_NAME_MAX_LENGTH), nullable=False)
    data_sources = db.Column(db.String(255), nullable=False)


drug_name_res_desc = ResourceDesc([CHEMBL_ID, "name", "data_sources"])


class DrugNameSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = DrugName
        load_instance = True  # Optional: deserialize to model instances
        include_relationships = True
        include_fk = True


class DrugSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Drug
        load_instance = True  # Optional: deserialize to model instances
        include_relationships = True

    raw_names = ma.Nested(
        DrugNameSchema(
            many=True,
            exclude=(CHEMBL_ID,),
        ),
    )
    links = StrDictMethod("_get_links")

    def _get_links(self, obj: Any) -> Mapping[str, str]:
        return get_links(self, meta.name, obj)
