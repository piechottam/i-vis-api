from typing import Any, Mapping, Sequence

from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from sqlalchemy import VARBINARY, Column, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, declarative_mixin, declared_attr, relationship

from ... import Base, ma
from ...db_utils import CoreTypeModel, ResDescMixin
from ...fields import StrDictMethod
from ...resource import ResourceDesc
from ..utils import get_links
from . import meta

CHEMBL_MAX_LENGTH = 13
CHEMBL_ID = "chembl_id"
DRUG_NAME_MAX_LENGTH = 255
CHEMBL_PREFIX = "CHEMBL"


class Drug(CoreTypeModel):
    __tablename__ = "drugs"
    __table_args__ = (UniqueConstraint(CHEMBL_ID),)

    chembl_id: Mapped[str] = Column(
        String(CHEMBL_MAX_LENGTH), nullable=False, index=True
    )
    names: Mapped[Sequence["DrugName"]] = relationship(
        "DrugName", back_populates="drug"
    )


@declarative_mixin
class DrugMixin(ResDescMixin):
    chembl_id_nullable = True

    @declared_attr
    def chembl_id(self) -> Mapped[str]:
        return Column(
            ForeignKey(Drug.chembl_id),
            nullable=self.chembl_id_nullable,
            name=CHEMBL_ID,
            index=True,
        )

    @declared_attr
    def drug(self) -> Mapped[Drug]:
        return relationship(Drug)


class DrugName(Base, DrugMixin):
    __tablename__ = "drug_names"
    __table_args__ = (UniqueConstraint(CHEMBL_ID, "name"),)
    chembl_id_nullable = False

    id = Column(Integer, primary_key=True)
    name = Column(VARBINARY(DRUG_NAME_MAX_LENGTH), nullable=False)
    data_sources = Column(String(255), nullable=False)


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
