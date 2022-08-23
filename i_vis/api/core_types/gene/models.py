from typing import Any, Mapping, Sequence

from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, declarative_mixin, declared_attr, relationship

from ... import Base, ma
from ...db_utils import CoreTypeModel, ResDescMixin
from ...fields import StrDictMethod
from ...resource import ResourceDesc
from ..utils import get_links
from . import meta

HGNC_MAX_LENGTH = 12
HGNC_ID = "hgnc_id"
GENE_NAME_MAX_LENGTH = 255
GENE_NAME_TYPE_MAX_LENGTH = 100
HGNC_PREFIX = "HGNC:"


class Gene(CoreTypeModel):
    __tablename__ = "genes"
    __table_args__ = (UniqueConstraint(HGNC_ID),)

    hgnc_id: Mapped[str] = Column(
        String(HGNC_MAX_LENGTH),
        nullable=False,
        index=True,
    )
    names: Mapped[Sequence["GeneName"]] = relationship(
        "GeneName", back_populates="gene"
    )


@declarative_mixin
class GeneMixin:
    hgnc_id_nullable = True

    @declared_attr
    def hgnc_id(self) -> Mapped[str]:
        return Column(
            ForeignKey(Gene.hgnc_id),
            nullable=self.hgnc_id_nullable,
            name=HGNC_ID,
            index=True,
        )

    @declared_attr
    def gene(self) -> Mapped[Gene]:
        return relationship(Gene)


class GeneName(Base, GeneMixin, ResDescMixin):
    __tablename__ = "gene_names"
    __table_args__ = (UniqueConstraint(HGNC_ID, "name"),)
    hgnc_id_nullable = False

    id: Mapped[int] = Column(Integer, primary_key=True)
    type: Mapped[str] = Column(String(GENE_NAME_TYPE_MAX_LENGTH), nullable=False)
    name: Mapped[str] = Column(String(GENE_NAME_MAX_LENGTH), nullable=False)
    data_sources: Mapped[str] = Column(String(255), nullable=False)


gene_name_res_desc = ResourceDesc([HGNC_ID, "name", "type", "data_sources"])


class GeneNameSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = GeneName
        load_instance = True  # Optional: deserialize to model instances
        include_relationships = True
        include_fk = True


class GeneSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Gene
        load_instance = True  # Optional: deserialize to model instances
        include_relationships = True

    names = ma.Nested(
        GeneNameSchema(
            many=True,
            exclude=("gene", HGNC_ID),
        ),
    )
    links = StrDictMethod("_get_links")

    def _get_links(self, obj: Any) -> Mapping[str, str]:
        return get_links(self, meta.name, obj)
