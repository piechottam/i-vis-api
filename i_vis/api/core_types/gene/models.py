from typing import Sequence, Any, Mapping
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from sqlalchemy.orm import declared_attr, declarative_mixin, Mapped

from . import meta
from ..utils import get_links
from ... import db, ma
from ...db_utils import ResDescMixin, CoreTypeMixin
from ...fields import StrDictMethod
from ...resource import ResourceDesc

HGNC_MAX_LENGTH = 12
HGNC_ID = "hgnc_id"
GENE_NAME_MAX_LENGTH = 255
GENE_NAME_TYPE_MAX_LENGTH = 100
HGNC_PREFIX = "HGNC:"


class Gene(db.Model, CoreTypeMixin, ResDescMixin):
    __tablename__ = "genes"
    __table_args__ = (db.UniqueConstraint(HGNC_ID),)

    hgnc_id = db.Column(db.VARBINARY(HGNC_MAX_LENGTH), nullable=False, index=True)
    names: Mapped[Sequence["GeneName"]] = db.relationship(
        "GeneName", back_populates="gene"
    )


@declarative_mixin
class GeneMixin:
    hgnc_id_nullable = True

    @declared_attr
    def hgnc_id(self) -> Mapped[str]:
        return db.Column(
            db.ForeignKey(Gene.hgnc_id),
            nullable=self.hgnc_id_nullable,
            name=HGNC_ID,
            index=True,
        )

    @declared_attr
    def gene(self) -> Mapped[Gene]:
        return db.relationship(Gene)


class GeneName(db.Model, GeneMixin, ResDescMixin):
    __tablename__ = "gene_names"
    __table_args__ = (db.UniqueConstraint(HGNC_ID, "name"),)
    hgnc_id_nullable = False

    id = db.Column(db.Integer, primary_key=True)
    type = db.Column(db.String(GENE_NAME_TYPE_MAX_LENGTH), nullable=False)
    name = db.Column(db.String(GENE_NAME_MAX_LENGTH), nullable=False)
    data_sources = db.Column(db.String(255), nullable=False)


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
