from typing import Sequence, Any, Mapping
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from sqlalchemy.orm import declared_attr, declarative_mixin, Mapped

from . import meta
from ..utils import get_links
from ... import db, ma
from ...db_utils import ResDescMixin
from ...fields import StrDictMethod

HGNC_MAX_LENGTH = 12
GENE_NAME_MAX_LENGTH = 255
GENE_NAME_TYPE_MAX_LENGTH = 100
HGNC_ID = "hgnc_id"
HGNC_PREFIX = "HGNC:"


class Gene(db.Model, ResDescMixin):
    __tablename__ = "genes"

    hgnc_id = db.Column(db.String(HGNC_MAX_LENGTH), primary_key=True)
    # TODO what data?
    status = db.Column(db.String(15), nullable=False)
    name = db.Column(db.String(125), nullable=False, unique=True, index=True)
    symbol = db.Column(db.String(30), nullable=False, unique=True, index=True)
    locus_group = db.Column(db.String(20), nullable=False)
    locus_type = db.Column(db.String(30), nullable=False)

    names: Mapped[Sequence["GeneName"]] = db.relationship(
        "GeneName", back_populates="gene"
    )

    # TODO reference raw_data, by data_source
    # raw_data


@declarative_mixin
class GeneMixin:
    hgnc_id_nullable = True

    @declared_attr
    def hgnc_id(self) -> Mapped[str]:
        return db.Column(
            db.ForeignKey(Gene.hgnc_id),
            nullable=self.hgnc_id_nullable,
            name=HGNC_ID,
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
