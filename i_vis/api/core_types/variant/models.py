from sqlalchemy.orm import declared_attr, Mapped, declarative_mixin

from ... import db
from ..variant_utils import (
    DescType,
    VARIANT_REF_LEN,
    VARIANT_DESC_LEN,
    ModifyFlag,
)


@declarative_mixin
class VariantMixin:
    @declared_attr
    def hgvs(self) -> Mapped[str]:
        return db.Column(
            db.String(VARIANT_REF_LEN + VARIANT_REF_LEN),
            nullable=False,
            index=True,
            name="hgvs",
        )

    @declared_attr
    def ref(self) -> Mapped[str]:
        return db.Column(
            db.String(VARIANT_REF_LEN),
            nullable=False,
            index=True,
            name="ref",
        )

    @declared_attr
    def ref_version(self) -> Mapped[int]:
        return db.Column(
            db.SmallInteger,
            nullable=True,
            index=True,
            name="ref_version",
        )

    @declared_attr
    def desc(self) -> Mapped[str]:
        return db.Column(
            db.String(VARIANT_DESC_LEN),
            nullable=False,
            index=True,
            name="desc",
        )

    @declared_attr
    def desc_type(self) -> Mapped[DescType]:
        return db.Column(
            db.Enum(DescType),
            nullable=False,
            index=True,
            name="desc_type",
        )

    @declared_attr
    def modify_flags(self) -> Mapped[ModifyFlag]:
        return db.Column(
            db.Integer,
            nullable=False,
            default=ModifyFlag.NONE,
            index=True,
            name="modify_flags",
        )

    # FUTURE add
    # @declared_attr
    # def type(self) -> Mapped[VariantType]:
    #    return db.Column(
    #        db.Enum(VariantType),
    #        nullable=False,
    #        index=True,
    #        name="type",
    #    )

    @property
    def to_hgvs(self) -> str:
        ref = str(self.ref)
        if self.ref_version:
            ref = f"{ref}.{str(self.ref_version)}"
        desc = str(self.desc)
        if self.desc_type:
            desc = f"{desc}.{str(self.desc_type)}"
        return f"{ref}:{desc}"
