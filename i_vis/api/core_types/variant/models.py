from sqlalchemy import Column, Enum, Integer, SmallInteger, String
from sqlalchemy.orm import Mapped, declarative_mixin, declared_attr

from ...db_utils import CoreTypeModel
from ..variant_utils import VARIANT_DESC_LEN, VARIANT_REF_LEN, DescType, ModifyFlag


@declarative_mixin
class VariantMixin:
    @declared_attr
    def hgvs(self) -> Mapped[str]:
        return Column(
            String(VARIANT_REF_LEN + VARIANT_REF_LEN),
            nullable=False,
            index=True,
            name="hgvs",
        )

    @declared_attr
    def ref(self) -> Mapped[str]:
        return Column(
            String(VARIANT_REF_LEN),
            nullable=False,
            index=True,
            name="ref",
        )

    @declared_attr
    def ref_version(self) -> Mapped[int]:
        return Column(
            SmallInteger,
            nullable=True,
            index=True,
            name="ref_version",
        )

    @declared_attr
    def desc(self) -> Mapped[str]:
        return Column(
            String(VARIANT_DESC_LEN),
            nullable=False,
            index=True,
            name="desc",
        )

    @declared_attr
    def desc_type(self) -> Mapped[DescType]:
        return Column(
            Enum(DescType),
            nullable=False,
            index=True,
            name="desc_type",
        )

    @declared_attr
    def flags(self) -> Mapped[ModifyFlag]:
        return Column(
            Integer,
            nullable=False,
            default=ModifyFlag.NONE,
            index=True,
            name="modify_flags",
        )

    # FUTURE add
    # @declared_attr
    # def type(self) -> Mapped[VariantType]:
    #    return Column(
    #        Enum(VariantType),
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


class HarmonizedVariant(CoreTypeModel, VariantMixin):
    __abstract__ = True
