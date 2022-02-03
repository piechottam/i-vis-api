from flask_wtf import FlaskForm
from wtforms import (
    TextAreaField,
    SubmitField,
    SelectMultipleField,
    widgets,
)
from wtforms.validators import DataRequired

from .plugin import DataSource


class MultiCheckboxField(SelectMultipleField):
    widget = widgets.ListWidget(prefix_label=False)
    option_widget = widgets.CheckboxInput()


class VariantSearchForm(FlaskForm):
    """Variant search form."""

    variants = TextAreaField("Variants", validators=[DataRequired()])
    sources = MultiCheckboxField(
        "Source",
        choices=[
            (data_source.name, data_source.name)
            for data_source in DataSource.instances()
        ],
    )
    submit = SubmitField("Search")
