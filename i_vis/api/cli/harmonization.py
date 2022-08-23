import click
from flask.cli import AppGroup
from tabulate import tabulate

from ..plugin import DataSource
from ..resource import ResourceId, Table, res_registry
from .utils import validate_data_source, validate_rid

app_group = AppGroup("harm", short_help="Show harmonization relevant info(s).")


@app_group.command("list", short_help="List harmonizations for data sources.")
@click.argument("data-source", nargs=1, callback=validate_data_source)
def list_harmonizations(data_source: DataSource) -> None:
    table = []
    for res in data_source.task_builder.offered_resources():
        if res.get_type() != Table.get_type():
            continue

        etl = data_source.etl_manager.part2etl[res.qname]
        row = {
            "rid": str(res.rid),
            "core_type": ",".join(core_type.name for core_type in etl.core_types),
        }
        table.append(row.values())
    headers = ("rid", "type")
    print(tabulate(table, headers=headers, tablefmt="plain"))


@app_group.command("show", short_help="Show details of harmonizations.")
@click.argument("res-id", nargs=1, callback=validate_rid)
def show(rid: ResourceId) -> None:
    res = res_registry[rid]
    if not res:
        print("Unknown resource")
        # print("No harmonization associated.")
