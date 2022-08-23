"""
CLI user(s)
"""

from secrets import token_urlsafe

import click
from flask.cli import AppGroup, with_appcontext
from tabulate import tabulate

from .. import session
from ..models import User
from ..utils import get_logger
from .utils import validate_unique_mail, validate_unique_username

app_group = AppGroup("user", short_help="Show user(s) relevant info(s).")

logger = get_logger()


@app_group.command("create", short_help="Create user.")
@click.argument("user", nargs=1, callback=validate_unique_username)
@click.argument("mail", nargs=1, callback=validate_unique_mail)
@with_appcontext
def create(user: str, mail: str) -> None:
    """Create user and personal token"""

    new_user = User(name=user, mail=mail)
    new_user.set_password(token_urlsafe(12))
    session.add(new_user)
    session.commit()
    print(f"User created, uid: {new_user.id}")


@app_group.command("edit", short_help="Edit user.")
@click.argument("uid", nargs=1)
@click.option(
    "-n",
    "--new-name",
    help="New name.",
    default="False",
)
@click.option(
    "-m",
    "--new-mail",
    help="New mail.",
    default="",
)
@with_appcontext
def edit(uid: int, new_name: str, new_mail: str) -> None:
    """Edit user"""
    user = User.query.get(uid)
    if new_name:
        user.name = new_name
    if new_mail:
        user.mail = new_mail
    choice = input("Really change user details? Yn")
    print(f"name: {user.name}; mail: {user.mail}")
    if choice != "Y":
        print("Aborted")
    session.add(user)
    session.commit()
    print("User updated")


@app_group.command("remove", short_help="Remove user.")
@click.argument("uid", nargs=1)
@with_appcontext
def remove(uid: int) -> None:
    """Remove user"""
    user = User.get(uid)
    choice = input(f"Really remove user: {user.name}? Yn")
    if choice != "Y":
        print("Aborted")
    session.delete(user)
    session.commit()
    print("User updated")


@app_group.command("list", short_help="List users.")
@with_appcontext
def show() -> None:
    table = []
    for user in User.query.all():
        row = {
            "uid": user.id,
            "name": user.name,
            "mail": user.mail,
            "token": user.token,
        }
        table.append(row.values())
    headers = ("uid", "Name", "Mail", "Token")
    print(tabulate(table, headers=headers, tablefmt="plain"))
