"""CLI commands for LlmMemeDescriber administration."""
import sys
import getpass
from sqlmodel import Session, select

from .config import load_settings
from .db import init_db
from .models import BasicAuthUser
from .auth import get_password_hasher


def create_user():
    """Create a new Basic Auth user via CLI."""
    print("=== Create Basic Auth User ===")

    # Load settings and initialize database
    settings = load_settings()
    if not settings.basic_auth:
        print("ERROR: BASIC_AUTH must be enabled in settings.")
        print("Set BASIC_AUTH=true in your .env file or environment.")
        sys.exit(1)

    engine = init_db()

    # Get username
    username = input("Username: ").strip()
    if not username:
        print("ERROR: Username cannot be empty.")
        sys.exit(1)

    # Check if user exists
    with Session(engine) as session:
        stmt = select(BasicAuthUser).where(BasicAuthUser.username == username)
        existing = session.exec(stmt).first()
        if existing:
            print(f"ERROR: User '{username}' already exists.")
            sys.exit(1)

    # Get password (hidden input)
    password = getpass.getpass("Password: ")
    password_confirm = getpass.getpass("Confirm password: ")

    if password != password_confirm:
        print("ERROR: Passwords do not match.")
        sys.exit(1)

    if len(password) < 8:
        print("ERROR: Password must be at least 8 characters.")
        sys.exit(1)

    # Hash password
    ph = get_password_hasher()
    password_hash = ph.hash(password)

    # Create user
    from datetime import datetime, timezone
    user = BasicAuthUser(
        username=username,
        password_hash=password_hash,
        enabled=True,
        created_at=datetime.now(timezone.utc)
    )

    with Session(engine) as session:
        session.add(user)
        session.commit()

    print(f"✓ User '{username}' created successfully.")


def list_users():
    """List all Basic Auth users."""
    print("=== Basic Auth Users ===")

    settings = load_settings()
    if not settings.basic_auth:
        print("ERROR: BASIC_AUTH must be enabled in settings.")
        sys.exit(1)

    engine = init_db()

    with Session(engine) as session:
        stmt = select(BasicAuthUser)
        users = session.exec(stmt).all()

        if not users:
            print("No users found.")
            return

        print(f"\n{'Username':<20} {'Enabled':<10} {'Created':<25} {'Last Used':<25}")
        print("-" * 80)
        for user in users:
            enabled = "✓" if user.enabled else "✗"
            created = user.created_at.strftime("%Y-%m-%d %H:%M:%S") if user.created_at else "N/A"
            last_used = user.last_used_at.strftime("%Y-%m-%d %H:%M:%S") if user.last_used_at else "Never"
            print(f"{user.username:<20} {enabled:<10} {created:<25} {last_used:<25}")


def delete_user():
    """Delete a Basic Auth user."""
    print("=== Delete Basic Auth User ===")

    settings = load_settings()
    if not settings.basic_auth:
        print("ERROR: BASIC_AUTH must be enabled in settings.")
        sys.exit(1)

    engine = init_db()

    username = input("Username to delete: ").strip()
    if not username:
        print("ERROR: Username cannot be empty.")
        sys.exit(1)

    with Session(engine) as session:
        stmt = select(BasicAuthUser).where(BasicAuthUser.username == username)
        user = session.exec(stmt).first()

        if not user:
            print(f"ERROR: User '{username}' not found.")
            sys.exit(1)

        confirm = input(f"Are you sure you want to delete '{username}'? (yes/no): ").strip().lower()
        if confirm != 'yes':
            print("Cancelled.")
            sys.exit(0)

        session.delete(user)
        session.commit()

    print(f"✓ User '{username}' deleted successfully.")


def main():
    """Main CLI entry point."""
    if len(sys.argv) < 2:
        print("Usage: python -m llm_memedescriber.cli <command>")
        print("\nCommands:")
        print("  create-user   Create a new Basic Auth user")
        print("  list-users    List all Basic Auth users")
        print("  delete-user   Delete a Basic Auth user")
        sys.exit(1)

    command = sys.argv[1]

    if command == "create-user":
        create_user()
    elif command == "list-users":
        list_users()
    elif command == "delete-user":
        delete_user()
    else:
        print(f"ERROR: Unknown command '{command}'")
        sys.exit(1)


if __name__ == "__main__":
    main()
