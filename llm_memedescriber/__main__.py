import sys

# Check if CLI command
if len(sys.argv) > 1 and sys.argv[1] in ['create-user', 'list-users', 'delete-user']:
    from .cli import main as cli_main
    cli_main()
    sys.exit(0)

# Otherwise run normal app
from .main import main

if __name__ == "__main__":
    main()