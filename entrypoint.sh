
#!/bin/sh

# This script waits for the database to be ready before starting the main application.

# Exit immediately if a command exits with a non-zero status.
set -e

# Check if DATABASE_URL is set
if [ -z "$DATABASE_URL" ]; then
  echo "Error: DATABASE_URL is not set." >&2
  exit 1
fi

# Parse host and port from DATABASE_URL
# Format: postgresql://user:password@host:port/dbname
DB_HOST=$(echo $DATABASE_URL | sed -e 's/.*@//' -e 's/:.*//')
DB_PORT=$(echo $DATABASE_URL | sed -e 's/.*://' -e 's/\/.*//')

echo "Waiting for database at $DB_HOST:$DB_PORT..."

# Loop until we can establish a connection
while ! nc -z $DB_HOST $DB_PORT; do
  sleep 0.5 # wait for half a second before retrying
done

echo "Database is up - executing command"

# Execute the main command (passed as arguments to this script)
exec "$@"
