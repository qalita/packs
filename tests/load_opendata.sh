#!/usr/bin/env bash
set -euo pipefail

# Configuration
POSTGRES_HOST=${POSTGRES_HOST:-localhost}
POSTGRES_PORT=${POSTGRES_PORT:-5433}
POSTGRES_DB=${POSTGRES_DB:-app_db}
POSTGRES_USER=${POSTGRES_USER:-postgres}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-postgres}

MYSQL_HOST=${MYSQL_HOST:-localhost}
MYSQL_PORT=${MYSQL_PORT:-3307}
MYSQL_DB=${MYSQL_DB:-app_db}
MYSQL_USER=${MYSQL_USER:-app}
MYSQL_PASSWORD=${MYSQL_PASSWORD:-app}
MYSQL_ROOT_PASSWORD=${MYSQL_ROOT_PASSWORD:-root}

ORACLE_HOST=${ORACLE_HOST:-localhost}
ORACLE_PORT=${ORACLE_PORT:-1522}
ORACLE_SERVICE=${ORACLE_SERVICE:-FREEPDB1}
ORACLE_USER=${ORACLE_USER:-app}
ORACLE_PASSWORD=${ORACLE_PASSWORD:-app}

# MSSQL (SQL Server)
MSSQL_HOST=${MSSQL_HOST:-localhost}
MSSQL_PORT=${MSSQL_PORT:-1434}
MSSQL_DB=${MSSQL_DB:-app_db}
MSSQL_SA_USER=${MSSQL_SA_USER:-sa}
MSSQL_SA_PASSWORD=${MSSQL_SA_PASSWORD:-YourStrong@Passw0rd}
MSSQL_APP_USER=${MSSQL_APP_USER:-app}
MSSQL_APP_PASSWORD=${MSSQL_APP_PASSWORD:-app}

DATA_DIR="$(cd "$(dirname "$0")" && pwd)/data"
IRIS_CSV="$DATA_DIR/iris.csv"
mkdir -p "$DATA_DIR"

echo "Downloading sample OpenData (Iris dataset as CSV)..."
IRIS_URL="https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv"
curl -fsSL "$IRIS_URL" -o "$IRIS_CSV"

echo "Preparing SQL for PostgreSQL..."
cat > "$DATA_DIR/iris_postgres_host.sql" <<SQL
DROP TABLE IF EXISTS iris;
CREATE TABLE iris (
  sepal_length DOUBLE PRECISION,
  sepal_width DOUBLE PRECISION,
  petal_length DOUBLE PRECISION,
  petal_width DOUBLE PRECISION,
  species TEXT
);
\\copy iris(sepal_length, sepal_width, petal_length, petal_width, species)
FROM '$IRIS_CSV' WITH (FORMAT csv, HEADER true);
SQL

cat > "$DATA_DIR/iris_postgres_container.sql" <<'SQL'
DROP TABLE IF EXISTS iris;
CREATE TABLE iris (
  sepal_length DOUBLE PRECISION,
  sepal_width DOUBLE PRECISION,
  petal_length DOUBLE PRECISION,
  petal_width DOUBLE PRECISION,
  species TEXT
);
\copy iris(sepal_length, sepal_width, petal_length, petal_width, species) FROM '/data/iris.csv' WITH (FORMAT csv, HEADER true);
SQL

echo "Preparing SQL for MySQL..."
cat > "$DATA_DIR/iris_mysql_host.sql" <<SQL
DROP TABLE IF EXISTS iris;
CREATE TABLE iris (
  sepal_length DOUBLE,
  sepal_width DOUBLE,
  petal_length DOUBLE,
  petal_width DOUBLE,
  species VARCHAR(50)
);
LOAD DATA LOCAL INFILE '$IRIS_CSV'
INTO TABLE iris
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(sepal_length, sepal_width, petal_length, petal_width, species);
SQL

cat > "$DATA_DIR/iris_mysql_container.sql" <<'SQL'
DROP TABLE IF EXISTS iris;
CREATE TABLE iris (
  sepal_length DOUBLE,
  sepal_width DOUBLE,
  petal_length DOUBLE,
  petal_width DOUBLE,
  species VARCHAR(50)
);
LOAD DATA LOCAL INFILE '/data/iris.csv'
INTO TABLE iris
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(sepal_length, sepal_width, petal_length, petal_width, species);
SQL

echo "Preparing SQL for Oracle..."
cat > "$DATA_DIR/iris_oracle.sql" <<SQL
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE iris';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
CREATE TABLE iris (
  sepal_length NUMBER,
  sepal_width NUMBER,
  petal_length NUMBER,
  petal_width NUMBER,
  species VARCHAR2(50)
);
-- External table approach to load CSV (assumes DIRECTORY data_dir exists and is granted to $ORACLE_USER)
CREATE TABLE iris_ext (
  sepal_length NUMBER,
  sepal_width NUMBER,
  petal_length NUMBER,
  petal_width NUMBER,
  species VARCHAR2(50)
) ORGANIZATION EXTERNAL (
  TYPE ORACLE_LOADER
  DEFAULT DIRECTORY data_dir
  ACCESS PARAMETERS (
    RECORDS DELIMITED BY NEWLINE
    SKIP 1
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
    (sepal_length, sepal_width, petal_length, petal_width, species)
  )
  LOCATION ('iris.csv')
)
REJECT LIMIT UNLIMITED;
INSERT INTO iris SELECT * FROM iris_ext;
SQL

echo "Preparing SQL for MSSQL (SQL Server)..."
cat > "$DATA_DIR/iris_mssql.sql" <<SQL
IF DB_ID(N'$MSSQL_DB') IS NULL
BEGIN
  CREATE DATABASE [$MSSQL_DB];
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'$MSSQL_APP_USER')
BEGIN
  CREATE LOGIN [$MSSQL_APP_USER] WITH PASSWORD = N'$MSSQL_APP_PASSWORD', CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF;
END;
GO

USE [$MSSQL_DB];
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$MSSQL_APP_USER')
BEGIN
  CREATE USER [$MSSQL_APP_USER] FOR LOGIN [$MSSQL_APP_USER];
END;
GO

IF NOT EXISTS (
  SELECT 1
  FROM sys.database_role_members drm
  JOIN sys.database_principals r ON drm.role_principal_id = r.principal_id
  JOIN sys.database_principals m ON drm.member_principal_id = m.principal_id
  WHERE r.name = N'db_owner' AND m.name = N'$MSSQL_APP_USER'
)
BEGIN
  ALTER ROLE db_owner ADD MEMBER [$MSSQL_APP_USER];
END;
GO

IF OBJECT_ID(N'dbo.iris', N'U') IS NOT NULL DROP TABLE dbo.iris;
CREATE TABLE dbo.iris (
  sepal_length FLOAT NULL,
  sepal_width  FLOAT NULL,
  petal_length FLOAT NULL,
  petal_width  FLOAT NULL,
  species NVARCHAR(50) NULL
);
GO

BULK INSERT dbo.iris
FROM '/data/iris.csv'
WITH (
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  ROWTERMINATOR = '0x0a',
  TABLOCK
);
GO
SQL

command_exists() { command -v "$1" >/dev/null 2>&1; }

echo "Loading into PostgreSQL..."
if docker ps --format '{{.Names}}' | grep -q '^tests-postgres$'; then
  docker exec -i tests-postgres psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /data/iris_postgres_container.sql
elif command_exists psql; then
  export PGPASSWORD="$POSTGRES_PASSWORD"
  psql "host=$POSTGRES_HOST port=$POSTGRES_PORT dbname=$POSTGRES_DB user=$POSTGRES_USER" -f "$DATA_DIR/iris_postgres_host.sql"
else
  echo "psql not found and container tests-postgres not running. Skipping PostgreSQL load." >&2
fi

echo "Loading into MySQL..."
if docker ps --format '{{.Names}}' | grep -q '^tests-mysql$'; then
  docker exec -i tests-mysql bash -lc "mysql --local-infile=1 -u '$MYSQL_USER' -p'$MYSQL_PASSWORD' '$MYSQL_DB' < /data/iris_mysql_container.sql"
elif command_exists mysql; then
  mysql --local-infile=1 -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" < "$DATA_DIR/iris_mysql_host.sql"
else
  echo "mysql client not found and container tests-mysql not running. Skipping MySQL load." >&2
fi

echo "Loading into Oracle..."
(
  set +e
  if docker ps --format '{{.Names}}' | grep -q '^tests-oracle$'; then
    # Inside the container, the listener is on 1521 and host is localhost
    ORA_IN_HOST=localhost
    ORA_IN_PORT=1521
    docker exec -i tests-oracle bash -lc "cat <<'SQL' | sqlplus -s \"sys/$ORACLE_PASSWORD@//$ORA_IN_HOST:$ORA_IN_PORT/$ORACLE_SERVICE as sysdba\"\nCREATE OR REPLACE DIRECTORY data_dir AS '/data';\nGRANT READ, WRITE ON DIRECTORY data_dir TO $ORACLE_USER;\nSQL" || true
    docker exec -i tests-oracle bash -lc "sqlplus -s \"$ORACLE_USER/$ORACLE_PASSWORD@//$ORA_IN_HOST:$ORA_IN_PORT/$ORACLE_SERVICE\" @/data/iris_oracle.sql" || true
  elif command_exists sqlplus; then
    cat > "$DATA_DIR/iris_oracle_loader.sql" <<SQL
@$DATA_DIR/iris_oracle.sql
SQL
    sqlplus -s "$ORACLE_USER/$ORACLE_PASSWORD@//$ORACLE_HOST:$ORACLE_PORT/$ORACLE_SERVICE" @"$DATA_DIR/iris_oracle_loader.sql" || true
  else
    echo "sqlplus not found and container tests-oracle not running. Skipping Oracle load." >&2
  fi
) || echo "Oracle load failed (non-fatal). Continuing..." >&2

echo "Loading into MSSQL (SQL Server)..."
if docker ps --format '{{.Names}}' | grep -q '^tests-mssql$'; then
  if docker exec tests-mssql bash -lc 'test -x /opt/mssql-tools/bin/sqlcmd'; then
    docker exec -i tests-mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U "$MSSQL_SA_USER" -P "$MSSQL_SA_PASSWORD" -b -i /data/iris_mssql.sql
  else
    MSSQL_NET=$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{println $k}}{{end}}' tests-mssql | head -n1)
    docker run --rm --network "$MSSQL_NET" -v "$DATA_DIR":/data mcr.microsoft.com/mssql-tools /opt/mssql-tools/bin/sqlcmd -S tests-mssql -U "$MSSQL_SA_USER" -P "$MSSQL_SA_PASSWORD" -b -i /data/iris_mssql.sql
  fi
elif command_exists sqlcmd; then
  sqlcmd -S "$MSSQL_HOST,$MSSQL_PORT" -U "$MSSQL_SA_USER" -P "$MSSQL_SA_PASSWORD" -b -i "$DATA_DIR/iris_mssql.sql"
else
  echo "sqlcmd not found and container tests-mssql not running. Skipping MSSQL load." >&2
fi

echo "Done."
