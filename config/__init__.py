import os


class PostgresReadWriteConfig:

    def __init__(self):
        self.username = os.environ["POSTGRES_SHOALCREEK_READWRITE_USERNAME"]
        self.password = os.environ["POSTGRES_SHOALCREEK_READWRITE_PASSWORD"]
        self.conn_str = f"""postgresql://hydro-gis-postgres-scentralus-production.postgres.database.azure.com:5432
        /?user={self.username}&password={self.password}&sslmode=require&database=shoal_creek_wq_bio_mitigation"""

    def __str__(self):
        return self.conn_str


class PostgresReadOnlyConfig:

    def __init__(self):
        self.username = os.environ["POSTGRES_SHOALCREEK_READONLY_USERNAME"]
        self.password = os.environ["POSTGRES_SHOALCREEK_READONLY_PASSWORD"]
        self.conn_str = f"""postgresql://hydro-gis-postgres-scentralus-production.postgres.database.azure.com:5432
        /?user={self.username}&password={self.password}&sslmode=require&database=shoal_creek_wq_bio_mitigation"""

    def __str__(self):
        return self.conn_str
