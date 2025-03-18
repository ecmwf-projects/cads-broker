"""module for entry points."""

# Copyright 2023, European Union.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sqlalchemy as sa

import alembic.context
import cads_broker


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to alembic.context.execute() here emit the given string to the
    script output.
    """
    url = alembic.context.config.get_main_option("sqlalchemy.url")
    alembic.context.configure(
        url=url,
        target_metadata=cads_broker.database.BaseModel.metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with alembic.context.begin_transaction():
        alembic.context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the alembic.context.
    """
    url = alembic.context.config.get_main_option("sqlalchemy.url")
    engine = sa.create_engine(url, poolclass=sa.pool.NullPool)  # type: ignore
    with engine.connect() as connection:
        alembic.context.configure(
            connection=connection,
            target_metadata=cads_broker.database.BaseModel.metadata,
        )

        with alembic.context.begin_transaction():
            alembic.context.run_migrations()


if alembic.context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
