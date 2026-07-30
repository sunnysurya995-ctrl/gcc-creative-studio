# Copyright 2025 Google LLC
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
"""Database migrations runner and locking mechanism."""


import asyncio
import logging
import os
import sys

import asyncpg

from src.database import get_connection, async_session_local

logger = logging.getLogger(__name__)

# A fixed ID for the advisory lock.
# This ensures that all instances of the application will contend for the
# same lock. The ID is arbitrary but must be consistent across all
# instances.
MIGRATION_LOCK_ID = 42


async def run_pending_migrations():
    """Acquires a Postgres advisory lock and runs Alembic migrations.
    This ensures that only one instance runs migrations at a time.
    """
    logger.info("Attempting to run pending database migrations...")

    conn = None
    try:
        # We need a raw connection to execute the lock command and keep it open
        # while the subprocess runs.
        # Using the existing get_connection helper which returns an asyncpg
        # connection wrapped in SQLAlchemy's AsyncConnection if using
        # create_async_engine, BUT get_connection returns the raw asyncpg
        # connection from the Connector?
        # Let's check src/database.py again.
        # get_connection returns `conn` from `connector.connect_async`,
        # which IS an asyncpg connection.

        conn = await get_connection()

        # Acquire advisory lock
        # pg_advisory_lock waits until the lock is available.
        # We use transaction-level lock or session-level?
        # Since we are holding the connection, session-level is fine.
        logger.info("Acquiring advisory lock for migrations...")
        await conn.execute("SELECT pg_advisory_lock($1)", MIGRATION_LOCK_ID)
        logger.info("Advisory lock acquired.")

        # Run Alembic migrations in a subprocess
        # We use subprocess to avoid event loop conflicts with the running app
        # and to ensure a clean environment for Alembic.
        # Resolve alembic executable path relative to the current python
        # interpreter

        alembic_cmd = os.path.join(os.path.dirname(sys.executable), "alembic")

        logger.info("Running '%s upgrade head'...", alembic_cmd)
        process = await asyncio.create_subprocess_exec(
            alembic_cmd,
            "upgrade",
            "head",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            # Combine stdout and stderr to check for migration actions
            # Alembic often logs to stderr by default
            full_output = (stdout.decode() if stdout else "") + (
                stderr.decode() if stderr else ""
            )

            if "Running upgrade" in full_output:
                logger.info("Migrations applied successfully.")
                logger.info("Alembic Output:\n%s", full_output.strip())
            else:
                logger.info(
                    "Database is already up to date. No pending migrations."
                )
                # We can still log the output at debug level if needed, or just
                # skip it to reduce noise
                logger.debug("Alembic Output:\n%s", full_output.strip())
        else:
            logger.error("Migrations failed.")
            if stdout:
                logger.info("Alembic Output:\n%s", stdout.decode().strip())
            if stderr:
                logger.error("Alembic Error:\n%s", stderr.decode().strip())
            # We might want to raise an exception here to stop startup if
            # migrations fail
            raise RuntimeError("Database migrations failed.")

    except Exception as e:
        logger.error("Error during migration process: %s", e)
        raise
    finally:
        if conn:
            try:
                # Release advisory lock
                logger.info("Releasing advisory lock...")
                await conn.execute(
                    "SELECT pg_advisory_unlock($1)", MIGRATION_LOCK_ID
                )
                logger.info("Advisory lock released.")
                await conn.close()
            except asyncpg.PostgresError as e:
                logger.error(
                    "Error releasing lock or closing connection: %s", e
                )


async def ensure_default_workspace():
    """Creates a default public workspace if none exists.
    This is called on startup to ensure the app is usable.
    """
    from src.workspaces.repository.workspace_repository import WorkspaceRepository
    from src.workspaces.schema.workspace_model import (
        WorkspaceModel,
        WorkspaceScopeEnum,
    )
    from src.users.repository.user_repository import UserRepository
    from src.users.user_model import UserRoleEnum, User
    from src.config.config_service import config_service
    from sqlalchemy import select

    logger.info("Checking for default public workspace...")

    try:
        async with async_session_local() as db:
            workspace_repo = WorkspaceRepository(db)

            # Check if a public workspace already exists
            public_workspace = await workspace_repo.get_public_workspace()
            if public_workspace:
                logger.info(
                    f"Public workspace already exists: '{public_workspace.name}' (ID: {public_workspace.id})"
                )
                return

            logger.warning("No public workspace found. Creating a default one...")

            # We need an owner. Try to find any existing user or create a system user.
            user_repo = UserRepository(db)

            # Try to find any user first (use the SQLAlchemy model User, not UserModel)
            stmt = select(User).limit(1)
            result = await db.execute(stmt)
            owner = result.scalar_one_or_none()

            if not owner:
                # Create a system user to own the workspace
                logger.info("No users found. Creating a system user...")

                system_user_data = {
                    "email": "system@creative-studio.local",
                    "name": "System",
                    "roles": [UserRoleEnum.USER.value, UserRoleEnum.ADMIN.value],
                }
                owner = await user_repo.create(system_user_data)
                logger.info(f"Created system user with ID: {owner.id}")

            # Create the default workspace
            project_id = config_service.PROJECT_ID or "creative-studio"
            workspace_name = (
                project_id.replace("-", " ").replace("_", " ").title()
                + " Workspace"
            )

            default_workspace = WorkspaceModel(
                name=workspace_name,
                owner_id=owner.id,
                scope=WorkspaceScopeEnum.PUBLIC,
                members=[],
            )
            created = await workspace_repo.create(default_workspace)
            logger.info(
                f"Default public workspace '{workspace_name}' created successfully (ID: {created.id})."
            )

    except Exception as e:
        logger.error(f"Failed to ensure default workspace exists: {e}", exc_info=True)
