# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from typing import TYPE_CHECKING

from flask import g
from sqlalchemy import func, select, case

from airflow.api_connexion import security
from airflow.api_connexion.schemas.dag_stats_schema import (
    dag_stats_collection_schema,
)
from airflow.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_connexion.parameters import check_limit, format_parameters
from airflow.models.dag import DagRun
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import DagRunState
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.api_connexion.types import APIResponse


@security.requires_access_dag("GET", DagAccessEntity.RUN)
@format_parameters({"limit": check_limit})
@provide_session
def get_dag_stats(*, dag_ids: str | None = None, offset: int | None = None, limit: int | None = None, session: Session = NEW_SESSION) -> APIResponse:
    """Get Dag statistics."""
    allowed_dag_ids = get_auth_manager().get_permitted_dag_ids(methods=["GET"], user=g.user)
    if dag_ids:
        dags_list = set(dag_ids.split(","))
        filter_dag_ids = dags_list.intersection(allowed_dag_ids)
    else:
        filter_dag_ids = allowed_dag_ids

    query = (
        select(
            DagRun.dag_id,
            *[func.sum(case([
                (DagRun.state == state, 1),
            ], else_= 0)).label(state) for state in DagRunState
            ]
        )
        .group_by(DagRun.dag_id)
        .where(DagRun.dag_id.in_(filter_dag_ids))
        .order_by(DagRun.dag_id)
    )

    dag_state_stats = session.execute(query.offset(offset).limit(limit)).all()

    dag_state_data = [row._asdict() for row in dag_state_stats]
    dag_stats = {}
    for dag in dag_state_data:
        dag_stats[dag["dag_id"]] = [{"state": state, "count": dag[state]} for state in DagRunState]
    dags = [{"dag_id": stat, "stats": dag_stats[stat]} for stat in dag_stats]
    return dag_stats_collection_schema.dump({"dags": dags, "total_entries": len(filter_dag_ids)})
