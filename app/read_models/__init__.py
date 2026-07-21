"""Materialized read models built exclusively from local MySQL facts."""

from app.read_models.materialization import LocalReadModelMaterializer

__all__ = ["LocalReadModelMaterializer"]
