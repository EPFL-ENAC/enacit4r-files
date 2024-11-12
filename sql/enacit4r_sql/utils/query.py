from sqlmodel import SQLModel, select
from sqlalchemy import func, or_, and_, cast, String
import json


def paramAsDict(param: str):
    return json.loads(param) if param else {}


def paramAsArray(param: str):
    return json.loads(param) if param else []


class QueryBuilder:

    def __init__(self, model: SQLModel, filter: dict, sort: list, range: list, joinModels: dict = {}):
        self.model = model
        self.filter = filter
        self.sort = sort
        self.range = range
        self.joinModels = joinModels

    def build_count_query(self):
        return self._apply_filter(select(func.count(func.distinct(self.model.id))))

    def build_query(self, total_count):
        query_ = self._apply_filter(select(self.model))
        query_ = self._apply_sort(query_)
        return self._apply_range(query_, total_count)

    def build_filter_query(self, query_from):
        query_ = self._apply_filter(query_from)
        return query_

    def _apply_filter(self, query_):
        return self._apply_model_filter(query_, self.model, self.filter)

    def _apply_model_filter(self, query_, model, filter):
        if len(filter):
            for field, value in filter.items():
                if field == "$and":
                    for sub_filter in value:
                        for sub_field, sub_value in sub_filter.items():
                            query_ = self._apply_column_filter(
                                query_, model, sub_field, sub_value)
                elif field in self.joinModels:
                    joinModel = self.joinModels[field]
                    query_ = self._apply_model_filter(query_, joinModel, value)
                else:
                    query_ = self._apply_column_filter(
                        query_, model, field, value)
        return query_

    def _apply_column_filter(self, query_, model, field, value):
        column = getattr(model, field)
        if isinstance(value, list):
            if len(value) == 1 and value[0] is None:
                query_ = self._apply_filter_value(
                    query_, field, column, value[0])
            elif None in value:
                noNoneValues = [v for v in value if v is not None]
                query_ = query_.where(
                    or_(column.is_(None), column.in_(noNoneValues)))
            else:
                query_ = query_.where(column.in_(value))
        else:
            query_ = self._apply_filter_value(
                query_, field, column, value)
        return query_

    def _apply_filter_value(self, query_, field, column, value):
        if field == "id" or field == "identifier" or isinstance(value, int):
            query_ = query_.where(column == value)
        elif value is None:
            query_ = query_.where(column.is_(None))
        elif isinstance(value, dict):
            query_ = self._apply_filter_object(query_, field, column, value)
        else:
            query_ = query_.where(column.ilike(f"%{value}%"))
        return query_

    def _apply_filter_object(self, query_, field, column, value):
        if '$exists' in value:
            if value['$exists']:
                query_ = query_.where(
                    and_(column.isnot(None), cast(column, String) != 'null'))
            elif not value['$exists']:
                query_ = query_.where(
                    or_(column.is_(None), cast(column, String) == 'null'))

        if '$ge' in value:
            query_ = query_.where(column >= value['$ge'])
        if '$gte' in value:
            query_ = query_.where(column >= value['$gte'])
        if '$gt' in value:
            query_ = query_.where(column > value['$gt'])

        if '$le' in value:
            query_ = query_.where(column <= value['$le'])
        if '$lte' in value:
            query_ = query_.where(column <= value['$lte'])
        if '$lt' in value:
            query_ = query_.where(column < value['$lt'])

        if '$in' in value:
            query_ = query_.where(column.in_(value['$in']))
        if '$nin' in value:
            query_ = query_.where(column.notin_(value['$nin']))

        if '$eq' in value:
            query_ = query_.where(column == value['$eq'])
        if '$ne' in value:
            query_ = query_.where(column != value['$ne'])

        if '$like' in value:
            query_ = query_.where(column.like(f"%{value['$like']}%"))
        if '$ilike' in value:
            query_ = query_.where(column.ilike(f"%{value['$ilike']}%"))
        if '$contains' in value:
            query_ = query_.where(column.contains(value['$contains']))

        return query_

    def _apply_sort(self, query_):
        if len(self.sort) == 2:
            sort_field, sort_order = self.sort
            attr = getattr(self.model, sort_field)
            if sort_order == "ASC":
                query_ = query_.order_by(attr)
            else:
                query_ = query_.order_by(attr.desc())
        return query_

    def _apply_range(self, query_, total_count):
        if len(self.range) == 2 and self.range[1] >= 0:
            start, end = self.range
            query_ = query_.offset(start).limit(end - start + 1)
            return start, end, query_
        else:
            return 0, total_count, query_
