import operator
import re
from typing import Any

class AgentFilter:
    # Supported operators for DSL expressions
    OPERATORS = {
        "<": operator.lt,
        "<=": operator.le,
        ">": operator.gt,
        ">=": operator.ge,
        "==": operator.eq,
        "!=": operator.ne,
    }

    @staticmethod
    def parse_dsl_filter(expr: str):
        """
        Parses a DSL expression like 'price < 600' into a lambda function.
        """
        pattern = r'^\s*(\w+)\s*(<=|>=|==|!=|<|>)\s*(.+?)\s*$'
        match = re.match(pattern, expr)
        if not match:
            raise ValueError(f"Invalid DSL filter: {expr}")
        key, op, value = match.groups()

        # Evaluate value safely (e.g. numbers, strings, bools)
        try:
            value = eval(value, {"__builtins__": {}})
        except Exception:
            value = value.strip('"').strip("'")

        return lambda item: AgentFilter.OPERATORS[op](item.get(key), value)


    def filter_json(self, data: Any, projection: Any) -> Any:
        """
        Filters a JSON-like structure based on a flexible projection.

        Supports:
        - Field inclusion (True)
        - Field exclusion ("!key": True)
        - Wildcards ("*": True)
        - Callable predicates
        - DSL string filters (e.g. "price < 600")
        - $filter: lambda or DSL string for list filtering
        - $sort_by: field name for sorting
        - $reverse: reverse the sort
        - $limit: number of list items to include
        - $min / $max: find item with min/max value by key
        - items: nested projection for list elements
        """
        if isinstance(projection, dict):
            if isinstance(data, list):
                # $filter
                filter_func = projection.get("$filter")
                if isinstance(filter_func, str):
                    filter_func = self.parse_dsl_filter(filter_func)
                if callable(filter_func):
                    data = list(filter(filter_func, data))

                # $sort_by
                if "$sort_by" in projection:
                    key = projection["$sort_by"]
                    data = sorted(data, key=lambda x: x.get(key))
                    if projection.get("$reverse", False):
                        data.reverse()

                # $min / $max
                if "$min" in projection:
                    key = projection["$min"]
                    data = [min(data, key=lambda x: x.get(key))]
                elif "$max" in projection:
                    key = projection["$max"]
                    data = [max(data, key=lambda x: x.get(key))]

                # $limit
                if "$limit" in projection:
                    data = data[:projection["$limit"]]

                # items
                item_proj = projection.get("items", {})
                return [self.filter_json(item, item_proj) for item in data]

            # Normal object projection
            result = {}
            for key, proj_value in projection.items():
                if key in ["$filter", "$sort_by", "$reverse", "$limit", "$min", "$max", "items"]:
                    continue
                if key.startswith("!"):
                    continue
                if key == "*":
                    if isinstance(data, dict):
                        result.update({
                            k: v for k, v in data.items() if f"!{k}" not in projection
                        })
                    continue
                if key in data:
                    result[key] = self.filter_json(data[key], proj_value)
            return result

        elif isinstance(projection, list) and isinstance(data, list):
            return [self.filter_json(item, projection[0]) for item in data]

        elif projection is True:
            return data

        elif callable(projection):
            return data if projection(data) else None

        return None


if __name__ == '__main__':
    
    #USAGE
    data = {
        "flights": [
            {"price": 500, "airline": "A"},
            {"price": 300, "airline": "B"},
            {"price": 700, "airline": "C"},
            {"price": 400, "airline": "D"}
        ]
    }

    projection = {
        "flights": {
            "$filter": "price < 600",
            "$sort_by": "price",
            "$limit": 2,
            "items": {
                "price": True,
                "airline": True
            }
        }
    }
    
    projection_looking_for_min_price = {
        "flights": {
            "$min": "price",
            "items": {
                "price": True,
                "airline": True
            }
        }
    }

    AGF = AgentFilter()
    filtered = AGF.filter_json(data, projection)
    print(filtered)
    