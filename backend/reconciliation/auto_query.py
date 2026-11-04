"""
Build parameterized ABAP Open SQL from CDS metadata alone.

No hardcoded field names.
No hardcoded parameter values.
Everything is derived from the CDS DDL and a small config.
"""

from datetime import date, datetime, timedelta


# ---------------------------------------------------------------------
# Environment system-field resolution
# ---------------------------------------------------------------------
#
# Maps @Environment.systemField values to callables that produce a
# literal-ready value at runtime. Extend as needed.
#

def _today_yyyymmdd():
    return date.today().strftime("%Y%m%d")


def _now_hhmmss():
    return datetime.now().strftime("%H%M%S")


def _week_begin():
    today = date.today()
    return (today - timedelta(days=today.weekday())).strftime("%Y%m%d")


def _week_end():
    today = date.today()
    return (today + timedelta(days=6 - today.weekday())).strftime("%Y%m%d")


def _month_begin():
    today = date.today()
    return today.replace(day=1).strftime("%Y%m%d")


def _month_end():
    today = date.today()
    next_month = (today.replace(day=28) + timedelta(days=4)).replace(day=1)
    return (next_month - timedelta(days=1)).strftime("%Y%m%d")


def _year_begin():
    return date.today().replace(month=1, day=1).strftime("%Y%m%d")


def _year_end():
    return date.today().replace(month=12, day=31).strftime("%Y%m%d")


SYSTEM_FIELD_RESOLVERS = {
    "#SYSTEM_DATE":       _today_yyyymmdd,
    "#USER_DATE":         _today_yyyymmdd,
    "#SYSTEM_TIME":       _now_hhmmss,
    "#WEEK_BEGIN_DATE":   _week_begin,
    "#WEEK_END_DATE":     _week_end,
    "#MONTH_BEGIN_DATE":  _month_begin,
    "#MONTH_END_DATE":    _month_end,
    "#YEAR_BEGIN_DATE":   _year_begin,
    "#YEAR_END_DATE":     _year_end,
}


# ---------------------------------------------------------------------
class ParameterResolutionError(ValueError):
    """Raised when a required parameter cannot be resolved automatically."""


# ---------------------------------------------------------------------
# Value formatting (mirrors DataRetriever._format_parameter_value)
# ---------------------------------------------------------------------

def _format_literal(value):
    if isinstance(value, bool) or value is None:
        raise ValueError("Cannot format None or bool as SQL literal.")

    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

    if isinstance(value, int):
        return str(value)

    raise ValueError(f"Unsupported literal type: {type(value).__name__}")


# ---------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------

def resolve_parameter_value(param, overrides=None):
    """
    Resolve a single parameter's value from (in order):
      1. Explicit overrides dict (case-insensitive key match)
      2. @Environment.systemField annotation
      3. @Consumption.defaultValue annotation
      4. Raise ParameterResolutionError. Parameter names are never used
         to invent values.
    """
    overrides = overrides or {}
    name_upper = param["name"].upper()

    # 1. Explicit override
    for k, v in overrides.items():
        if k.upper() == name_upper:
            return v

    annotations = param.get("annotations", {})

    # 2. Environment system field
    env_anno = annotations.get("@Environment.systemField")
    if env_anno:
        env_anno = env_anno.strip()
        resolver = SYSTEM_FIELD_RESOLVERS.get(env_anno)
        if resolver:
            return resolver()

    # 3. Consumption default
    default_anno = annotations.get("@Consumption.defaultValue")
    if default_anno:
        raw = default_anno.strip().strip("'\"")
        # Try to coerce to int if it looks numeric
        if raw.isdigit():
            return int(raw)
        return raw

    # No parameter-name guessing: a value must come from an explicit
    # override or verified CDS annotation.
    raise ParameterResolutionError(
        f"Cannot auto-resolve value for parameter '{param['name']}' "
        f"(type={param['type']}). "
        f"Annotations: {annotations or 'none'}. "
        f"Provide an explicit override or declare a CDS default/system-field annotation."
    )


def resolve_all_parameters(metadata, overrides=None):
    """
    Resolve every parameter declared by the view.
    Returns an ordered dict {param_name: value}.
    """
    resolved = {}
    for param in metadata["parameters"]:
        resolved[param["name"]] = resolve_parameter_value(param, overrides)
    return resolved


# ---------------------------------------------------------------------
# Query builder
# ---------------------------------------------------------------------

def build_select(view_name, metadata, overrides=None, field_selection=None):
    """
    Build a parameterized ABAP Open SQL SELECT from metadata alone.

    Parameters
    ----------
    view_name : str
    metadata : dict
        Output of CDSMetadataClient.discover_metadata().
    overrides : dict, optional
        Caller-supplied parameter values.
    field_selection : list[str], optional
        If None, ALL fields from metadata are selected.

    Returns
    -------
    sql : str
    resolved_params : dict
    """
    # --- field selection ------------------------------------------------
    if field_selection:
        # Validate against metadata
        declared = {f["name"].upper() for f in metadata["fields"]}
        for f in field_selection:
            if f.upper() not in declared:
                raise ValueError(
                    f"Field '{f}' is not declared in {view_name}. "
                    f"Available: {sorted(declared)}"
                )
        select_fields = field_selection
    else:
        # Select every declared field
        select_fields = [f["name"] for f in metadata["fields"]]

    if not select_fields:
        raise ValueError(f"No selectable fields discovered for {view_name}.")

    # --- parameter resolution ------------------------------------------
    resolved_params = resolve_all_parameters(metadata, overrides)

    # --- source construction -------------------------------------------

    if resolved_params:
        # Parameterized CDS queries use qualified field names.
        qualified_fields = ",\n    ".join(
            f"{view_name}~{field}"
            for field in select_fields
        )

        assignments = ",\n    ".join(
            f"{name} = {_format_literal(value)}"
            for name, value in resolved_params.items()
        )

        source = (
            f"{view_name}(\n"
            f"    {assignments}\n"
            f")"
        )

    else:
        qualified_fields = ",\n    ".join(select_fields)
        source = view_name

    sql = (
        f"SELECT\n"
        f"    {qualified_fields}\n"
        f"FROM {source}"
    )

    # Catch overlong lines before sending the query to SAP.
    longest_line = max(len(line) for line in sql.splitlines())

    if longest_line > 255:
        raise ValueError(
            f"Generated query contains a line of {longest_line} "
            "characters; maximum allowed is 255."
        )

    return sql, resolved_params
