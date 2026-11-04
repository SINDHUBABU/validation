import re
import re
import xml.etree.ElementTree as ET
from typing import Any

from adt.diagnostics import redact_sensitive_text


def _remove_invalid_xml10_characters(text: str) -> str:
    """Remove characters forbidden by the XML 1.0 specification."""
    return re.sub(
        r"[\x00-\x08\x0B\x0C\x0E-\x1F\uFFFE\uFFFF]",
        "",
        text,
    )

NAMESPACE = {
    "dp": "http://www.sap.com/adt/dataPreview",
}


def parse_data_preview(xml_response: str) -> dict[str, Any]:
    """
    Parse the XML response returned by the SAP ADT data-preview endpoint.

    Returns:
        {
            "total_rows": int,
            "executed_query": str,
            "execution_time": float | None,
            "is_hana_analytical_view": bool,
            "columns": list[dict],
            "rows": list[dict]
        }
    """
    xml_response = _remove_invalid_xml10_characters(xml_response)

    try:
        root = ET.fromstring(xml_response)
    except ET.ParseError as exc:
        raise ValueError(
            f"Invalid SAP data-preview XML: {exc}"
        ) from exc

    def get_text(path: str, default: str = "") -> str:
        element = root.find(path, NAMESPACE)

        if element is None or element.text is None:
            return default

        return element.text.strip()

    total_rows_text = get_text("dp:totalRows")
    if not total_rows_text:
        details = []
        for element in root.iter():
            local_name = element.tag.rsplit("}", 1)[-1].casefold()
            text = (element.text or "").strip()
            if text and any(token in local_name for token in ("error", "message", "exception", "diagnostic")):
                details.append(text)
        if not details:
            details.append(
                "Response omitted totalRows "
                f"(root={root.tag.rsplit('}', 1)[-1]}, "
                f"elements={sum(1 for _ in root.iter())})."
            )
        raise ValueError(
            "SAP data-preview response did not include totalRows. "
            f"Response details: {redact_sensitive_text('; '.join(details))}"
        )
    executed_query = get_text("dp:executedQueryString")
    execution_time_text = get_text("dp:queryExecutionTime")
    analytical_view_text = get_text(
        "dp:isHanaAnalyticalView",
        "false",
    )

    try:
        total_rows = int(total_rows_text)
    except ValueError as exc:
        raise ValueError(
            f"SAP data-preview response returned an invalid totalRows value: {total_rows_text!r}."
        ) from exc

    try:
        execution_time = (
            float(execution_time_text)
            if execution_time_text
            else None
        )
    except ValueError:
        execution_time = None

    is_analytical_view = (
        analytical_view_text.lower() == "true"
    )

    columns = []

    for column_element in root.findall("dp:columns", NAMESPACE):
        metadata_element = column_element.find(
            "dp:metadata",
            NAMESPACE,
        )

        if metadata_element is None:
            continue

        attributes = metadata_element.attrib

        column_name = attributes.get(
            f"{{{NAMESPACE['dp']}}}name",
            "",
        )

        column_type = attributes.get(
            f"{{{NAMESPACE['dp']}}}type",
            "",
        )

        description = attributes.get(
            f"{{{NAMESPACE['dp']}}}description",
            "",
        )

        key_attribute = attributes.get(
            f"{{{NAMESPACE['dp']}}}keyAttribute",
            "false",
        ).lower() == "true"

        is_key_figure = attributes.get(
            f"{{{NAMESPACE['dp']}}}isKeyFigure",
            "false",
        ).lower() == "true"

        dataset = column_element.find(
            "dp:dataSet",
            NAMESPACE,
        )

        values = []

        if dataset is not None:
            for data_element in dataset.findall(
                "dp:data",
                NAMESPACE,
            ):
                # Preserve empty values as None.
                if data_element.text is None:
                    values.append(None)
                else:
                    values.append(data_element.text)

        columns.append({
            "name": column_name,
            "type": column_type,
            "description": description,
            "is_key": key_attribute,
            "is_key_figure": is_key_figure,
            "values": values,
        })

    # Transpose column-oriented XML into row-oriented dictionaries.
    rows = []

    if columns:
        row_count = max(
            len(column["values"])
            for column in columns
        )

        for index in range(row_count):
            row = {}

            for column in columns:
                values = column["values"]

                row[column["name"]] = (
                    values[index]
                    if index < len(values)
                    else None
                )

            rows.append(row)
    elif total_rows:
        raise ValueError(
            "SAP data-preview response reports rows but contains no column metadata."
        )

    return {
        "total_rows": total_rows,
        "executed_query": executed_query,
        "execution_time": execution_time,
        "is_hana_analytical_view": is_analytical_view,
        "columns": [
            {
                key: value
                for key, value in column.items()
                if key != "values"
            }
            for column in columns
        ],
        "rows": rows,
    }
