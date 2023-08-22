import json
import os
from typing import Any, Dict, List, Tuple, Type, get_type_hints

import jsonschema
from dagster_externals import ExternalExecutionContextData, Notification
from pydantic import BaseModel, create_model
from typing_extensions import TypedDict, TypeGuard

OUTPUT_FILEPATH = os.path.join(
    os.path.dirname(__file__), "../python_modules/dagster-externals/externals_protocol_schema.json"
)


def main():
    context_schema = create_pydantic_model_from_typeddict(
        ExternalExecutionContextData
    ).model_json_schema()
    notification_schema = create_pydantic_model_from_typeddict(Notification).model_json_schema()
    merged_schema = merge_schemas(context_schema, notification_schema)
    jsonschema.Draft7Validator.check_schema(merged_schema)

    with open(OUTPUT_FILEPATH, "w") as f:
        f.write(json.dumps(merged_schema, indent=2))


def create_pydantic_model_from_typeddict(typed_dict_cls: Type[TypedDict]) -> Type[BaseModel]:
    """Create a Pydantic model from a TypedDict class.

    We use this instead of the Pydantic-provided `create_model_from_typeddict` because we need to
    handle nested `TypedDict`. This funciton will convert any child `TypedDict` to a Pydantic model,
    which is necessary for Pydantic JSON schema generation to work correctly.
    """
    annotations = get_type_hints(typed_dict_cls)
    fields: Dict[str, Tuple[Type, ...]] = {}
    for name, field_type in annotations.items():
        pydantic_type = (
            create_pydantic_model_from_typeddict(field_type)
            if is_typed_dict_class(field_type)
            else field_type
        )
        fields[name] = (pydantic_type, ...)
    return create_model(typed_dict_cls.__name__, **fields)  # type: ignore


def is_typed_dict_class(cls: Type) -> TypeGuard[Type[TypedDict]]:
    return isinstance(cls, type) and issubclass(cls, dict) and get_type_hints(cls) is not None


def merge_schemas(*schemas: Dict[str, Any]) -> Dict[str, Any]:
    """Merge multiple JSON schemas into a single schema with a top-level `oneOf` property.

    This function is necessary because Pydantic does not support merging schemas.
    """
    one_of: List[Any] = []
    defs = {}
    for schema in schemas:
        defs.update(schema.get("$defs", {}))
        defs[schema["title"]] = {k: v for k, v in schema.items() if k != "definitions"}
        one_of.append({"$ref": f"#/$defs/{schema['title']}"})
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "oneOf": one_of,
        "$defs": defs,
    }


if __name__ == "__main__":
    main()
