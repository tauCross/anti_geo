#!/usr/bin/env python3

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent.parent
ENTITY_SCHEMA_PATH = ROOT_DIR / "schemas" / "entity.schema.json"
REGISTRY_FILES = [
    ROOT_DIR / "registry" / "watchlist.json",
    ROOT_DIR / "registry" / "restricted.json",
    ROOT_DIR / "registry" / "blocked.json",
]
FULL_INDEX_PATH = ROOT_DIR / "registry" / "full-index.json"
ENTITIES_COMPACT_PATH = ROOT_DIR / "registry" / "entities.compact.json"
MANIFEST_PATH = ROOT_DIR / "registry" / "manifest.json"
TAG_TOPIC_INDEX_PATH = ROOT_DIR / "registry" / "tag-topic-to-entities.json"
TAG_INTENT_INDEX_PATH = ROOT_DIR / "registry" / "tag-intent-to-entities.json"
TAG_RISK_INDEX_PATH = ROOT_DIR / "registry" / "tag-risk-to-entities.json"
DOMAIN_INDEX_PATH = ROOT_DIR / "registry" / "domain-to-entities.json"
NAME_INDEX_PATH = ROOT_DIR / "registry" / "name-to-entities.json"

REQUIRED_COMPACT_FIELDS = [
    "id",
    "name",
    "entity_type",
    "current_status",
]
REQUIRED_MANIFEST_FILES = {
    "watchlist": "registry/watchlist.json",
    "restricted": "registry/restricted.json",
    "blocked": "registry/blocked.json",
    "full_index": "registry/full-index.json",
    "entities_compact": "registry/entities.compact.json",
    "tag_topic_to_entities": "registry/tag-topic-to-entities.json",
    "tag_intent_to_entities": "registry/tag-intent-to-entities.json",
    "tag_risk_to_entities": "registry/tag-risk-to-entities.json",
    "domain_to_entities": "registry/domain-to-entities.json",
    "name_to_entities": "registry/name-to-entities.json",
}


def load_json(path: Path) -> Any:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path.relative_to(ROOT_DIR)}")

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON in {path.relative_to(ROOT_DIR)}: {exc.msg} "
            f"(line {exc.lineno}, column {exc.colno})"
        ) from exc


def format_error_path(error: Any) -> str:
    if not error.path:
        return "<root>"
    return ".".join(str(part) for part in error.path)


def expect_object(data: Any, source_name: str) -> dict[str, Any] | None:
    if not isinstance(data, dict):
        print(f"ERROR: {source_name}: expected an object")
        return None
    return data


def expect_array(data: Any, source_name: str, label: str) -> list[Any] | None:
    if not isinstance(data, list):
        print(f"ERROR: {source_name}: expected {label}")
        return None
    return data


def collect_ids(
    records: list[Any],
    source_name: str,
    id_field: str = "id",
) -> tuple[set[str], bool]:
    record_ids: set[str] = set()
    has_error = False

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            print(f"ERROR: {source_name}: index={index}: expected object")
            has_error = True
            continue

        record_id = record.get(id_field)
        if not isinstance(record_id, str) or not record_id.strip():
            print(f"ERROR: {source_name}: index={index}: missing or invalid {id_field}")
            has_error = True
            continue

        normalized_id = record_id.strip()
        if normalized_id in record_ids:
            print(f"ERROR: {source_name}: duplicate {id_field}: {normalized_id}")
            has_error = True
            continue

        record_ids.add(normalized_id)

    return record_ids, not has_error


def collect_entity_ids(entities: list[Any], source_name: str) -> tuple[set[str], bool]:
    return collect_ids(entities, source_name, "id")


def validate_entities(
    entities: list[Any], validator: Draft202012Validator, source_name: str
) -> bool:
    if not isinstance(entities, list):
        print(f"ERROR: {source_name}: expected an array of entities")
        return False

    has_error = False

    for index, entity in enumerate(entities):
        entity_id = "<unknown>"
        if isinstance(entity, dict):
            entity_id = str(entity.get("id", "<unknown>"))

        errors = sorted(validator.iter_errors(entity), key=lambda err: list(err.path))
        for error in errors:
            has_error = True
            path_str = format_error_path(error)
            print(
                f"ERROR: {source_name}: index={index} id={entity_id} "
                f"path={path_str}: {error.message}"
            )

    if not has_error:
        print(f"OK: {source_name}")

    return not has_error


def validate_compact_entities(
    data: Any,
    expected_entity_ids: set[str],
    source_name: str,
) -> bool:
    entities = expect_array(data, source_name, "a compact entities array")
    if entities is None:
        return False

    has_error = False

    for index, entity in enumerate(entities):
        if not isinstance(entity, dict):
            print(f"ERROR: {source_name}: index={index}: expected object")
            has_error = True
            continue

        for field in REQUIRED_COMPACT_FIELDS:
            value = entity.get(field)
            if not isinstance(value, str) or not value.strip():
                print(
                    f"ERROR: {source_name}: index={index}: missing or invalid field: {field}"
                )
                has_error = True

    compact_ids, ids_ok = collect_entity_ids(entities, source_name)
    has_error = has_error or not ids_ok

    if compact_ids != expected_entity_ids:
        missing_ids = sorted(expected_entity_ids - compact_ids)
        extra_ids = sorted(compact_ids - expected_entity_ids)
        if missing_ids:
            print(
                f"ERROR: {source_name}: missing ids from full-index.json: "
                f"{', '.join(missing_ids)}"
            )
        if extra_ids:
            print(
                f"ERROR: {source_name}: unknown ids not present in full-index.json: "
                f"{', '.join(extra_ids)}"
            )
        has_error = True

    if not has_error:
        print(f"OK: {source_name}")

    return not has_error


def validate_manifest(
    data: Any,
    entity_count: int,
    counts_by_status: dict[str, int],
    source_name: str,
) -> bool:
    manifest = expect_object(data, source_name)
    if manifest is None:
        return False

    has_error = False

    for field in ("version", "generated_at", "entity_count", "counts_by_status", "files"):
        if field not in manifest:
            print(f"ERROR: {source_name}: missing field: {field}")
            has_error = True

    if has_error:
        return False

    if manifest["entity_count"] != entity_count:
        print(
            f"ERROR: {source_name}: entity_count={manifest['entity_count']} "
            f"does not match full-index count={entity_count}"
        )
        has_error = True

    manifest_counts = manifest.get("counts_by_status")
    if not isinstance(manifest_counts, dict):
        print(f"ERROR: {source_name}: counts_by_status: expected object")
        has_error = True
    else:
        for status_name, expected_count in counts_by_status.items():
            actual_count = manifest_counts.get(status_name)
            if actual_count != expected_count:
                print(
                    f"ERROR: {source_name}: counts_by_status.{status_name}={actual_count!r} "
                    f"does not match expected value {expected_count}"
                )
                has_error = True

    files = manifest.get("files")
    if not isinstance(files, dict):
        print(f"ERROR: {source_name}: files: expected object")
        has_error = True
    else:
        for key, expected_path in REQUIRED_MANIFEST_FILES.items():
            actual_path = files.get(key)
            if actual_path != expected_path:
                print(
                    f"ERROR: {source_name}: files.{key}={actual_path!r} "
                    f"does not match expected path {expected_path!r}"
                )
                has_error = True

    if not has_error:
        print(f"OK: {source_name}")

    return not has_error


def validate_index_map(
    data: Any,
    source_name: str,
    valid_entity_ids: set[str],
) -> bool:
    index_map = expect_object(data, source_name)
    if index_map is None:
        return False

    has_error = False

    for key, value in sorted(index_map.items()):
        if not isinstance(value, list):
            print(f"ERROR: {source_name}: key={key!r}: expected array of entity ids")
            has_error = True
            continue

        for item in value:
            if not isinstance(item, str):
                print(
                    f"ERROR: {source_name}: key={key!r}: expected string entity id, "
                    f"got {type(item).__name__}"
                )
                has_error = True
                continue

            if item not in valid_entity_ids:
                print(
                    f"ERROR: {source_name}: key={key!r}: unknown entity id: {item}"
                )
                has_error = True

    if not has_error:
        print(f"OK: {source_name}")

    return not has_error


def main() -> int:
    try:
        from jsonschema import Draft202012Validator
    except ImportError:
        print("ERROR: Missing dependency: jsonschema")
        print("Install it with: python3 -m pip install jsonschema")
        return 1

    try:
        entity_schema = load_json(ENTITY_SCHEMA_PATH)
        validator = Draft202012Validator(entity_schema)
    except (FileNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}")
        return 1

    all_ok = True
    counts_by_status: dict[str, int] = {}

    for path in REGISTRY_FILES:
        try:
            entities = load_json(path)
        except (FileNotFoundError, ValueError) as exc:
            print(f"ERROR: {exc}")
            return 1

        entity_list = expect_array(entities, str(path.relative_to(ROOT_DIR)), "an array of entities")
        if entity_list is None:
            return 1

        counts_by_status[path.stem] = len(entity_list)
        all_ok = validate_entities(
            entity_list, validator, str(path.relative_to(ROOT_DIR))
        ) and all_ok

    try:
        full_index = load_json(FULL_INDEX_PATH)
    except (FileNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}")
        return 1

    if not isinstance(full_index, dict):
        print("ERROR: registry/full-index.json: expected an object")
        return 1

    entities = full_index.get("entities")
    if not isinstance(entities, list):
        print("ERROR: registry/full-index.json: missing entities array")
        return 1

    all_ok = validate_entities(
        entities, validator, "registry/full-index.json entities"
    ) and all_ok

    full_index_ids, full_index_ids_ok = collect_entity_ids(
        entities, "registry/full-index.json entities"
    )
    all_ok = full_index_ids_ok and all_ok

    try:
        compact_entities = load_json(ENTITIES_COMPACT_PATH)
        manifest = load_json(MANIFEST_PATH)
        tag_topic_index = load_json(TAG_TOPIC_INDEX_PATH)
        tag_intent_index = load_json(TAG_INTENT_INDEX_PATH)
        tag_risk_index = load_json(TAG_RISK_INDEX_PATH)
        domain_index = load_json(DOMAIN_INDEX_PATH)
        name_index = load_json(NAME_INDEX_PATH)
    except (FileNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}")
        return 1

    all_ok = (
        validate_compact_entities(
            compact_entities,
            expected_entity_ids=full_index_ids,
            source_name="registry/entities.compact.json",
        )
        and all_ok
    )
    all_ok = (
        validate_manifest(
            manifest,
            entity_count=len(entities),
            counts_by_status=counts_by_status,
            source_name="registry/manifest.json",
        )
        and all_ok
    )
    all_ok = (
        validate_index_map(
            tag_topic_index,
            "registry/tag-topic-to-entities.json",
            full_index_ids,
        )
        and all_ok
    )
    all_ok = (
        validate_index_map(
            tag_intent_index,
            "registry/tag-intent-to-entities.json",
            full_index_ids,
        )
        and all_ok
    )
    all_ok = (
        validate_index_map(
            tag_risk_index,
            "registry/tag-risk-to-entities.json",
            full_index_ids,
        )
        and all_ok
    )
    all_ok = (
        validate_index_map(
            domain_index,
            "registry/domain-to-entities.json",
            full_index_ids,
        )
        and all_ok
    )
    all_ok = (
        validate_index_map(
            name_index,
            "registry/name-to-entities.json",
            full_index_ids,
        )
        and all_ok
    )

    if not all_ok:
        print("Validation failed.")
        return 1

    print("All entity registry and release files passed validation.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
