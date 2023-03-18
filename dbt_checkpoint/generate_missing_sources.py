import argparse
import os
import time
from pathlib import Path
from typing import Any, Dict, FrozenSet, Optional, Sequence

from yaml import dump, safe_dump, safe_load

from dbt_checkpoint.check_script_ref_and_source import check_refs_sources
from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import JsonOpenError, add_default_args, get_json


def create_missing_sources(
    sources: Dict[FrozenSet[str], Dict[str, str]],
    output_path: str
) -> Dict[str, Any]:
    status_code = 0
    if sources:
        updated = False
        path = Path(output_path)
        # is file and exists
        if not path.is_file():
            # create file if it doesn't exist
            path.touch()
            print(f"Target schema file not found at `{output_path}`. Created it.")

        schema = safe_load(path.open())
        schema_sources = schema.get("sources", []) if schema else []

        for _, source in sources.items():
            source_name = source.get("source_name")
            table_name = source.get("table_name")

            matching_sources = list(
                filter(lambda schema_source: schema_source.get("name") == source_name, schema_sources))
            # check if schema_source exists in schema_sources
            is_schema_source_already_declared = len(matching_sources) > 0

            # if schema_source does not exist in schema_sources, add it with this new source
            if not is_schema_source_already_declared:
                print(f"Adding schema source for `{source_name}.{table_name}`.")
                source = {"name": source_name, "tables": [{"name": table_name}]}
                schema_sources.append(source)
                updated = True
            else:
                # if schema_source exists in schema_sources, add the new source to the existing schema_source
                for matching_source in matching_sources:
                    tables = matching_source.setdefault("tables", [])
                    is_table_already_declared = any(
                        table.get("name") == table_name for table in tables)
                    # check if table exists in tables, if so we don't need to add it
                    if is_table_already_declared:
                        continue
                    else:
                        # if table does not exist in tables, add it
                        print(
                            f"Generating missing source `{source_name}.{table_name}`.")
                        tables.append({"name": table_name})
                        updated = True

        if updated:
            # write the updated schema file
            with open(path, "w") as file:
                safe_dump(schema, file, default_flow_style=False, sort_keys=False)
                

    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    parser.add_argument(
        "--schema-file",
        required=True,
        type=str,
        help="""Location of schema.yml file. Where new source tables should
        be created.
        """,
    )
    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    check_refs_sources_properties = check_refs_sources(
        paths=args.filenames, manifest=manifest
    )

    sources = check_refs_sources_properties.get("sources")

    #print(f"sources: {sources}")
    print(f"-----------------")

    hook_properties = create_missing_sources(
        sources, output_path=args.schema_file)
    end_time = time.time()

    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "If any source is missing this hook tries to create it.",
            "status": hook_properties.get("status_code"),
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )
    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
