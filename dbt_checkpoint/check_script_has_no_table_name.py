import argparse
import os
import re
import time
from pathlib import Path
from typing import Generator, Optional, Sequence, Set, Tuple
import sqlfluff
from sqlfluff.core import FluffConfig


from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    get_dbt_manifest,
    red,
    yellow,
)

REGEX_COMMENTS = r"(?<=(\/\*|\{#))((.|[\r\n])+?)(?=(\*+\/|#\}))|[ \t]*--.*"
REGEX_SPLIT = r"[\s]+"
IGNORE_WORDS = ["", "(", "{{", "{"]  # pragma: no mutate
REGEX_PARENTHESIS = r"([\(\)])"  # pragma: no mutate
REGEX_BRACES = r"([\{\}])"  # pragma: no mutate


def prev_cur_next_iter(
    sql: Sequence[str],
) -> Generator[Tuple[Optional[str], str, Optional[str]], None, None]:
    sql_iter = iter(sql)
    prev = None
    cur = next(sql_iter).lower()
    try:
        while True:
            nxt = next(sql_iter).lower()  # pragma: no mutate
            yield prev, cur, nxt
            prev = cur
            cur = nxt
    except StopIteration:
        yield prev, cur, None


def replace_comments(sql: str) -> str:
    return re.sub(REGEX_COMMENTS, "", sql)


def add_space_to_parenthesis(sql: str) -> str:
    return re.sub(REGEX_PARENTHESIS, r" \1 ", sql)


def add_space_to_braces(sql: str) -> str:
    return re.sub(REGEX_BRACES, r" \1 ", sql)


def add_space_to_source_ref(sql: str) -> str:
    return sql.replace("{{", "{{ ").replace("}}", " }}")


def has_table_name(
    sql: str, filename: str, dotless: Optional[bool] = False, dialect: Optional[str] = "ansi"
) -> Tuple[int, Set[str]]:
    status_code = 0
    config = FluffConfig(overrides={
        "dialect": dialect,
        # "ignore_templated_areas": True,
        "templater": "dbt",
    })
    parsed_sql = sqlfluff.parse(sql, config=config)
    table_names = set(parsed_sql.tree.get_table_references())
    return status_code, table_names


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    parser.add_argument("--ignore-dotless-table", action="store_true")
    parser.add_argument("--dialect", type=str, default="ansi")

    args = parser.parse_args(argv)
    status_code = 0

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    script_args = vars(args)

    start_time = time.time()
    for filename in args.filenames:
        sql = Path(filename).read_text()
        status_code_file, tables = has_table_name(
            sql, filename, args.ignore_dotless_table
        )
        if status_code_file:
            result = "\n- ".join(list(tables))  # pragma: no mutate
            print(
                f"{red(filename)}: "
                f"does not use source() or ref() macros for tables:\n",
                f"- {yellow(result)}",
            )
            status_code = status_code_file

    end_time = time.time()

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check the script has no table name (is not using source() or ref() macro for all tables).",  # pragma: no mutate
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
