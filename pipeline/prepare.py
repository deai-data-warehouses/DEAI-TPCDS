import argparse
import re
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from time import perf_counter

from constants import (
    QUERY_TEMPLATES_DIR,
    SCENARIOS_DIR,
    SQL_SCHEMA_PATH,
    STAGED_TPCDS_DIR,
    TPCDS_TOOL_DIR,
    STAGED_TOOLS_DIR,
    get_scenario_paths,
)

from utils import get_table_names


def _patch_function_signature(source_file: Path, name: str, old_args: str, new_args: str) -> None:
    source = source_file.read_text()
    definition = re.compile(
        rf"(?m)^([ \t]*[^\r\n]*?\b{re.escape(name)}\s*\()"
        rf"([^\r\n)]*)(\)[ \t]*(?:\r?\n[ \t]*)?\{{)"
    )
    match = definition.search(source)
    if match:
        arguments = match.group(2).strip()
        if arguments == new_args:
            return
        if arguments != old_args:
            raise RuntimeError(
                f"Unexpected parameters for {name} in {source_file}: {arguments}"
            )
        source = (
            source[: match.start(2)]
            + new_args
            + source[match.end(2) :]
        )
        source_file.write_text(source)
        return

    if re.search(rf"\b{re.escape(name)}\s*\(\s*{re.escape(new_args)}\s*\)", source):
        return
    raise RuntimeError(f"Could not find the {name} definition in {source_file}")


def _patch_makefile(makefile: Path) -> None:
    contents = makefile.read_text()
    setting = re.compile(r"(?m)^(\s*LINUX_CFLAGS\s*=\s*)(.*)$")
    match = setting.search(contents)
    if not match:
        raise RuntimeError(f"Could not find LINUX_CFLAGS in {makefile}")
    flags = match.group(2).split()
    if "-fcommon" not in flags:
        flags.append("-fcommon")
        contents = contents[: match.start(2)] + " ".join(flags) + contents[match.end(2) :]
        makefile.write_text(contents)


def _patch_sources(tools_dir: Path) -> None:
    signatures = (
        ("date.c", "getDateWeightFromJulian", "jDay, nDistribution", "int jDay, int nDistribution"),
        ("tdefs.c", "getSimpleTdefsByNumber", "nTable", "int nTable"),
        ("tdef_functions.c", "getTdefFunctionsByNumber", "nTable", "int nTable"),
    )
    for filename, name, old_args, new_args in signatures:
        _patch_function_signature(tools_dir / filename, name, old_args, new_args)

    makefiles = [tools_dir / name for name in ("Makefile", "makefile")]
    existing_makefiles = [path for path in makefiles if path.is_file()]
    if not existing_makefiles:
        raise FileNotFoundError(f"No Makefile or makefile found in {tools_dir}")
    for makefile in existing_makefiles:
        _patch_makefile(makefile)


def _patch_netezza_template(template: Path) -> None:
    contents = template.read_text()
    definitions = re.findall(r"(?m)^\s*define\s+_END\s*=\s*(.*?)\s*;\s*$", contents)
    if definitions:
        if definitions[-1] != '""':
            raise RuntimeError(f"Unexpected _END definition in {template}")
        return
    with template.open("a") as stream:
        if contents and not contents.endswith("\n"):
            stream.write("\n")
        stream.write('define _END = "";\n')


def _stage_tool_source(source_dir: Path) -> None:
    tools_source = source_dir / "tools"
    templates_source = source_dir / "query_templates"
    if not tools_source.is_dir() or not templates_source.is_dir():
        raise FileNotFoundError(
            f"{source_dir} must contain both 'tools' and 'query_templates' directories"
        )
    if not (tools_source / "tpcds.sql").is_file():
        raise FileNotFoundError(f"TPC-DS schema not found: {tools_source / 'tpcds.sql'}")

    Path(STAGED_TPCDS_DIR).mkdir(parents=True, exist_ok=True)
    shutil.copytree(tools_source, STAGED_TOOLS_DIR, dirs_exist_ok=True)
    shutil.copytree(templates_source, QUERY_TEMPLATES_DIR, dirs_exist_ok=True)


def _expected_query_names() -> list[str]:
    templates = sorted(Path(QUERY_TEMPLATES_DIR).glob("query*.tpl"))
    if not templates:
        raise FileNotFoundError(f"No query*.tpl templates found in {QUERY_TEMPLATES_DIR}")
    return [f"{template.stem}.sql" for template in templates]


def _raw_data_complete(table_names: list[str], data_dir: Path) -> bool:
    return (data_dir / ".complete").is_file() and _raw_data_files_present(
        table_names, data_dir
    )

def _raw_data_files_present(table_names: list[str], data_dir: Path) -> bool:
    return all(
        (data_dir / f"{table_name}.dat").is_file()
        and (data_dir / f"{table_name}.dat").stat().st_size > 0
        for table_name in table_names
    )


def _queries_complete(query_names: list[str], output_dir: Path) -> bool:
    return (output_dir / ".complete").is_file() and _query_files_present(
        output_dir, query_names
    )


def _query_files_present(output_dir: Path, query_names: list[str]) -> bool:
    return all(
        (output_dir / name).is_file() and (output_dir / name).stat().st_size > 0
        for name in query_names
    )


def _generate_queries(
    query_names: list[str], output_dir: Path, scale_factor: float
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / ".complete").unlink(missing_ok=True)
    for old_query in output_dir.glob("query*.sql"):
        old_query.unlink()

    script = Path(__file__).resolve().parent.parent / "scripts" / "generate_queries.sh"
    subprocess.run(
        [
            "bash",
            str(script),
            "--tpcds-tools-dir",
            STAGED_TPCDS_DIR,
            "--output-dir",
            str(output_dir),
            "--scale",
            str(scale_factor),
        ],
        check=True,
    )

    if not _query_files_present(output_dir, query_names):
        missing = [
            name for name in query_names
            if not (output_dir / name).is_file()
            or (output_dir / name).stat().st_size == 0
        ]
        raise RuntimeError(f"Query generation did not produce expected files: {missing}")
    (output_dir / ".complete").touch()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build TPC-DS tools and generate raw data and benchmark queries."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="rebuild the tools and regenerate raw data and query files",
    )
    parser.add_argument(
            "--scale-factor", type=float, required=True, help="TPC-DS data scale factor used for this run"
    )
    args = parser.parse_args()

    data_dir_str, _, queries_dir_str = get_scenario_paths(args.scale_factor)
    data_dir = Path(data_dir_str)
    queries_dir = Path(queries_dir_str)
    source_dir = Path(TPCDS_TOOL_DIR)
    if not source_dir.is_dir():
        raise FileNotFoundError(f"Unzipped TPC-DS tools directory not found: {source_dir}")
    Path(SCENARIOS_DIR).mkdir(parents=True, exist_ok=True)

    staged = (
        Path(SQL_SCHEMA_PATH).is_file()
        and (Path(QUERY_TEMPLATES_DIR) / "netezza.tpl").is_file()
    )
    if not staged or args.force:
        _stage_tool_source(source_dir)

    _patch_sources(Path(STAGED_TOOLS_DIR))
    _patch_netezza_template(Path(QUERY_TEMPLATES_DIR) / "netezza.tpl")

    schema_sql = Path(SQL_SCHEMA_PATH).read_text()
    table_names = get_table_names(schema_sql)
    if not table_names:
        raise RuntimeError(f"No TPC-DS tables found in {SQL_SCHEMA_PATH}")
    query_names = _expected_query_names()

    data_complete = _raw_data_complete(table_names, data_dir)
    queries_complete = _queries_complete(query_names, queries_dir)
    tools_ready = (Path(STAGED_TPCDS_DIR) / ".build-complete").is_file() and all(
        (Path(STAGED_TOOLS_DIR) / binary).is_file()
        for binary in ("dsdgen", "dsqgen")
    )
    if not args.force and data_complete and queries_complete and tools_ready:
        print("TPC-DS tools, raw data, and queries are already complete; nothing to do.")
        return

    build_marker = Path(STAGED_TPCDS_DIR) / ".build-complete"
    if args.force or not tools_ready:
        build_marker.unlink(missing_ok=True)
        subprocess.run(
            ["make", "-B"] if args.force else ["make"],
            cwd=STAGED_TOOLS_DIR,
            check=True,
        )
        for binary in ("dsdgen", "dsqgen"):
            if not (Path(STAGED_TOOLS_DIR) / binary).is_file():
                raise RuntimeError(f"TPC-DS build did not produce {binary}")
        build_marker.touch()
    else:
        print("TPC-DS utilities are already built; skipping compilation.")

    if args.force or not data_complete:
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / ".complete").unlink(missing_ok=True)
        data_generation_started = perf_counter()
        print(
            f"[{datetime.now():%Y-%m-%d %H:%M:%S}] "
            "Generate raw .dat files started."
        )
        subprocess.run(
            [
                str(Path(STAGED_TOOLS_DIR) / "dsdgen"),
                "-SCALE",
                str(args.scale_factor),
                "-DIR",
                str(data_dir),
                "FORCE",
            ],
            cwd=STAGED_TOOLS_DIR,
            check=True,
        )
        if not _raw_data_files_present(table_names, data_dir):
            raise RuntimeError(f"Raw data generation left missing or empty files in {str(data_dir)}")
        (data_dir / ".complete").touch()
        print(
            f"[{datetime.now():%Y-%m-%d %H:%M:%S}] "
            "Generate raw .dat files finalized "
            f"(elapsed: {perf_counter() - data_generation_started:.2f}s)."
        )
    else:
        print(f"Raw data is already complete in {str(data_dir)}; skipping generation.")

    if args.force or not queries_complete:
        _generate_queries(query_names, queries_dir, args.scale_factor)
    else:
        print(f"Queries are already complete in {str(queries_dir)}; skipping generation.")

    print("TPC-DS preparation completed.")


if __name__ == "__main__":
    main()
