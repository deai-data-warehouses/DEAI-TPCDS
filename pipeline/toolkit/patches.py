import re
from pathlib import Path


def patch_function_signature(source_file: Path, name: str, old_args: str, new_args: str) -> None:
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


def patch_makefile(makefile: Path) -> None:
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


def patch_sources(tools_dir: Path) -> None:
    signatures = (
        ("date.c", "getDateWeightFromJulian", "jDay, nDistribution", "int jDay, int nDistribution"),
        ("tdefs.c", "getSimpleTdefsByNumber", "nTable", "int nTable"),
        ("tdef_functions.c", "getTdefFunctionsByNumber", "nTable", "int nTable"),
    )
    for filename, name, old_args, new_args in signatures:
        patch_function_signature(tools_dir / filename, name, old_args, new_args)

    makefiles = [tools_dir / name for name in ("Makefile", "makefile")]
    existing_makefiles = [path for path in makefiles if path.is_file()]
    if not existing_makefiles:
        raise FileNotFoundError(f"No Makefile or makefile found in {tools_dir}")
    for makefile in existing_makefiles:
        patch_makefile(makefile)


def patch_netezza_template(template: Path) -> None:
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