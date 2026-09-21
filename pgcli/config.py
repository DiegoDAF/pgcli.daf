import shutil
import os
import platform
import stat
from os.path import expanduser, exists, dirname
import re
from typing import TextIO
from configobj import ConfigObj


def config_location():
    if "XDG_CONFIG_HOME" in os.environ:
        return "%s/pgcli/" % expanduser(os.environ["XDG_CONFIG_HOME"])
    elif platform.system() == "Windows":
        user_profile = os.getenv("USERPROFILE", "")
        return user_profile + "\\AppData\\Local\\dbcli\\pgcli\\"
    else:
        return expanduser("~/.config/pgcli/")


def state_location():
    """Directory for state files (log, history) per the XDG Base Directory Spec.

    Config lives in $XDG_CONFIG_HOME (~/.config); log/history are state, not
    config, so they belong in $XDG_STATE_HOME (~/.local/state). See issue #1497.
    """
    if "XDG_STATE_HOME" in os.environ:
        return "%s/pgcli/" % expanduser(os.environ["XDG_STATE_HOME"])
    elif platform.system() == "Windows":
        user_profile = os.getenv("USERPROFILE", "")
        return user_profile + "\\AppData\\Local\\dbcli\\pgcli\\"
    else:
        return expanduser("~/.local/state/pgcli/")


def migrate_state_file(filename, config_dir=None, state_dir=None):
    """Move a state file (log/history) from the old config dir to the state dir.

    Only migrates when the file exists in the old location and is absent in the
    new one, so it runs at most once and never clobbers a newer file. Returns
    the new path to use.
    """
    config_dir = config_dir or config_location()
    state_dir = state_dir or state_location()
    old_path = os.path.join(expanduser(config_dir), filename)
    new_path = os.path.join(expanduser(state_dir), filename)
    if os.path.abspath(old_path) == os.path.abspath(new_path):
        return new_path  # same dir (e.g. Windows) - nothing to migrate
    try:
        if os.path.exists(old_path) and not os.path.exists(new_path):
            ensure_dir_exists(new_path)
            shutil.move(old_path, new_path)
    except OSError:
        # If migration fails, fall back to the new location; pgcli will just
        # start a fresh file there.
        pass
    return new_path


def load_config(usr_cfg, def_cfg=None):
    # avoid config merges when possible. For writing, we need an umerged config instance.
    # see https://github.com/dbcli/pgcli/issues/1240 and https://github.com/DiffSK/configobj/issues/171
    if def_cfg:
        cfg = ConfigObj()
        cfg.merge(ConfigObj(def_cfg, interpolation=False))
        cfg.merge(ConfigObj(expanduser(usr_cfg), interpolation=False, encoding="utf-8"))
    else:
        cfg = ConfigObj(expanduser(usr_cfg), interpolation=False, encoding="utf-8")
    cfg.filename = expanduser(usr_cfg)
    return cfg


def ensure_dir_exists(path):
    parent_dir = expanduser(dirname(path))
    os.makedirs(parent_dir, exist_ok=True)


def ensure_private_file(path):
    """Create ``path`` if missing and keep it readable by its owner only.

    The history records every statement typed, ``alter role ... password``
    included, the log can carry the same at DEBUG level and the config can
    hold DSN passwords; psql's readline history and libpq's .pgpass are 0600
    for the same reason. A regular file the user owns that already exists
    with wider permissions is tightened; device nodes (``history_file =
    /dev/null`` is the usual way to record nothing), FIFOs, directories and
    files owned by someone else are left alone, and any OSError (read-only
    media, exotic filesystems) is ignored so a permissions problem never
    keeps pgcli from starting.
    """
    path = expanduser(path)
    # O_NONBLOCK: opening a FIFO with no reader must fail, not hang pgcli.
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOCTTY", 0)
    try:
        fd = os.open(path, flags, 0o600)
    except OSError:
        return
    try:
        st = os.fstat(fd)
        owner = getattr(os, "getuid", lambda: st.st_uid)()
        if stat.S_ISREG(st.st_mode) and st.st_uid == owner and stat.S_IMODE(st.st_mode) & 0o077:
            # fchmod on the descriptor we hold: no path lookup between the stat and the chmod.
            if hasattr(os, "fchmod"):
                os.fchmod(fd, 0o600)
            else:
                os.chmod(path, 0o600)
    except OSError:
        # Tightening the mode is best effort: a filesystem that does not
        # support it must not keep pgcli from starting.
        pass
    finally:
        os.close(fd)


def write_default_config(source, destination, overwrite=False):
    destination = expanduser(destination)
    if not overwrite and exists(destination):
        return

    ensure_dir_exists(destination)

    shutil.copyfile(source, destination)
    ensure_private_file(destination)


def upgrade_config(config, def_config):
    cfg = load_config(config, def_config)
    cfg.write()


def get_config_filename(pgclirc_file=None):
    return pgclirc_file or "%sconfig" % config_location()


def get_config(pgclirc_file=None):
    from pgcli import __file__ as package_root

    package_root = os.path.dirname(package_root)

    pgclirc_file = get_config_filename(pgclirc_file)

    default_config = os.path.join(package_root, "pgclirc")
    write_default_config(default_config, pgclirc_file)

    return load_config(pgclirc_file, default_config)


def get_casing_file(config):
    casing_file = config["main"]["casing_file"]
    if casing_file == "default":
        casing_file = config_location() + "casing"
    return casing_file


def skip_initial_comment(f_stream: TextIO) -> int:
    """
    Initial comment in ~/.pg_service.conf is not always marked with '#'
    which crashes the parser. This function takes a file object and
    "rewinds" it to the beginning of the first section,
    from where on it can be parsed safely

    :return: number of skipped lines
    """
    section_regex = r"\s*\["
    pos = f_stream.tell()
    lines_skipped = 0
    while True:
        line = f_stream.readline()
        if line == "":
            break
        if re.match(section_regex, line) is not None:
            f_stream.seek(pos)
            break
        else:
            pos += len(line)
            lines_skipped += 1
    return lines_skipped
