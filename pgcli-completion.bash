_pg_databases()
{
    # -w was introduced in 8.4, https://launchpad.net/bugs/164772
    # "Access privileges" in output may contain linefeeds, hence the NF > 1
    COMPREPLY=( $( compgen -W "$( psql -AtqwlF $'\t' 2>/dev/null | \
        awk 'NF > 1 { print $1 }' )" -- "$cur" ) )
}

_pg_users()
{
    # -w was introduced in 8.4, https://launchpad.net/bugs/164772
    COMPREPLY=( $( compgen -W "$( psql -Atqwc 'select usename from pg_user' \
        template1 2>/dev/null )" -- "$cur" ) )
    [[ ${#COMPREPLY[@]} -eq 0 ]] && COMPREPLY=( $( compgen -u -- "$cur" ) )
}

_pg_services()
{
    # return list of available services
    local services
    if [[ -f "$HOME/.pg_service.conf" ]]; then
        services=$(grep -oP '(?<=^\[).*?(?=\])' "$HOME/.pg_service.conf")
    fi
    local suffix="${cur#*=}"
    COMPREPLY=( $(compgen -W "$services" -- "$suffix") )
}

_pgcli_dsn_aliases()
{
    # return list of DSN aliases from pgcli config and dsn.d directory
    local dsn_aliases
    local config_file="${HOME}/.config/pgcli/config"
    local dsn_dir="${HOME}/.config/pgcli/dsn.d"

    if [[ -f "$config_file" ]]; then
        # Extract DSN aliases from [alias_dsn] section (excluding includedir directive)
        dsn_aliases=$(awk '
            /^\[alias_dsn\]$/ { in_section=1; next }
            /^\[.*\]$/ { in_section=0 }
            in_section && /^[a-zA-Z0-9_-]+ *=/ && !/^#/ && !/^includedir *=/ {
                sub(/ *=.*/, "")
                print $0
            }
        ' "$config_file")
    fi

    # Also read from dsn.d directory if it exists
    if [[ -d "$dsn_dir" ]]; then
        for f in "$dsn_dir"/*.conf; do
            [[ -f "$f" ]] || continue
            # Extract alias name from each .conf file (first key = value line)
            # Pattern already excludes comments since # is not in [a-zA-Z0-9_-]
            local alias=$(awk '/^[a-zA-Z0-9_-]+ *=/ { sub(/ *=.*/, ""); print $0; exit }' "$f")
            [[ -n "$alias" ]] && dsn_aliases="$dsn_aliases $alias"
        done
    fi

    COMPREPLY=( $(compgen -W "$dsn_aliases" -- "$cur") )
}

_pgcli()
{
    local cur prev words cword
    _init_completion -s || return

    case $prev in
        -h|--host)
            _known_hosts_real "$cur"
            return 0
            ;;
        -U|--user|-u)
            _pg_users
            return 0
            ;;
        -d|--dbname)
            _pg_databases
            return 0
            ;;
        -D|--dsn)
            _pgcli_dsn_aliases
            return 0
            ;;
        -f|--file|-o|--output|--log-file|--pgclirc)
            _filedir
            return 0
            ;;
        --on-error)
            COMPREPLY=( $( compgen -W 'stop resume' -- "$cur" ) )
            return 0
            ;;
        --help|-v|--version|-p|--port|-R|--row-limit|--application-name|--prompt|--prompt-dsn|--ssh-tunnel|--init-command|-c|--command|--timeout|--warn)
            # these take a value pgcli cannot guess
            return 0
            ;;
    esac

    case "$cur" in
        service=*)
            _pg_services
            return 0
            ;;
        --*)
            # return list of available options
            COMPREPLY=( $( compgen -W '--host --port --user --username --password --no-password
                --single-connection --version --dbname --pgclirc --dsn --list-dsn
                --row-limit --application-name --less-chatty --prompt --prompt-dsn
                --list --ping --auto-vertical-output --warn --ssh-tunnel --log-file
                --init-command --yes --command --file --tuples-only --output --timeout
                --on-error --no-timings --no-status --help' -- "$cur" ) )
            [[ $COMPREPLY == *= ]] && compopt -o nospace
            return 0
            ;;
        -)
            # only complete long options
            compopt -o nospace
            COMPREPLY=( -- )
            return 0
            ;;
        *)
            # return list of available databases
            _pg_databases
    esac
} &&
complete -F _pgcli pgcli

_pgcli_wrapper()
{
    # pgcli_dump, pgcli_dumpall and pgcli_isready: their own options plus the
    # long options of the PostgreSQL tool they wrap, read from its --help.
    local cur prev words cword
    _init_completion -s || return

    local tool own
    case "${words[0]##*/}" in
        pgcli_dump)    tool=pg_dump;    own='--dsn --ssh-tunnel --tunnel-verbose' ;;
        pgcli_dumpall) tool=pg_dumpall; own='--dsn --ssh-tunnel --tunnel-verbose' ;;
        pgcli_isready) tool=pg_isready; own='--dsn --ssh-tunnel --verbose' ;;
        *) return ;;
    esac

    case $prev in
        --dsn)
            _pgcli_dsn_aliases
            return 0
            ;;
        -h|--host|--ssh-tunnel)
            _known_hosts_real "$cur"
            return 0
            ;;
        -U|--username)
            _pg_users
            return 0
            ;;
        -f|--file)
            _filedir
            return 0
            ;;
        -F|--format)
            COMPREPLY=( $( compgen -W 'plain custom directory tar' -- "$cur" ) )
            return 0
            ;;
        -d|--dbname|-p|--port|-n|--schema|-N|--exclude-schema|-t|--table|-T|--exclude-table|-j|--jobs|-Z|--compress|-E|--encoding|-l|--database|-S|--superuser|--role|--timeout)
            # these take a value
            return 0
            ;;
    esac

    # With --dsn the alias is the connection, so after it the useful thing to
    # offer is the options; a bare word would otherwise get file names.
    if [[ -z "$cur" || "$cur" == -* ]]; then
        local tool_opts
        tool_opts=$( "$tool" --help 2>/dev/null | grep -oE -- '--[a-z][a-z-]*=?' | sort -u )
        COMPREPLY=( $( compgen -W "$own $tool_opts" -- "${cur:---}" ) )
        [[ $COMPREPLY == *= ]] && compopt -o nospace
        return 0
    fi
} &&
complete -F _pgcli_wrapper pgcli_dump pgcli_dumpall pgcli_isready
