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
        --help|-v|--version|-p|--port|-R|--row-limit|--application-name|--prompt|--prompt-dsn|--ssh-tunnel|--log-file|--init-command|-c|--command|-f|--file|-t|--tuples-only|-o|--output)
            # all other arguments are noop with these
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
            COMPREPLY=( $( compgen -W '--host --port --user --password --no-password
                --single-connection --version --dbname --pgclirc --dsn --list-dsn
                --row-limit --application-name --less-chatty --prompt --prompt-dsn
                --list --ping --auto-vertical-output --warn --ssh-tunnel --log-file
                --init-command --yes --command --file --tuples-only --output --help' -- "$cur" ) )
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
