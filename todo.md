Upcoming
==============

### UPSTREAM (dbcli/pgcli) - estado real AUDITADO 2026-07-14
# Auditado con workflow (19 features): TODOS estan en nuestra fork; 18/19 NO
# estan en upstream (solo pgcli_isready se pisa con el --ping de upstream).
# Cruzado con estado de PRs. Regla: 1 PR chico y aislado por feature.

## EN VUELO - PR abierto, solo esperar review de j-bennet (NO es trabajo nuevo)
- [ ] -c/--command            -> PR #1542 OPEN
- [ ] -f/--file               -> PR #1543 OPEN
- [ ] -y/--yes                -> PR #1544 OPEN
- [ ] -t/--tuples-only        -> PR #1545 OPEN (ahora shortcut de \T)
- [ ] .pgpass + SSH tunnel    -> PR #1546 OPEN (MERGEABLE, re-review posteado 2026-07-06)
- [ ] \ne editar named query  -> PR #1609 OPEN (cubre editor de #1430)

## MERGED
- [x] trailing SQL comments   -> #1559 (en upstream 4.5.0)

## GENUINAMENTE PENDIENTE (nuestro, confirmado NO en upstream, SIN PR)
# Buenos candidatos (self-contained, aditivos, no entrelazados con sshtunnel):
- [ ] dsn.d/ (DSN aliases drop-in) - ademas resuelve issue abierto #1489. BUEN candidato
- [ ] namedqueries.d/ (named queries drop-in) + \nr (reload)
- [ ] SET <param> autocomplete desde pg_settings vivo - core de pgcli, alta aceptacion probable
- [ ] SET ROLE <role> autocomplete
- [ ] streaming NOTICE output (VACUUM/ANALYZE VERBOSE linea por linea)
- [ ] stream_results (output por statement estilo pgAdmin) - v4.5.1
- [ ] -o/--output (redirigir resultados a archivo)
- [ ] --no-timings / --no-status
- [ ] \restrict / \unrestrict (mitigacion CVE-2025-8714) - chequear si upstream sumo algo
- [ ] sanitizacion de passwords/paths en logs
- [ ] log_rotation_mode + log_destination (#1547 y #1541 cerrados; revisit si lo queremos)
# Pesados / baja prioridad:
- [ ] pgcli_dump / pgcli_dumpall (wrappers SSH tunnel) - PR GRANDE (necesita SSHTunnelManager reusable, upstream lo tiene inline)
- [ ] ssh_tunnel_save_password (keyring, v4.5.2) - depende de hooks paramiko; adaptar a sshtunnel es nicho

## DESCARTADOS para upstream (no mandar)
- [~] pgcli_isready: upstream YA tiene `--ping` (reemplaza pg_isready). Redundante
- [~] paramiko nativo (IdentityFile/User/Port/ProxyCommand de ~/.ssh/config): upstream lo obtiene GRATIS de la lib sshtunnel (_read_ssh_config, verificado 2026-07-14). Solo era necesario en la fork por reemplazar sshtunnel->paramiko. Nada que mandar (a lo sumo un toggle allow_agent, menor)

# Nota: el archivo `TODO` (mayusculas) en el repo es de UPSTREAM (dbcli/pgcli lo trae), no es nuestro tracking. El nuestro es este todo.md

### Issues upstream - triage (que podemos hacer)
- [ ] #1590 (completion_refresh KeyError) - YA lo arreglamos via cherry-pick #1591 (v4.4.8). El PR #1591 (de shgol) esta abierto pero CONFLICTING. Postear comentario confirmando el fix + empujar rebase (texto listo en la conversacion)
- [ ] #1497 (log/history a $XDG_STATE_HOME en vez de $XDG_CONFIG_HOME) - NO AHORA. Requiere: nueva funcion state_location() en config.py + cambiar resolucion de "default" de log_file/history_file (main.py:700-701 y 1169-1170) + fallback de log_destination (main.py:708-715) + MIGRACION (mover archivos viejos al arrancar, camino recomendado (a)) + tests + changelog + bump
- [ ] #1489 (alias dsn no detectado) - NO se reproduce en 4.5.1. Era 4.1.0 en Windows. El fix vive en NUESTRO refactor de DsnAliases (dsn.d/), no upstreameado: item #5 de la discussion #1603. Comentar pidiendo retest y/o linkear al #5
- [ ] #1398 [easy] (respetar PSQL_EDITOR env var) - quick win ajeno, por si sumamos PRs

### Discussion #1603 (features sobre upstream)
- [ ] Eventual: sumar ssh_tunnel_save_password como feature a ofrecer en la lista (#stream_results ya posteado por Diego)

### Bookkeeping / nice-to-have
- [ ] Evaluar cherry-pick upstream #1601 (licencia SPDX BSD-3-Clause + saca dynamic version, migran a setuptools_scm) - toca como versionamos, revisar con calma
- [ ] Branches feature/stream-results y feature/ssh-tunnel-keyring: ya estan en main; se pueden borrar o conservar si los queremos para PRs upstream separados
- [ ] integration/nb-install: branch throwaway, ya no hace falta (main == su contenido). Se puede borrar


2026-06-26
===================
- [x] Feature \ne <name>: editar named query en $EDITOR y guardar a [named queries] (crea si no existe; copia override si venia de namedqueries.d). Special command + handler edit_named_query. 2 unit tests. Diego lo probo interactivo: OK
- [x] Fix SSL en dsn.d: agregado sslmode=verify-ca + sslrootcert a los DSN de AWS locales que faltaban (con backup previo). Verificado con `select 1` en dev/qa. (Detalle operativo interno, fuera de este repo)
- [x] /fewer-permission-prompts: agregados 3 patrones read-only a .claude/settings.json (gmail search_threads, ruff check *, journalctl *)
- [x] Release v4.5.2 (\ne) pusheado a fork + GitHub release. NOTA: no habia que sacarlo (Diego no lo pidio explicito); se deja porque ya esta. Aprendido: release solo con "saca el release"
- [x] Verificar CI en fork: 4.5.1 pgcli workflow success; 4.5.2 pgcli workflow success (3.10/3.11/3.12/3.13) + CodeQL. behave (stream_results.feature) paso en CI (no corria local)

2026-06-25
===================
- [x] RELEASE v4.5.1: merge de ambos features a main, tag v4.5.1, push a fork (DiegoDAF/pgcli.daf), GitHub release con changelog completo desde v4.4.8 (https://github.com/DiegoDAF/pgcli.daf/releases/tag/v4.5.1)
- [x] Feature stream_results (default False): output por statement en vivo (estilo psql/pgAdmin) en vez de buffer al final. Validado interactivamente por Diego ("lo veo bien")
- [x] Feature ssh_tunnel_save_password (default False): guarda passphrase/password del tunel SSH en keyring del OS (estilo pgAdmin). Validado por Diego; keyring round-trip verificado en su maquina (secretstorage)
- [x] ProxyCommand retry-safe: proxycommand como string, ProxyCommand fresco por intento (test dedicado)
- [x] Tests: 3 unit stream (test_main.py) + 12 unit ssh (test_ssh_tunnel.py), todos verdes; mypy + ruff limpios; 223 passed / 20 skipped en el suite relevante
- [x] behave: stream_results.feature escrito (wiring OK via dry-run); ssh no factible (sin servidor SSH), cubierto por unit tests
- [x] Realineacion de version: 4.4.8 -> 4.5.1 (track upstream 4.5.0). Decidido: ambos features en 4.5.1, no 4.5.2
- [x] Install ahora requiere extras [sshtunnel,keyring]; CLAUDE.md (parent) actualizado: seccion CRITICA, proceso de release, Ultima Version Liberada y tabla de releases
- [x] Instalado en nb 4.5.1 con [sshtunnel,keyring]; config de Diego: stream_results=True, ssh_tunnel_save_password=True, keyring=True
- [x] Revertir corrupcion accidental en changelog.rst (#1573 -> valor CSS hsla(...))
- [x] Revisar repo padre: upstream en 4.5.0, ya tenemos todo via cherry-picks; solo commits de infra nuevos (#1600/#1601/#1602)
- [x] Triage de issues upstream (ver Upcoming); #1489 revisado (no se reproduce)
- [x] Discussion #1603: item #18 (stream_results) posteado por Diego
- [x] Crear este todo.md
