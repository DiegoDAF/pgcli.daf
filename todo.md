Upcoming
==============

### FEATURE: namedqueries.d versionados por server (pedido Diego 2026-07-29)
# Objetivo: queries por-version estilo .psqlrc-10 / .psqlrc-12 / .psqlrc-17 de psql
- [ ] namedqueries.d pasa de "archivo = grupo de queries" a "archivo = UNA query"
- [ ] Version del server en el NOMBRE del archivo: `activity.conf` (sin version =
      siempre se carga) vs `activity-10.conf`, `activity-17.conf` (variantes por version)
- [ ] pgcli carga solo las variantes que soporta el server CONECTADO, y el nombre
      expuesto queda limpio: `\n activity` ejecuta la variante correcta segun el server
- [ ] Decisiones de disenio a cerrar al implementar:
      1. Semantica del sufijo: psql usa match EXACTO de version, pero para queries de
         catalogo lo util es "minimo requerido": elegir la variante con mayor version
         <= version del server, fallback a la sin version (ej: server PG15 con
         activity-10 y activity-17 -> usa activity-10). PROPUESTA: best-fit <=.
      2. Formato del archivo: mantener `name = "sql"` (configobj, cambio minimo) o
         pasar a SQL crudo con nombre = filename (adios quoting de una linea,
         multilinea natural). PROPUESTA: SQL crudo, con compat hacia atras para los
         .conf agrupados existentes.
      3. Recarga: la version se conoce recien al conectar -> filtrar en connect y
         re-filtrar en \c (cambio de server) y \nr (reload)
- [ ] Compat: los namedqueries.d agrupados existentes siguen funcionando como hoy

### FORK: features inspiradas en pgadmin4 (analisis 2026-07-15)
# Lista completa (47) + detalle en notas LOCALES (no en este repo publico):
#   ../pgadmin-feature-ideas.md  (los 47, con valor/portabilidad/esfuerzo)
#   ../pgadmin-feature-plans.md  (planes detallados de EXPLAIN / Query-tool / Conexiones)
- [x] #1 psql-style paste (paste_mode + F6 toggle) -> v4.5.4, PUSHEADO
- [x] #2 EXPLAIN summary (slowest nodes / time by relation / estimate misses; explain_summary default False) -> v4.5.5, LOCAL listo para push
- [x] #3 Query-tool bundle -> v4.5.6, LOCAL (commit sin push; falta test de Diego en nb). Ver seccion 2026-07-15
- [~] #4 Conexiones -> CERRADO 2026-07-17 (redundante, verificado en codigo). post-connect SQL: YA ESTA (init-commands global/DSN/--init-command). .pg_service.conf: YA ESTA (parse_service_info lee ~/.pg_service.conf/PGSERVICEFILE/PGSYSCONFDIR + service=/PGSERVICE). keepalives + connect_timeout: ya usables por passthrough de libpq en el connstring (?connect_timeout=10&keepalives=1...), feature dedicada = YAGNI. SSL ~ expansion: unico gap real (no se expande ~ en sslrootcert/sslcert/sslkey) pero usamos rutas absolutas -> sin necesidad practica. Reabrir solo si aparece un caso concreto
- [ ] Backlog (~40 restantes en ideas.md): sub-warnings de EXPLAIN (nested-loop/hash-spill/bitmap-recheck), tweaks de autocomplete, params chicos de conexion, y varios de bajo valor. Ir picando por valor

### UPSTREAM (dbcli/pgcli) - estado real AUDITADO 2026-07-14
# Auditado con workflow (19 features): TODOS estan en nuestra fork; 18/19 NO
# estan en upstream (solo pgcli_isready se pisa con el --ping de upstream).
# Cruzado con estado de PRs. Regla: 1 PR chico y aislado por feature.

## EN VUELO - PR abierto, solo esperar review de j-bennet (NO es trabajo nuevo)
- [ ] -c/--command            -> PR #1542 OPEN (CLEAN, review respondido 2026-07-21)
- [ ] -f/--file               -> PR #1543 OPEN (style arreglado; 1 flake del test del editor en 3.10, pedirle re-run a j-bennet)
- [ ] -y/--yes                -> PR #1544 OPEN (CLEAN, refactor de review aplicado)
- [ ] -t/--tuples-only        -> PR #1545 OPEN (CLEAN, al dia con upstream 2026-07-21)
- [ ] \ne editar named query  -> PR #1609 OPEN (conflicto por merge de #1546 resuelto 2026-07-24, MERGEABLE de nuevo)

## MERGED
- [x] trailing SQL comments   -> #1559 (en upstream 4.5.0)
- [x] .pgpass + SSH tunnel    -> PR #1546 MERGED 2026-07-24 (d69ecbe en upstream main)! Nuestro fork ya lo tenia; nada que cherry-pickear

## BUG DE UPSTREAM que arreglamos nosotros (PR-worthy, aislado en commit 5d60b80)
- [ ] explain mode (F5) rompe special commands: `if explain_mode / elif pgspecial` en pgexecute.run() -> los meta-comandos nunca se detectan con F5 ON. Fix nuestro en 4.5.7. original/main tiene el bug identico (commit 372da81, 2022). EXCELENTE candidato a PR upstream (self-contained, con tests). Ademas arregla \G en explain mode y el guard de restrict-mode

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


2026-07-16
===================
- [x] FIX -d/--dsn override (reportado por Diego): los flags CLI (-d/-U/-h/-p) se ignoraban con --dsn/URI; el connection string siempre ganaba (ej: `pgcli --dsn prod -d otherdb` conectaba igual a la db del alias). Folded en 4.5.7 (SIN bump; Diego pidio no subir version). LOCAL, sin push
  - Root cause (2 capas): (1) PGExecute.connect() reduce a preserved_params={dsn,password,hostaddr} cuando hay dsn -> descarta dbname/user/host/port; (2) PGCli.connect() re-deriva host/port del dsn. Solo lo embebido en el dsn toma efecto
  - Fix: connect_uri() hornea los overrides en el dsn via make_conninfo (+ los sigue pasando como kwargs para pgpass/keyring). Precedencia psql: SOLO flags explicitos de command line overridean (via ctx.get_parameter_source == COMMANDLINE); ni el default de -p (5432) ni env vars (PGPORT/PGHOST/PGDATABASE) pisan el dsn
  - Gotcha encontrado: -p tiene default=5432 (no vacio) -> `if port:` siempre true horneaba 5432 sobre TODO dsn. Por eso el gate por parameter-source. Tambien: -l/--ping fuerza database=postgres -> capturar explicit_dbname antes del clobber
  - Workflow de review adversarial (3 agentes): encontro la regresion de -l/--ping + el multi-host+`-h` (minor, no fixeado, narrow). connect_uri es NUESTRO (dsn=routing), NO es PR upstream limpio
  - 6 tests en test_main.py (2 actualizados + 4 nuevos: dbname override, combined, port-default-no-forwarded, explicit-flags-forwarded). Adversarial: los 6 fallan sin el fix. Suite 2973 (sin DB) / 3102 (con DB throwaway). ruff+mypy limpios. End-to-end verificado por CLI real (6 escenarios)
- [x] FIX explain mode (F5) rompia special commands (v4.5.7, LOCAL commit 5d60b80, SIN push; pendiente test de Diego)
  - Bug (screenshot de Diego): con F5/explain ON, pgexecute.run() tenia `if explain_mode: prefijo / elif pgspecial:` -> el if/elif salteaba la deteccion de special commands, asi \q, exit, \d, \i, named queries, \autocommit, \G, \c... se volvian `EXPLAIN (...) <cmd>` -> syntax error. No se podia ni salir (peor tras reconnect por idle-timeout, que reintenta el comando)
  - Confirmado en log ~/.local/state/pgcli/pgcli-Wed.log + reproducido a nivel run() en PG throwaway
  - Fix: `if pgspecial:` (special SIEMPRE primero) + mover el prefijo EXPLAIN a justo antes de execute_normal_sql (solo SQL real). Bonus: arregla \G en explain mode y mantiene el guard de restrict-mode (CVE-2025-8714) que tambien quedaba bypasseado por F5
  - BUG DE UPSTREAM (original/main tiene el if/elif identico; commit 372da81 "add explain visualizer #1279", 2022). Candidato a PR upstream -> ver seccion UPSTREAM
  - Workflow (5 agentes) para mapear blast radius / diseño / upstream / tests / regresion
  - 4 tests nuevos @dbtest (special no envuelto / describe corre como special / SQL normal si envuelto / \G stripped). Verificacion adversarial: 3 fallan sin el fix. Suite 2969 passed (sin DB), ruff+mypy limpios. Build+install 4.5.7 con [sshtunnel,keyring]

2026-07-15
===================
- [x] #3 Query-tool bundle (v4.5.6, LOCAL, commit a main SIN push; pendiente test de Diego en nb)
  - [x] autocommit toggle: special command `\autocommit [on|off]` + config `autocommit` (default True). Se preserva en reconexiones (pgexecute.auto_commit). Bloquea el cambio si hay transaccion abierta. Toolbar marca "Autocommit: OFF" cuando esta off. 6 unit tests. Smoke test end-to-end: rollback efectivo con off, auto-commit con on
  - [x] \hist [N]: historial de SQL de la sesion con timing (total_time) + OK/ERR, salteando special commands. Usa self.query_history (MetaQuery). Alias \history. 5 unit tests. Verificado end-to-end (incluye flag ERR en query fallida y limite N)
  - [x] execute-selection: binding F9 (filter=has_selection) corre solo el texto seleccionado (estilo pgAdmin run-selection). Seleccion via vi visual mode o shift+flechas en emacs. Toolbar muestra hint "[F9] Run selection" solo cuando hay seleccion. 3 unit tests (test_key_bindings.py nuevo)
  - [~] cancel-query: NO hacia falta. psycopg3 3.3.4 ya cancela server-side en Ctrl-C (Connection.wait captura KeyboardInterrupt -> _try_cancel/cancel_safe -> drena QueryCanceled -> re-lanza). Verificado empiricamente con PG throwaway: backend queda idle y la conexion reusable. Agregar cancel propio seria doble-cancel/dead-code
  - [~] macros: DESCARTADO. Se solapa con named queries (\n, \ns, \ne, \nr que ya guardan/ejecutan snippets); lo unico que suman es bindear snippet a tecla -> bajo valor en REPL, riesgo de colision de teclas + bindings dinamicos
  - [x] post_connection_sql: YA EXISTIA como init-commands (global/DSN/--init-command). Sin trabajo
  - [x] changelog 4.5.6 (unreleased) + bump __init__ 4.5.5->4.5.6 + pgclirc (autocommit=True)
  - [x] Suite completa: 2969 passed, 135 skipped, 1 xfailed, 1 xpassed. ruff + mypy limpios
  - [x] Build wheel 4.5.6 + install `uv tool install --force ...[sshtunnel,keyring]`. Verificado: version, keyring (SecretService), paramiko 5.0, comandos registrados, F9 bound

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
