Upcoming
==============

### BUG anotado 2026-08-10: `--` en named queries de una linea comenta el RESTO de la query
# Sintoma: al aplanar una query multilinea a `name = "sql"` (formato namedqueries.d),
# un comentario `-- ...` embebido deja de comentar "su linea" y comenta todo lo que
# sigue. Y no es solo al convertir: pgexecute.run() hace sqlparse.format(strip_comments)
# sobre el statement ya aplanado, asi que cualquier named query de una linea con `--`
# adentro pierde el resto al EJECUTARSE.
# Opciones de fix (evaluar al implementar):
#   a) al aplanar/guardar: convertir `-- x` a `/* x */` (seguro, preserva el comentario)
#   b) soportar valores MULTILINEA en namedqueries.d (ConfigObj banca triple-quote):
#      la query conserva sus saltos de linea y el `--` vuelve a comentar solo su linea.
#      Bonus: adios al infierno de una-linea para queries largas
#   c) strip de comentarios en el momento de la conversion (lo que hice a mano hoy)
# PROPUESTA: (b) como fix de fondo + (a) como salvaguarda en \ns/\ne al guardar
- [x] HECHO 2026-08-10: multilinea soportado punta a punta (ConfigObj triple-quote, doc en pgclirc) + save() de \ns/\ne convierte `-- x` a `/* x */` token-aware (sqlparse; literales intactos). 5 tests

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

## MERGED
- [x] trailing SQL comments   -> #1559 (en upstream 4.5.0)
- [x] .pgpass + SSH tunnel    -> PR #1546 MERGED 2026-07-24 (d69ecbe en upstream main)! Nuestro fork ya lo tenia; nada que cherry-pickear
- [x] \ne editar named query  -> PR #1609 MERGED 2026-08-03 (5d5e082)! Fork ya tiene version propia mas rica (namedqueries.d/get_source); nada que cherry-pickear

## !! ACCION: #1543 esta trabado por una PREGUNTA SIN RESPONDER de j-bennet (25/07)
- [ ] j-bennet pregunto: "There seems to be a persistent failure in the integration scenario:
      `Scenario: edit sql in file with external editor`. Does that pass for you locally?"
      NUNCA se respondio (Diego mando la respuesta anterior, sobre destructive commands, pero no esta).
      Por eso el PR no se movio. Tenemos LA respuesta: pasa local, es el flake de pexpect timeout=2,
      y ya lo arreglamos en nuestro fork (commit 1c4c4c8, timeouts -> 10s, verificado verde).
      Ofrecer ese parche como PR chico aparte

## PRs NUEVOS en upstream (revisado 2026-08-18) - utilidad para nosotros
- [ ] #1614 (VXNCXNX, ABIERTO): detectar UPDATE incondicional con sqlparse en vez de split por espacios.
      ARREGLA UN BUG REAL NUESTRO (verificado): `update t set a = 'x where y', b = 2` NO dispara el
      warning porque el `where` suelto adentro del string literal enga~na al split ingenuo. Es un
      agujero de seguridad para laburo en prod con destructive_warning. CHERRY-PICK cuando mergee
      (o implementarlo nosotros si tarda)
- [ ] #1615 + issue #1618: soporte sqlparse 0.6.x. Tenemos pin `sqlparse >=0.3.0,<0.6` (0.5.5 instalado).
      No urge, pero hay que seguirlo para cuando salga 0.6
- [~] #1617 (ChrisJr404, ABIERTO): fix de deteccion de alias_dsn para --list-dsn y -D (root cause de
      #1489: usan load_config pelado en vez de get_config). REDUNDANTE PARA NOSOTROS: probado en home
      fresco, nuestro refactor de DsnAliases ya no tiene el sintoma (--list-dsn sale limpio con exit 0,
      --dsn inexistente da el mensaje correcto). Revisar por si trae algun matiz al mergear
- [x] #1616 (ChrisJr404) MERGEADO 2026-08-18: honrar PSQL_EDITOR. ERA NUESTRA FEATURE (la tenemos desde
      v4.5.3 pero nunca la mandamos). Nada que pickear: misma semantica neta (ellos devuelven
      EDITOR/VISUAL explicito, nosotros None y deja que click caiga solo). Leccion: lo que no
      mandamos, lo manda otro
# NOTA: #1616 lo mergeo dbaty, NO j-bennet (j-bennet sin actividad desde el 3/8). Hay revisor nuevo
# moviendo cosas (49 minutos de abierto a mergeado). Buen momento para destrabar nuestros PRs

## EN OBSERVACION: PR #1613 (dcavalcante, ABIERTO 2026-08-12) - cherry-pick cuando mergee
- [ ] "Add filesystem meta-commands and path completion" (+150/-7): agrega \cd y \ls, y extiende
      el path completion (hoy SOLO existe para \i) a \e, \i, \log-file, \ls, \o.
      UTIL para nosotros: completar rutas en \o y \e es lo que mas usamos. \cd/\ls es de yapa.
      NO pickear todavia: esta sin mergear, es de un contribuidor nuevo, el CI de builds ni corrio
      (codex-review fail; primer PR necesita aprobacion de workflow) y cambia la firma del
      namedtuple Path (Path(only_directories=False)), asi que puede moverse en el review

## CHERRY-PICKS desde upstream: HECHOS 2026-08-03 (f0b0733 + d195baa, pusheados)
- [x] #1611: sugerir columnas tras una columna llamada "type" (fixes #1412)
- [x] #1612: decodificar identifiers bytes en completion metadata (fixes #1405)

## BUG DE UPSTREAM que arreglamos nosotros (PR-worthy, aislado en commit 5d60b80)
- [ ] explain mode (F5) rompe special commands: `if explain_mode / elif pgspecial` en pgexecute.run() -> los meta-comandos nunca se detectan con F5 ON. Fix nuestro en 4.5.7. original/main tiene el bug identico (commit 372da81, 2022). EXCELENTE candidato a PR upstream (self-contained, con tests). Ademas arregla \G en explain mode y el guard de restrict-mode

## PLAN DE PRs A UPSTREAM (armado 2026-08-18, de SIMPLE a COMPLEJO)
# Regla de siempre: 1 PR = 1 cosa, con tests y changelog, rama limpia desde original/main.
# Anotar cada uno en la discussion #1603 a medida que se mandan.
#
# TIER 1 - bugs de UPSTREAM, chicos y aislados (mayor chance de merge rapido)
- [ ] 1. behave de-flake: timeouts de pexpect 2s/1s/5s -> 10s en iocommands.py. SOLO tests, ~10 lineas.
        Upstream lo tiene (4 ocurrencias de timeout=2). j-bennet ya pregunto por ese fallo en #1543
        y ya se lo ofrecimos en el comentario del 18/08. EMPEZAR POR ACA
- [ ] 2. explain mode (F5) rompe los special commands: `if explain_mode / elif pgspecial` en
        pgexecute.run(). ~12 lineas + tests. Bug de upstream desde 2022 (commit 372da81). Alto valor:
        con F5 encendido no se puede ni salir. De yapa arregla \G y el guard de restrict-mode
- [ ] 3. -l/--ping descartan el connection string: `if list_databases or ping_database: database =
        "postgres"` pisa la URI/conninfo entera. ~6 lineas + tests. Confirmado en upstream (main.py
        ~1638). Mandar solo la guarda `is_conn_string`; el fallback de dbname necesita que connect_uri
        acepte dbname (que es nuestro), asi que va aparte o simplificado
- [ ] 4. error amigable para \comandos desconocidos (hoy se mandan al server y vuelve
        'syntax error at or near "\"'). ~15 lineas + tests
#
# TIER 2 - features chicas y autocontenidas
- [ ] 5. --no-timings / --no-status
- [ ] 6. SET ROLE <role> autocomplete
- [ ] 7. --on-error [STOP|RESUME] (util sobre todo con -f, pero se sostiene solo)
- [ ] 8. SET <param> autocomplete desde pg_settings vivo (core de pgcli, buena aceptacion esperable)
#
# TIER 3 - features medianas
- [ ] 9. streaming NOTICE output (VACUUM/ANALYZE VERBOSE linea por linea)
- [ ] 10. -o/--output
- [ ] 11. \restrict / \unrestrict (mitigacion CVE-2025-8714)
- [ ] 12. paste_mode + F6
- [ ] 13. stream_results
#
# TIER 4 - grandes, hay que partirlos
- [ ] 14. namedqueries.d/ + \nr
- [ ] 15. EXPLAIN summary
- [ ] 16. Query-tool bundle: partir en 3 (\autocommit / \hist / F9 run-selection)
- [ ] 17. namedqueries versionadas por server (DEPENDE del 14)
- [ ] 18. pgcli_dump / pgcli_dumpall (el mas grande: necesita SSHTunnelManager reusable)
#
# BLOQUEADOS (esperar que mergeen otros PRs nuestros primero)
- [~] fix de -o con --tuples-only / -c / -f: upstream no tiene -t ni -c/-f todavia (#1542/#1543/#1545)
- [~] log_truncate_on_rotation: depende de que entre log rotation (que ya rebotaron una vez)
- [~] multilinea + defusal de `--` en named queries: depende del 14
#
# NO UPSTREAMEABLES (decidido con evidencia)
- [~] fixes de override de DSN (--dsn / URI / key=value): entrelazados con nuestro ruteo dsn=, que
      upstream no tiene. Ver auditoria 2026-07-21
- [~] dsn.d/: ademas ahora #1617 esta arreglando #1489 por otro camino; evaluar cuando mergee
- [~] ssh_tunnel_save_password / paramiko nativo: ver seccion DESCARTADOS

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


2026-08-11
===================
- [x] De-flake behave: timeouts de pexpect en iocommands.py (2s/1s/5s -> 10s). El escenario del editor externo erroraba intermitente en runners lentos (upstream #1543/#1544/#1609 y nuestro propio CI en cee716d). Commit 1c4c4c8, CI del fork verde de nuevo. Mismo parche ofrecido a j-bennet en el hilo de #1543
- [x] FIX -l/--ping descartaban el connection string (reporte Diego): cli() reemplazaba el argumento posicional por "postgres" siempre que se pasara --list/--ping, tirando la URI/conninfo entera (host, user, port, sslmode) y cayendo a socket local con el usuario del SO. Ahora solo se descarta un nombre de base pelado; los connection strings pasan intactos. Sub-caso: si el connstring no nombra base, se inyecta "postgres" para el listado (como psql) en vez del default de libpq (nombre del usuario del SO). Commit c23353a, 8 tests (5 fallan sin el fix), suite 3145 con DB, instalado en nb
  - Nota: sus pruebas 1 y 2 fallaban por OTRA cosa (sin sslrootcert en la forma -h/-U, y en la 2 uso sintaxis de query URL `&` en vez de conninfo con ESPACIOS). Con el fix, `pgcli -h <host> -U <user> -l "sslmode=verify-ca sslrootcert=/path"` ya funciona
  - Candidato a PR upstream (el clobber es de ellos)

2026-08-10
===================
- [x] row_limit = 0 en la config personal de Diego (chau "The result was limited to 1000 rows" para el; el default del producto queda 1000, cero cambio de codigo)
- [x] 4 named queries wraparround_* nuevas (pedido Diego): wraparround_db (edad xid por database), wraparround_top10/top1000 (relaciones por edad de relfrozenxid, limit 10/1000), wraparround_top10_gexec (generador de vacuum freeze). En namedqueries.d (un archivo c/u, sin sufijo = todas las versiones) Y como \set en ~/.psqlrc + los 10 ~/.psqlrc-XX (backups .bak-<ts> hechos). OJO al aplanar: se saco el comentario -- embebido de la query original (comentaba el resto de la linea). Verificado: \n en pgcli + :var en psql + gexec con -t -o genera .sql limpio

2026-08-07
===================
- [x] FIX -o/--output escribia la QUERY en el archivo de salida (reporte Diego: -t -c "select 'vacuum...'" -o vac.sql dejaba el archivo sucio). Regla nueva alineada a psql: el transcript de la query queda SOLO en \o interactivo; con --tuples-only o en modo -c/-f van solo las filas. Commit 49d7947, LOCAL sin push. 3 tests (2 fallan sin el fix); suite 3132 con DB; instalado en nb
  - Nota para Diego: su archivo ademas estaba VACIO de filas porque el filtro era pg_total_relation_size < 3000 (BYTES, nada mide eso; una pagina ya son 8192). Seguramente queria < 3 GB
  - Candidato a PR upstream (el transcript en -o es de ellos)

2026-08-05
===================
- [x] Error amigable para \comandos desconocidos (pedido Diego tras el caso \set ON_ERROR_STOP en -f): en vez de mandar el meta-comando al server como SQL (syntax error confuso), pgcli falla client-side con mensaje claro. Respeta on_error=STOP (el primer intento con `continue` seguia ejecutando el resto del archivo; lo cazo el test). Commit 2d15a04, LOCAL sin push
- [x] Flag --on-error [STOP|RESUME]: override por invocacion del on_error de la config (util para -f). Mismo commit
  - 4 tests nuevos; suite 3129 con DB; instalado en nb. Smoke: archivo con \set -> error amigable y para; con --on-error RESUME sigue

2026-08-03
===================
- [x] CURADO FINAL namedqueries.d: baseline PG12 + nombres psqlrc como canon (decision Diego)
  - Baseline PG12: fuera los fallbacks 9.6/-10; el contenido valido en 12 pasa a ser el archivo base sin sufijo. Quedan SOLO 6 versionados: pg_stat_statements_human-13, pg_stat_io-16, checkpoints-17, node0-17, vacuum_current_activity_{full,min}-17
  - Dedupe: ganan los nombres del psqlrc; 13 renombres de enero a papelera (activity_1min->activity1minute, sessions_*->pg_sessions_*, slot_distance->slotDistance, cache_hit_index->cache_hit_cache_index, cache_hit_ratio->cache_hit_ratio_buffers, pg_stat_statements_top->pg_stat_statements_human, idles_5min->activesidles5minutes, actives_5min/activity_5min/activity_full_query/search_path->sp, etc.)
  - Custom sin contraparte psqlrc que QUEDAN: idle_txn, locks_who_waiting, vacuum_progress, index_create_progress
  - Estado final: 112 archivos, 107 queries (106 en PG12-15, pg_stat_io suma en 16+). E2E PG18 OK. ~/.psqlrc-* intactos
- [x] CONVERSION COMPLETA ~/.psqlrc-9.6..18 -> namedqueries.d versionados (pedido Diego; psqlrc INTACTOS, solo lectura)
  - 104 \set SQL detectados (111 menos prompts/menu). Resultado: 133 archivos, 120 queries en servers modernos, gating real: 117 en 9.6 / 119 en 12 / 120 en 16+
  - Variantes creadas: checkpoints-17 y node0-17 (pg_stat_checkpointer), vacuum_current_activity_{full,min}-17, pg_stat_statements_human-13 (total_time->total_exec_time), pg_stat_io-16, index_create_progress-12, partitions-10, y -10 para replication/pg_sessions*/slotDistance/sequences
  - Stubs de psqlrc ("requiere PG12+", "particionado declarativo no existe en 9.6", select de literal) tratados como AUSENTES -> la query desaparece en servers viejos en vez de mostrar el cartelito
  - 19 archivos existentes reemplazados por la version psqlrc (mas fresca, regenerada 07-31); 23 identicos se conservaron; 16 nombres solo-pgcli intactos. Todo lo reemplazado en papelera via gio trash
  - E2E PG18: 122 en \n; checkpoints/pg_stat_io/bloat ejecutan OK
  - PENDIENTE curado por Diego: quedaron duplicados semanticos con nombre distinto (activity1minute vs activity_1min, slotDistance vs slot_distance, pg_sessions_blocked vs sessions_blocked, etc.) - podar cuando decida con cual quedarse

2026-07-31
===================
- [x] FEATURE namedqueries.d versionados por server, estilo .psqlrc-NN (pedido Diego 2026-07-29; LOCAL commit a0621ec, sin push)
  - Sufijo de version en el filename: `activity-17.conf` = requiere server >= 17; sin sufijo = fallback universal. Best-fit: gana la variante con mayor version <= server; si TODAS las variantes piden server mas nuevo, la query desaparece de \n. Soporta punteadas pre-10 (`-9.6`). Antes de conectar se ofrece la variante mas alta
  - Filtro aplicado post-connect (_filter_named_queries_for_server) y preservado en \nr (que recreaba la instancia y lo perdia). \c no cambia de server en pgcli -> sin re-filtro extra
  - Compat total: archivos agrupados legacy siguen andando (el sufijo aplica a todas las queries del archivo)
  - 8 tests nuevos en test_namedqueries.py; suite 3118 con DB; ruff+mypy limpios. Instalado en nb
  - Config PERSONAL de Diego migrada: 13 .conf agrupados -> 59 archivos query-por-archivo (originales en papelera via gio trash; set cargado verificado IDENTICO). Unica variante real creada: index_create_progress-12.conf (pg_stat_progress_create_index es PG12+; en <12 desaparece, chau stub "requiere PG12+")
  - HALLAZGO: los ~/.psqlrc-9.6..18 fueron regenerados 2026-07-31 10:30 y sus diferencias entre versiones son SOLO formato (case/espacios) para las queries que mapean a namedqueries -> no habia variantes reales que crear (se probaron 7 y se revirtieron). Cuando Diego tenga SQL genuinamente distinto por version (ej: activity con query_id PG14+), crear <name>-<ver>.conf y listo

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
