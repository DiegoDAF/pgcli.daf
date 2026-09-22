Upcoming
==============

### EN ESPERA, DECISION DE DIEGO (aplazado el 2026-09-22: "lo vemos mas tarde")
- [x] 1) PERMISOS DEL .pgpass: RESUELTO 2026-09-22 (a2b603ab), con el alcance acotado que
      pidio Diego ("tirale el control de 0600"): se agrego SOLO el chequeo de permisos, no la
      unificacion de los dos lectores.
      # El chequeo ahora vive en un solo lugar, `pgpass.has_safe_permissions()` (era privado,
      # pasa a publico), y lo usan tanto pgcli como los wrappers. Los wrappers ademas IMPRIMEN
      # el mismo warning que libpq ("has group or world access; permissions should be u=rw
      # (0600) or less") en vez de devolver None calladitos: si no, el dia que alguien apreta
      # los permisos el backup falla sin explicacion.
      # MEDIDO contra el .pgpass real antes de tocar: 544 lineas, 541 con comodin, 3 concretas,
      # y las 3 siguen resolviendo. 4 tests, verificados con mutacion (fallan sin el chequeo).
- [ ] QUEDA ABIERTO de esa familia: unificar el MATCHEO de los dos lectores. Siguen siendo dos
      implementaciones: la de los wrappers hace fnmatch sobre host/database/user (soporta globs
      tipo `*.example.com`) y la de pgpass.py compara por igualdad o `*`. Cambiar de una a otra
      puede dejar de matchear lineas que hoy matchean, y en el .pgpass real 541 de 544 lineas
      usan comodines, asi que el riesgo es concreto. Medir entrada por entrada antes de tocar
- [ ] 2) INSTALAR LA BUILD CON EL FIX DE URI en las dos maquinas de trabajo: corren la build del
      2026-09-22 de la manana, que NO tiene el fix de `-d postgresql://...` ni los 139 tests.
      # 2026-09-22: los 4 servidores SI quedaron con la build nueva (sha256 d3c989914a46...).
      # El inventario por maquina vive FUERA de este repo, en ../instalaciones-locales.md:
      # los hostnames de infra no van a un repo publico.
      # La version sigue en 4.7.2 SIN bumpear (a proposito), asi que `pgcli --version` no
      # distingue una build de la otra: comparar por el sha256 del codigo instalado.
      # Build de la manana, hoy instalada en las dos: a4de3d09e19eafa071c4d06c6f14d64e367c1233f4e7db1275113c06f3c3e6fe
      # Receta: python -m build --wheel && uv tool install --force --reinstall --python 3.12 \
      #   "dist/pgcli-4.7.2-py3-none-any.whl[sshtunnel,keyring]"   (y scp del wheel a `d`)

### LECCION 2026-09-21: ESPERAR A QUE BAJE LA COLA NOS COSTO DOS PRs
# Los dos eran fixes triviales y objetivos, de los que entran en un minuto:
#   - CodeQL deprecado: lo mandamos (#1639) y dbaty ya lo tenia hecho y con mas cosas (#1641)
#   - el @dbtest faltante: lo teniamos ARREGLADO EN RAMA desde el 16/09 y no lo mandamos.
#     Ben Beasley (musicinmybrain) lo mando como #1643 y se mergeo en un dia. Encima el test
#     sin @dbtest lo habiamos introducido NOSOTROS en upstream con los PRs de -c/-f
- [ ] REGLA NUEVA: la politica de "no saturar la cola" aplica a FEATURES, no a fixes chicos y
      objetivos. Esos se mandan apenas estan listos
- [ ] REGLA NUEVA (pedido de Diego el 21/09): al mandar un PR, revisar los LINTERS, no solo la
      suite. El #1645 volvio con una alerta de CodeQL ("except clause does nothing but pass and
      there is no explanatory comment") con ruff, mypy y 2794 tests en verde. CodeQL NO aparece
      como check rojo: llega como comentario inline de github-advanced-security[bot], hay que
      leerlo con `gh api repos/dbcli/pgcli/pulls/<n>/comments`.
      Todo `except` que se trague un error lleva comentario adentro explicando por que

### PENDIENTE DE DIEGO (auditoria 2026-09-16): decisiones y pushes
- [x] RESUELTO 2026-09-17 (11f60e0): `Include` de `~/.ssh/config` ahora se expande antes de que
      paramiko parsee. Medido 17/17 contra `ssh -G`. Lo no obvio, que hubo que MEDIR: ssh
      RESTAURA el contexto Host/Match despues del include, y ademas solo procesa el include si
      ese bloque matchea; un archivo plano no puede expresar esa condicion para los bloques que
      el incluido abre, asi que en ese caso se conservan las directivas sueltas que si aplican
      al padre y se descarta el resto con un debug. El config real de Diego (39 hosts, sin
      Include) resuelve identico que antes
- [ ] QUEDA de la misma familia: `/etc/ssh/ssh_config` y `/etc/ssh/ssh_config.d/` siguen sin
      leerse (ssh los lee DESPUES del config del usuario). Y `%r`/`%p` dentro de un ProxyJump se
      expanden con el User/Port del bloque o el usuario LOCAL, nunca con los de la URL del tunel:
      eso es de paramiko y no se arregla por la API publica
- [x] PUSH HECHO 2026-09-16: `fork/main` = `1cbc87f`, 20 commits despues del tag v4.6.2. Ninguno
      bumpea `__version__` (sigue 4.6.2, sin release nueva). CI del fork VERDE por primera vez
      desde antes del 09-10: los 5 jobs de la matriz (3.10 a 3.14) con unit + integration (behave)
      + ReST + ruff + mypy, y CodeQL en verde con las actions v4/v5 pinneadas por SHA
- [x] HECHO 2026-09-16 14:50: build local 4.6.2 (con los 21 commits) instalada en `t` y en `d`
      (`--force --reinstall --python 3.12`, extras `[sshtunnel,keyring]`). OJO: el numero de version
      es 4.6.2 igual que el release publicado, pero el codigo tiene los commits de la auditoria
- [x] WORKAROUND REVERTIDO en `t`: los 3 bloques volvieron a `ProxyJump` (backup del workaround en
      `~/.ssh/config.bak-workaround-20260916-1448`). VALIDADO contra los 3 hosts reales: el tunel SSH
      levanta por el jump y el canal direct-tcpip llega a 5432 y 6432 del otro lado. Sin tocar
      ninguna base: solo se abrio y cerro el canal TCP, sin handshake de PG ni queries
- [x] `d` estaba prendida y tenia 4.6.1 (no 4.5.8 como decia la nota): actualizada a la build nueva.
      Su `~/.ssh/config` no tiene ni ProxyJump ni ProxyCommand, no alcanza esos hosts por jump
- [x] PERMISOS: el fix solo ajusta el archivo que abre, asi que los logs rotados de otros dias y el
      `history` seguian 664. Pasados a 600 a mano en `t` y `d` (one-shot; de aca en mas los crea
      pgcli con 600). El history solo se ajusta solo en modo interactivo: el bloque `-c`/`-f` sale
      antes de llegar a esa linea, por diseno
- [ ] HISTORIAL PUBLICO: `todo.md` nombro la cuenta de trabajo en los commits c229a45 (2026-09-03) y
      9e02396 (2026-09-10), ya pusheados a `fork/main` (repo publico). El archivo actual ya esta
      limpio (9a709eb); reescribir el historial es decision de Diego (implica force-push del fork)
- [x] HECHO 2026-09-17: los dos CLAUDE.md (`~/scripts/` y `~/.claude/`) decian `pgcli.dev/`, un
      directorio que NO existe. Ahora dicen `pgcli.daf/`, con una linea aclarando el nombre viejo
      y otra diciendo que `pgcli/pgcli/` es el clon de solo lectura de referencia. Backups al lado
- [x] HECHO 2026-09-17: de los 4 candidatos chicos salidos de la auditoria, DOS se mandaron
      (#1639 CodeQL y #1640 sqlparse) y los otros dos quedaron en rama, listos (ver la seccion
      "LISTOS PARA MANDAR"). El de ProxyJump no va a pgcli: upstream usa la libreria `sshtunnel`,
      cuyo `_read_ssh_config` tambien lee solo proxycommand; ese PR iria a pahaz/sshtunnel
- [ ] Upstream mergeo #1542/#1543/#1544 con `-c`/`-f` saliendo con exit 0 aunque falle un statement;
      el fork sale con 1 (psql -c tambien sale 1; -f sale 3 con ON_ERROR_STOP). Evaluar PR
      "exit non-zero on error like psql" cuando haya lugar en la cola

### CHEQUEO OBLIGATORIO ANTES DE QUE MERGEE UN PR (anotado 2026-09-10)
# QUE PASO: en el grafico de contribuidores de dbcli/pgcli del ultimo mes aparecen
# la cuenta de TRABAJO (2 commits) y `claude` (3 commits). NO son commits sueltos:
# GitHub, al hacer SQUASH MERGE, agrega como Co-authored-by a TODOS los autores
# distintos de la rama. Yo mire solo el campo author y dije que upstream estaba
# limpio; estaba equivocado, hay que mirar la rama ENTERA y los trailers.
#   - la cuenta de trabajo salio de 3 merges hechos con el boton "Update branch" de la
#     web estando logueado con la cuenta de trabajo: 19c9730 (2026-03-27) y c8f1b0d
#     (2026-08-24) en feature/command-option, a665991 (2026-08-24) en feature/yes-option.
#     Terminaron como co-autores de 924e7d4 (#1542) y 8e4aef4 (#1544).
#   - claude salio de los trailers Co-Authored-By que arrastraban esas ramas
#     (12 en command-option, 9 en yes-option).
# NO SE PUEDE DESHACER: ya esta en main de dbcli.
- [ ] SIEMPRE antes de mandar un PR y ANTES de que lo mergeen:
      bash ../check_pr_authors.sh <rama> original/main   (vive fuera del repo, en ~/scripts/pgcli/)
      (mira autores de toda la rama + trailers; probado: detecta el caso del #1542)
- [ ] NUNCA apretar "Update branch" en la web sin mirar con que cuenta estas logueado.
      Mejor hacer el merge local y pushear, que ahi manda la identidad del repo
- [x] Arreglado el user.name local de pgcli.daf: decia "DiegoDAF", ahora "Diego"
      (tu CLAUDE.md pide "Diego"; habia 61 commits con el nombre equivocado)
# PENDIENTE DE DECISION: el user.email GLOBAL es el gmail personal, o sea cualquier
# repo sin override commitea con el gmail personal. Asi se filtraron 11 commits con
# ese mail al fork publico. Conviene invertirlo: poner el noreply como default global
# y dejar que el includeIf de la carpeta de trabajo ponga el mail de trabajo donde corresponde.
#   git config --global user.email "DiegoDAF@users.noreply.github.com"
# (ese includeIf ya existe y esta bien escrito, hoy no dispara porque la carpeta
#  de trabajo no es repo git ni tiene repos adentro)

### PLAN DE PRs A UPSTREAM (mapa completo, revisado 2026-09-16 con datos frescos de la API)

# --- MARCADOR ---------------------------------------------------------------
# 11 PRs nuestros MERGEADOS: #1542 -c, #1543 -f, #1544 -y, #1545 -t, #1546 .pgpass+tunel,
#   #1559 comentarios finales, #1609 \ne, #1619 timeouts behave, #1620 explain mode,
#   #1621 -l/--ping, #1622 --timeout
# 6 cerrados sin mergear (viejos, reemplazados): #1530, #1538, #1539, #1540, #1541, #1547
# 2 ABIERTOS hoy: #1637 y #1628
# La cola de upstream tiene 15 PRs abiertos en total, o sea que 2 nuestros es razonable.

### NUESTROS PRs (estado 2026-09-21)
# UPSTREAM PUBLICO 4.7.0 el 19/09 y 4.7.1 el 20/09. La 4.7.0 es LA RELEASE DONDE SALIERON
# NUESTRAS FEATURES: -c/--command, -f/--file, -y/--yes y -t/--tuples-only. La 4.7.1 salio al
# dia siguiente solo para arreglar el numero que reportaba `pgcli --version`.
- [ ] #1637 "--no-timings / --no-status" (item 11 del #1603). Abierto 15/09, sin review. ESPERAR
- [ ] #1628 "SQL_ASCII". BLOQUEADO por el #1629 de dbaty, que lleva 17 DIAS SIN UN SOLO
      COMENTARIO (parado desde el 04/09). Ojo: la 4.7.0 trae un fix de encoding que SUENA igual
      ("TypeError: cannot use a string pattern on a bytes-like object ... SQL_ASCII") pero es el
      del escape_name (#1612), que ya estaba: NO cubre lo nuestro (crash del prompt por
      get_socket_directory() ni el mensaje con b'...' del timezone).
      DECIDIR: preguntar en el #1629 si piensan avanzarlo y ofrecer tomarlo nosotros
- [ ] #1640 "sqlparse >= 0.6.0". SIGUE VIGENTE: upstream continua en >=0.3.0 con los 11 avisos
- [ ] #1645 "history/log/config en 0600". MANDADO 2026-09-21. Rebasado sobre 4.7.1
- [x] #1639 "CodeQL v4" CERRADO por dbaty el 21/09: "I did the same (and some more) in a
      separate pull request: #1641". Su PR ademas pinnea todas las actions y suma Dependabot
- [x] COMENTARIO en el #1638 (PR ajeno de Tatamis): le marcamos que su fix NO alcanzaba (el
      crash se MOVIA de meta.py:162 a pgcompleter.py:77). Lo reprodujo, aplico nuestra
      sugerencia y agrego tests. Ejemplo de que comentar PRs ajenos con evidencia medida sirve

### PROXIMOS A MANDAR, uno por vez y solo cuando baje la cola
- [ ] 1) Item 8 del #1603: `-o/--output`   <-- EL SIGUIENTE
      # Misma familia de scripting que los 4 que ya entraron (-c, -f, -y, -t). Chico y
      # autocontenido. Ahora tiene mejor pie que nunca: los 4 hermanos estan mergeados.
- [ ] 2) Item 6 del #1603: namedqueries.d + sufijo de version (estilo psqlrc-NN)
      # La version del server sale del startup packet (conn.info.server_version), sin
      # round-trip extra.
- [ ] 3) Item 5 del #1603: `dsn.d/`  (BAJADO el 2026-09-03, sigue ultimo)
      # Perdio su respaldo: el issue #1489 lo cerro j-bennet al mergear el #1617 de
      # ChrisJr404 (72c5f91), que resuelve -D y --list-dsn por otra via, sin drop-in.
      # a) hay que REBASAR sobre esa forma nueva (toca las mismas 2 lineas de main.py)
      # b) el pitch se para solo en el valor de la feature (Diego tiene 105 alias)
      # c) PREGUNTAR en el #1603 si les interesa el formato drop-in ANTES de escribir el PR
- [ ] NO MANDAR: item 21 (log_truncate_on_rotation) depende del item 12 (rotacion de logs),
      que upstream ya pospuso una vez ("revisit" en los #1541/#1547).

### LISTOS PARA MANDAR: 2 ramas ya escritas, probadas y esperando lugar en la cola
# Las cuatro salieron de la auditoria del 2026-09-16. Dos se mandaron el 17 (#1639 y #1640).
# Estas dos estan COMPLETAS: nacen de original/main, suite completa verde contra PG real,
# ruff y format limpios, y check_pr_authors.sh dice Limpio. Solo falta apretar el boton.
- [ ] RAMA `upstream/test-future-proofing` (2 archivos, 13 lineas, SIN changelog: upstream no
      registra cambios de test) -> "tests: stop using two APIs that the next major bump removes"
      # (a) `itertools.product` pasado a `parametrize` en test_smart_completion_multiple_schemata:
      #     pytest ya avisa (PytestRemovedIn10Warning) y pytest 10 lo convierte en error
      # (b) `click.get_text_stream("stdin")` en test_prompt_utils: Click 9 lo elimina y ya avisa.
      #     Es literalmente `sys.stdin`, y el test solo usa `.isatty()`
      # NO esta pusheada al fork todavia
- [ ] RAMA `upstream/private-state-files` (4 archivos, 117 lineas, 6 tests + changelog)
      -> "Create the history, log and config files readable by their owner only"
      # El mas grande de los cuatro y el unico que puede generar discusion, por eso va ultimo.
      # Adaptado al codigo de UPSTREAM (no tiene rotacion de logs ni $XDG_STATE_HOME, los puntos
      # de enganche son otros: main.py:663 el handler y main.py:1113 el FileHistory).
      # Incluye los dos casos que marco la revision adversarial: `/dev/null` (history_file
      # apuntando ahi es el modo "no grabar nada"; como root un chmod lo romperia para todo el
      # sistema) y FIFO (colgaba el arranque en open()). fstat+fchmod, sin ventana TOCTOU.
      # MANDAR CUANDO entre alguno de los que estan en vuelo. NO esta pusheada al fork todavia

### PRs DE TERCEROS que nos tocan (estado 2026-09-16)
- [ ] #1629 dbaty (SQL_ASCII): BLOQUEA nuestro #1628 por acuerdo. Sin movimiento desde el 04/09
- [ ] #1631 jackwalkerlabs (passwords literales en service files): CONFLICTING, 4 reviews.
      Detras de el tenemos ENCOLADO nuestro PR del warning por comentarios inline (a617ff2)
- [ ] #1635 jackwalkerlabs (ConfigObj -> configparser en pgclirc): va a CHOCAR con nuestro
      config.py. Ya lo probamos contra el pgclirc real de Diego: 85 claves, 84 identicas, y
      arregla un crash que hoy existe con una coma en `prompt`. Cuando entre, rebasar
- [ ] #1636 anandghegde (COPY stdin/stdout): 3 reviews. Nosotros YA lo tenemos aplicado
- [ ] #1633 MelvinCERBA (Keychain macOS): cherry-pick cuando entre
- [ ] #1632 jackwalkerlabs (service names en el prompt): 0 comentarios, nadie lo mira
- [ ] #1613 dcavalcante (meta-comandos de filesystem): 0 comentarios desde el 12/08
- [ ] #1605 youdie006: j-bennet le pidio resolver conflictos el 04/09, sigue sin moverse
- [ ] #1625 pacocartones (xfail): discutido, sin cerrar
- [ ] #1571 jrraymond ($XDG_STATE_HOME para log e history): TRABADO desde MAYO por conflictos.
      NOSOTROS YA LO TENEMOS implementado y funcionando desde la v4.5.7. Oportunidad: ofrecer
      nuestra version o ayudarlo a destrabarlo, es un feature que ya usamos todos los dias

### DISCUSSION #1603 "Features I maintain on top of upstream"
# https://github.com/dbcli/pgcli/discussions/1603 - creada 2026-06-03 por DiegoDAF
# Lectura honesta: como vidriera NO funciono. 1 upvote y cero comentarios de terceros en
# 3 meses, nadie pidio nunca un numero. Los 11 merges vinieron de mandar PRs chicos, no de
# la lista. Igual sirve como indice propio y como carta de presentacion al mandar cada PR.
# REESCRITA el 2026-09-16: ahora es SOLO una lista de PENDIENTES (decision de Diego).
# Quedaron 18 items; salieron los mergeados (4, 7, 9, 10 y 23) y todo el historial de merges.
# La numeracion NO se toca nunca: los numeros son con lo que la gente elige un item, y hay
# comentarios viejos que los citan. Por eso hay huecos, con una linea arriba que lo explica.
# El 2026-09-16, a pedido de Diego, se saco tambien la seccion final de bugs de upstream y
# los 4 candidatos de la auditoria: el post termina en el bloque Status.
- [ ] MANTENIMIENTO: cada vez que mergeen algo nuestro, SACAR ese item del post (no marcarlo
      como merged). Hoy quedan por sacar, cuando entren: item 11 (#1637) y los que sigan
- [ ] Eventual: sumar ssh_tunnel_save_password como feature a ofrecer en la lista
- [x] CONSULTA de Diego 2026-09-17: contador de visitas estilo odometro en el post. EVALUADO,
      NO se hace. Tres razones: (a) GitHub no sirve imagenes externas directo, las proxya por
      Camo y las cachea, asi que el numero cuenta refrescos del proxy y no visitas; (b) seria un
      beacon de un TERCERO dentro de un repo AJENO: cada persona que abra la discussion le pega
      a ese servicio por una imagen que pusimos nosotros, y queda escrito en el markdown;
      (c) la senal real ya la tenemos (1 upvote, 0 comentarios en 3 meses) y la metrica que
      importa son los merges, que vienen de mandar PRs chicos.
      SI Diego quiere el juguete, el lugar legitimo es el README de NUESTRO repo `pgcli.daf`,
      donde ademas GitHub Insights ya da trafico y clones reales sin depender de terceros

### FORK: features inspiradas en pgadmin4 (analisis 2026-07-15)
# Lista completa (47) + detalle en notas LOCALES (no en este repo publico):
#   ../pgadmin-feature-ideas.md  (los 47, con valor/portabilidad/esfuerzo)
#   ../pgadmin-feature-plans.md  (planes detallados de EXPLAIN / Query-tool / Conexiones)
- [x] #1 psql-style paste (paste_mode + F6 toggle) -> v4.5.4, PUSHEADO
- [x] #2 EXPLAIN summary (slowest nodes / time by relation / estimate misses; explain_summary default False) -> v4.5.5, LOCAL listo para push
- [x] #3 Query-tool bundle -> v4.5.6, LOCAL (commit sin push; falta test de Diego en nb). Ver seccion 2026-07-15
- [~] #4 Conexiones -> CERRADO 2026-07-17 (redundante, verificado en codigo). post-connect SQL: YA ESTA (init-commands global/DSN/--init-command). .pg_service.conf: YA ESTA (parse_service_info lee ~/.pg_service.conf/PGSERVICEFILE/PGSYSCONFDIR + service=/PGSERVICE). keepalives + connect_timeout: ya usables por passthrough de libpq en el connstring (?connect_timeout=10&keepalives=1...), feature dedicada = YAGNI. SSL ~ expansion: unico gap real (no se expande ~ en sslrootcert/sslcert/sslkey) pero usamos rutas absolutas -> sin necesidad practica. Reabrir solo si aparece un caso concreto
- [ ] Backlog (~40 restantes en ideas.md): sub-warnings de EXPLAIN (nested-loop/hash-spill/bitmap-recheck), tweaks de autocomplete, params chicos de conexion, y varios de bajo valor. Ir picando por valor

### Bookkeeping / nice-to-have
- [x] CERRADO 2026-09-17: el #1601 esta mergeado upstream desde el 2026-06-03 y nosotros YA
      tenemos lo que importa (`license = "BSD-3-Clause"`). Lo de `setuptools_scm` NO se adopta a
      proposito: tomaria la version de los tags de git y pelearia con nuestro bump manual de
      `pgcli/__init__.py`. Esa decision ya esta escrita como comentario en el pyproject
# (nota vieja) - toca como versionamos, revisar con calma
- [x] CERRADO 2026-09-17: las tres ramas que figuraban como borrables (feature/stream-results,
      feature/ssh-tunnel-keyring, integration/nb-install) YA NO EXISTEN, ni local ni en el fork.
      Verificado con `git rev-parse` y `git ls-remote`

2026-09-22
===================

### MODULOS FLOJOS: LOS SEIS AL 100% (pedido de Diego, segunda tanda)
# Se priorizo por VALOR, no por porcentaje: primero lo nuestro y lo que se ve en cada linea
# que el usuario mira, no el numero global.
- [x] `pgbuffer.py` 63->100%. NO tenia archivo de test propio, y es lo que decide si Enter
      ejecuta la query. Incluye el guard de regresion del XOR (#1646): `select 17 # 5;` tiene
      que verse como completo
- [x] `pgtoolbar.py` 66->100%. Cubiertos los tres estados del ciclo de F5, F6, autocommit OFF,
      el hint de F9 y los estados de transaccion
- [x] `key_bindings.py` 61->100%. F2, F3, F4 y F6 no tenian ningun test (F5 y F9 si). Tambien
      los dos bindings de `enter` que comparten tecla y se diferencian solo por el filtro
- [x] `packages/prompt_utils.py` 48->100%. Lo importante: `-y/--yes` saltea la confirmacion
      destructiva TAMBIEN sin tty, porque `force` se chequea ANTES del test de tty. Los dos
      tests que habia estaban envueltos en `if not stdin.isatty()`, asi que en terminal no
      probaban nada, y el primero ni llegaba al chequeo que su nombre menciona
- [x] `completion_refresher.py` 64->100%. Los 9 refreshers nunca se habian ejecutado en un test
- [x] `pgstyle.py` 48->100%. NO tenia archivo de test. Los tests definen su propio estilo de
      pygments en vez de afirmar sobre "native", cuyos colores cambian entre versiones
- [x] TOTAL del repo: 81 -> 87%. 3594 passed (eran 3321 al empezar el dia): +273 tests
# DOS SUPUESTOS MIOS QUE LA MEDICION CORRIGIO, y por eso conviene medir antes de afirmar:
#   1) El connstring de `-d` NO ignora `-h`/`-p`: lo que el connstring dice gana, lo que omite
#      lo completan los flags. El codigo estaba bien; el test mio estaba mal.
#   2) `get_app()` fuera de una aplicacion NO lanza: devuelve un DummyApplication con
#      selection_state None. El comentario que escribi decia lo contrario

### TESTS DE LOS WRAPPERS DE BACKUP + BUG DE URI EN -d (pedido de Diego: "subir la calidad")
# Punto de partida medido con coverage, no a ojo: dumpall.py 51% y SIN archivo de test propio,
# dump.py 68%. Lo no cubierto era justo parse_connection_args y build_tunneled_args, o sea el
# corazon del wrapper. Los tests viejos importaban esas funciones SOLO de pgcli.dump; la copia
# de dumpall no se probaba nunca.
- [x] BUG REAL encontrado y arreglado: una URI en `-d` (postgresql://user@host:port/db) no se
      parseaba ni se reescribia. Doble falla silenciosa: (a) el tunel se armaba a "localhost" en
      vez de al host real, y (b) pg_dump recibia la URI intacta mas `-h 127.0.0.1`, y la URI GANA,
      asi que se conectaba derecho al host real ignorando el tunel.
      MEDIDO con pg_dump 18 real, no deducido:
        viejo: pg_dump -d postgresql://daf@db.inexistente.invalid:5432/postgres -h 127.0.0.1 -p 55432
               -> "could not translate host name db.inexistente.invalid"  (bypass del tunel)
        nuevo: las 4 formas (URI, URI con query, keyword/value, flags) conectan por el tunel
- [x] SEGUNDA MITAD del mismo bug: parse_user_and_database solo entendia `-d mydb` pelado. Con una
      URI el "database" terminaba siendo la URI entera y el user quedaba en "postgres", asi que el
      lookup de .pgpass buscaba lo que no era. Eso es lo que decide si un dump tunelizado autentica
- [x] Tambien: `hostaddr=` en keyword/value ahora se reescribe (es lo que libpq realmente disca,
      dejarlo en la IP real tambien saltea el tunel); puerto no numerico en -p/--port/PGPORT ya no
      es un traceback sino un mensaje; PGPORT vacio cae al 5432 en vez de tumbar el dump
- [x] URIs que NO se pueden reescribir con honestidad (multi-host, host=/hostaddr= en el query)
      ahora se reportan en vez de reescribirse a medias
- [x] DRY: las 167 lineas duplicadas byte a byte entre dump.py y dumpall.py se fueron a
      `pgcli/dump_args.py`. Un fix en una sola copia era exactamente el modo de falla que tuvimos
- [x] COVERAGE: dump_args.py 100%, dump.py 68->98%, dumpall.py 51->97%, total 81->85%.
      139 tests nuevos en `tests/test_dump_args.py`, todos parametrizados sobre los DOS wrappers
      para que no vuelvan a divergir. Suite: 3460 passed, 8 skipped, 1 xfailed. ruff/mypy limpios
- [x] Un test viejo (test_dump.py::test_postgresql_uri_in_dbname) AFIRMABA el bug con el comentario
      "host extraction from URI is not done". Actualizado al comportamiento correcto
- [x] Los dos pendientes que salieron de aca subieron a Upcoming (permisos de .pgpass,
      instalar la build con el fix). Diego: "lo vemos mas tarde", 2026-09-22
- [ ] NO CORRIDO localmente: los escenarios behave de dump_commands.feature apuntan a
      `-h localhost` puerto 5432, que es tu Postgres real. Los valida el CI contra el suyo

### 4.7.2 COMPILADA E INSTALADA EN LAS DOS MAQUINAS (pedido de Diego)
# NO es un release: no se toco `__version__` (ya estaba en 4.7.2 desde el 21/09), no hay tag
# nuevo ni GitHub release. Es una build local para usar el codigo del dia a dia.
- [x] Suite completa contra PG descartable (initdb trust, puerto 55432, `unix_socket_directories=''`):
      3321 passed, 8 skipped, 1 xfailed. Sin la base eran 170 skipped, o sea que los `@dbtest`
      corrieron de verdad. ruff, ruff format y mypy limpios
- [x] Wheel `dist/pgcli-4.7.2-py3-none-any.whl` (md5 c44daa0a318a2b7cc83e86b158c610cb), copiado
      a `d` en la MISMA ruta del repo (antes el receipt de `d` apuntaba a `/tmp`, que no dice nada)
- [x] `uv tool install --force --reinstall --python 3.12 "<wheel>[sshtunnel,keyring]"` en `t` y en `d`.
      Verificado en ambas: paramiko 5.0.0, sqlparse 0.6.0, keyring SecretService activo,
      los 4 ejecutables responden

### LECCION: `md5sum ~/.local/bin/pgcli` NO sirve para comparar versiones
# Diego lo uso para comparar `t` con `d` y dieron distinto, pero ese archivo es el console script
# que genera uv. La UNICA diferencia era el shebang: `.../bin/python3` en `t` y `.../bin/python`
# en `d`, dos symlinks al MISMO interprete, escritos por versiones distintas de uv.
# El md5 cambia aunque el codigo sea identico, y peor: NO cambia cuando el codigo si difiere.
- [x] LO QUE SI HABIA: las dos decian `Version: 4.6.2` y el CODIGO era distinto (6 archivos:
      ssh_tunnel, explain_output_formatter, key_bindings, pgexecute, pgtoolbar, pyev). A `d` se le
      instalo la build del 16/09 y a `t` la del 17/09, que ya traia el fix de `Include` en
      ~/.ssh/config (11f60e02) y los cinco commits de EXPLAIN/F5. Mismo numero, codigo distinto:
      es exactamente la trampa de bumpear `__version__` tarde
- [ ] FORMA CORRECTA de comparar dos maquinas (dejar a mano, da un solo hash del codigo instalado):
      ssh <host> '~/.local/share/uv/tools/pgcli/bin/python3 -c "
      import hashlib,pathlib,glob
      p=pathlib.Path(glob.glob(\"/home/daf/.local/share/uv/tools/pgcli/lib/python3*/site-packages/pgcli\")[0])
      h=hashlib.sha256()
      for f in sorted(p.rglob(\"*.py\")): h.update(f.read_bytes())
      print(h.hexdigest())"'
      # 2026-09-22, las dos maquinas: a4de3d09e19eafa071c4d06c6f14d64e367c1233f4e7db1275113c06f3c3e6fe

2026-09-17
===================

### LIMPIEZA DEL todo.md (autorizada por Diego)
- [x] Bajadas a su fecha las 6 secciones de trabajo ya cerrado de agosto y septiembre, que
      seguian ocupando Upcoming. Su contenido esta mas abajo, tal cual estaba
- [x] ARCHIVADAS 5 secciones cuyo contenido ya era FALSO o estaba duplicado:
      - "UPSTREAM - estado real AUDITADO 2026-07-14": listaba #1542/#1543/#1544/#1545 como OPEN.
        Los cuatro se mergearon entre el 3 y el 9 de septiembre
      - "TIMEOUTS de conexion": el trabajo esta HECHO (--timeout salio en 4.5.8 y upstream lo
        mergeo como #1622). Los 28 "pendientes" eran el ranking de candidatos a PR de agosto,
        que hoy duplica y contradice al PLAN DE PRs A UPSTREAM
      - "Issues upstream - triage": decia que #1518/#1484 estaba "PENDIENTE (decision Diego)"
        cuando es el PR #1628, abierto desde el 1 de septiembre; y que $XDG_STATE_HOME era
        "NO AHORA" cuando esta en el fork desde la v4.5.7
      - "Discussion #1603 (features sobre upstream)": duplicaba la seccion nueva, y describia
        un cross-link que se reemplazo al reescribir el post el 2026-09-16
      - "BUG 2026-08-10 (`--` en named queries)": estaba entero en [x], sin pendientes
- [x] Upcoming queda solo con lo vivo. Nada se borro: todo el texto esta mas abajo

### 2026-08-31: fix \watch en -f/-c (bug encontrado por j-bennet en #1543)
- [x] j-bennet volvio (mergeo #1619, #1620 y #1622; dbaty revisa, ella mergea) y probando #1543
      encontro que `pgcli -f archivo` con `\watch` al final repetia el ARCHIVO ENTERO
      (el regex de get_watch_command captura todo el texto previo, es DOTALL)
- [x] Fix en NUESTRO fork (cede0f6): -f y -c pasan por _execute_statements() que splitea con
      sqlparse.split() y ejecuta de a un statement, como psql. \watch queda scoped a su statement;
      \watch pelado usa query_history. BONUS: -f/-c ahora salen con exit 1 si fallo un statement
      con on_error=STOP (como psql con ON_ERROR_STOP); RESUME mantiene exit 0.
      9 tests (5 fallan sin el fix). Wheel 4.6.1 recompilado e instalado con el fix.
- [x] Fix portado al PR #1543 (c724219, sin el cambio de exit codes para mantener el PR minimo)
      + merge de main + respuesta a j-bennet
- [x] #1621: aceptada la sugerencia de j-bennet (simplificar el override de -l). VERIFICADO contra
      psql real: `psql -l dbinexistente` falla con "database does not exist", o sea psql CONSERVA
      el nombre posicional; su version es mas fiel que la nuestra. Aplicado manteniendo el
      try/except (conninfo malformado -> error limpio de conexion, no traceback). bfea2cb
- [x] #1542/#1544/#1545 re-mergeados (conflicto solo en changelog por el Upcoming del #1620)

### 2026-09-01: PR #1628 (SQL_ASCII bytes) + merge del #1621
- [x] #1621 MERGEADO por j-bennet (9da9c8d). Van 8 PRs nuestros mergeados
- [x] Revisado el prototipo local `pr-sql-ascii-clean` (2 pasadas): fix de los issues #1518/#1484
      (psycopg devuelve bytes con encoding SQL_ASCII; crash de prompt, timezone b'...', muerte del
      refresh de completions en parse_defaults). Guards en socket dir, timezone y function metadata,
      mismo decode("utf-8","replace") del #1612. 6 tests (3 fallan sin fix), suite 2758 verde,
      adversarial ok. Primera pasada: fallaba ruff format y habia 2 commits; el prototipo lo
      squasheo y formateo (b9c9c56)
- [x] PR #1628 abierto con la nota de alcance deliberado (function_definition/search_path/schemata/
      databases sin guard, no involucrados en los crashes reportados). Respuestas pegadas en
      #1518 y #1484 con el link. #1603 actualizado (1621 merged + 1628 en la lista de bugs)
- [ ] Los guards ya estan en nuestro main (61f8102+63be76c); evaluar guardar tambien los 4 paths
      restantes en NUESTRO fork (alla no hace falta PR chico)

### 2026-09-01: metacomandos por linea en -f/-c (repregunta de j-bennet en #1543)
- [x] j-bennet pregunto como maneja sqlparse.split los metacomandos. Diego anticipo el agujero:
      interactivo submitea apenas el buffer arranca con \ (pgbuffer.py:53), pero en archivo
      `\echo hola` + select en la linea siguiente viajaban como UN chunk y el metacomando se
      TRAGABA el SQL (igual en el codigo viejo: pgexecute.run usa el mismo sqlparse.split adentro)
- [x] Fix (fork ae445f0+a7a1397, rama #1543 6f3bb1d): regla de psql, un backslash command abarca
      solo su linea; el resto del chunk vuelve al splitter. 3 tests (los 3 fallan sin el fix,
      tras endurecer el assert: el \echo tragon repite el texto del select en su salida, asi
      que assertar solo por texto no probaba nada). Wheel 4.6.1 recompilado e instalado

### 2026-09-10: #1542 MERGEADO (van 12) + rumbo del #1628 decidido
- [x] #1542 (-c/--command) MERGEADO por j-bennet: "Sounds good, let's merge." El nudge del dia
      anterior lo destrabo en menos de 3 horas. Van 12 PRs nuestros mergeados
- [x] #1628: j-bennet eligio la OPCION 3 del nudge: "get #1629 in first to fix the issue for the
      plain ASCII users (because we assume this is most users), then follow-up to fix less common
      problems". Le contestamos aceptando (issuecomment-5618880770) y ofreciendo a dbaty: (a) el
      fixture y tests de SQL_ASCII (que corren en el container UTF-8 del CI sin tocar el workflow,
      via `create database ... encoding 'SQL_ASCII' template template0`, al lado del create_db()
      que ya existe en tests/db_utils.py) y (b) una pasada de review cuando salga de draft
- [ ] ESPERAR que mergee el #1629 (sigue en DRAFT, sin tocar desde el 04/09) y recien ahi rehacer
      el #1628 como follow-up: fallback al decode defensivo cuando el server tire
      CharacterNotInRepertoire, o sea el override de encoding queda como camino primario
- [x] main sincronizado con upstream hasta 924e7d4 (46fa7df + merge -s ours de360a9). De los 4
      commits, 3 son nuestros que volvieron (#1542/#1544/#1545); el unico contenido real que
      faltaba era el #1630 (urls del pyproject), aplicado apuntando a NUESTRO fork.
      OJO con eso: meter el header [project.urls] en el medio del bloque [project] se traga
      requires-python y dependencies dentro de esa tabla. Va DESPUES de la ultima clave.
      Verificado parseando el TOML y leyendo la METADATA del wheel construido

### 2026-09-09: destrabar los 2 PRs abiertos
- [x] #1542: j-bennet pidio resolver conflictos el 04/09, los resolvi ese mismo dia pero
      NUNCA le conteste (misma trampa de julio: pushear el fix no es contestar la review).
      Comentario mandado (issuecomment-5605732251) avisando que quedo CLEAN 7/7, y de paso
      documentando los 2 cambios que salieron del merge con el -f ya mergeado: el -c usa
      _execute_statements (el \watch queda scoped) y -c junto con -f corren LOS DOS como psql
- [x] #1628: nudge pidiendo decision (issuecomment-5605735480). NO lo cerramos: el #1629 de
      dbaty sigue en DRAFT sin tocar desde el 04/09 y su propio autor lo llama "not fully
      tested"; cerrar el nuestro dejaria cero mergeable con los issues #1518/#1484 abiertos.
      El nudge ofrece 3 salidas (cerramos el nuestro / hacemos la version combinada / #1629
      primero y el fallback despues) para que elijan sin que parezca defensa de territorio
- [ ] ESPERAR respuesta. Si eligen la opcion 2, el trabajo es: override_client_encoding como
      mecanismo primario + decode defensivo como fallback ante CharacterNotInRepertoire

### 2026-09-08: PRs de terceros que nos afectan (barrido del repo padre)
# Upstream quieto desde el 04/09 (8e4aef4). Nuestros 2 abiertos sin novedad:
# #1542 CLEAN esperando review; #1628 esperando que decidan entre el nuestro y el #1629
# de dbaty (que sigue en DRAFT desde el 04/09, sin respuesta a nuestra comparacion medida).
- [x] HECHO 2026-09-09 (a617ff2): workaround propio, NO esperamos el merge del #1631.
      parse_service_info() pasa de ConfigObj a ConfigParser(interpolation=None,
      delimiters=("=",), comment_prefixes=("#",), inline_comment_prefixes=None).
      MAPEADO contra libpq real (10 casos, sonda = dbname que el server devuelve textual):
      coincidimos 10/10. Antes divergiamos en 5: abc#def -> 'abc', #abc -> '', a,b -> lista,
      'quoted'/"quoted" -> sin comillas.
      EXTRA que el #1631 no tiene: warning cuando un valor contiene " #", porque
      pg_service.conf NO soporta comentarios inline (a diferencia de postgresql.conf, ver
      guc-file.l:95) y el error de libpq ('invalid integer value "5432 # prod"') no los
      menciona. 14 tests, 7 fallan sin el fix. Idea de parche a PG anotada en
      ~/scripts/postgres/todo.md
- [ ] PR ENCOLADO (no mandar todavia): el WARNING de comentarios inline en service files.
      Es lo unico nuestro que el #1631 no cubre, y es el workaround real de la limitacion de
      libpq. DEPENDE del #1631: sin parseo literal el valor ya viene mutilado y el warning
      nunca se dispara, por eso NO se puede armar la rama sobre el main de upstream de hoy
      (sigue con ConfigObj). Cuando el #1631 mergee: rama desde original/main y portar de
      nuestro a617ff2 solo la funcion _warn_on_inline_comments() + sus 2 tests
      (test_pg_service_file_warns_on_apparent_inline_comment y el _no_warning_without_inline_hash).
      NO portar el cambio de parseo, ese ya lo hace el #1631.
- [x] Aportado al #1631 (2026-09-09, issuecomment-5605504837): tabla de los 10 casos medidos
      contra libpq real, la explicacion de para que sirve el itertools.repeat (dbaty lo
      cuestionaba: mantiene la numeracion de linea, sin el la linea 6 se reporta como 3),
      la CORRECCION de que el KeyError que dbaty temia NO existe (la guarda `if service not in`
      esta y ConfigParser.__contains__ devuelve False; probado con su codigo exacto, y sus
      comentarios son 4h POSTERIORES al unico commit asi que no es que se agrego despues),
      y el aviso de que la excepcion cambia de configobj.ParseError a configparser.ParsingError
      (nadie la captura, solo cambia el traceback). Ofrecidos los tests parametrizados
- [~] #1631 (jackwalkerlabs) "Preserve literal passwords in PostgreSQL service files":
      ES UN BUG REAL NUESTRO, verificado. parse_service_info() usa ConfigObj, que trata el
      `#` como comentario inline: un `password=abc#def` en .pg_service.conf se lee como 'abc'.
      Reproducido en nuestro fork. Su fix cambia a ConfigParser(interpolation=None,
      delimiters=("=",), comment_prefixes=("#",)). CHERRY-PICK cuando mergee.
      Impacto practico para Diego HOY: bajo, no tiene ~/.pg_service.conf. Pero tiene 5
      entradas en .pgpass con `#` en el password, asi que si algun dia migra a service file
      lo muerde en silencio. dbaty ya lo comento (2 veces el 07/09)
- [ ] #1633 (MelvinCERBA) "Avoid rewriting passwords loaded from keyring":
      APLICA A NUESTRO FORK: tenemos identico `if passwd and auth.keyring:
      auth.keyring_set_password(key, passwd)` (main.py:1219). Reescribimos la pass en el
      keyring en CADA conexion exitosa, incluso cuando la acabamos de leer de ahi.
      En macOS eso resetea el "Allow Once" del Keychain; en Linux es solo escritura inutil.
      Prioridad baja para nosotros, pero cherry-pick candidate
- [ ] #1632 (jackwalkerlabs) "Show PostgreSQL service names in the prompt": feature nueva
      (token `\service` en el prompt). No la tenemos. Solo util si usamos service files
- [~] #1630 (Add Changelog project URL) y #1625 (remove stale xfail): irrelevantes para nosotros

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

### TIMEOUTS de conexion (consulta Diego 2026-08-19, medido)
# pgcli NO setea ningun connect_timeout para la conexion a Postgres: usa el default de libpq, que es
# 0 = esperar indefinidamente (en la practica hasta que el SO abandone el TCP, ~2 min en Linux).
# Medido contra 192.0.2.1 (traga los SYN): sin connect_timeout seguia colgado a los 30s; con
# connect_timeout=5 corta exacto a los 5s. PGCONNECT_TIMEOUT tambien funciona (passthrough libpq).
# Lo unico que SI tiene timeout propio es el handshake del tunel SSH: 10s hardcodeado en
# ssh_tunnel.py (_base_connect_kwargs).
- [x] HECHO 2026-08-19 (commit 8f460ef): opcion --timeout + config connect_timeout (default 30).
      Precedencia: --timeout > connect_timeout del connstring > $PGCONNECT_TIMEOUT > config.
      El valor resuelto viaja como PARAMETRO aparte (no se mergea al dsn), asi el connstring del
      usuario llega intacto; PGExecute lo preserva junto al dsn igual que hostaddr.
      8 tests + medicion e2e por cada nivel. Candidato a PR upstream (no tienen nada de esto)
- [x] HECHO 2026-08-19: los 104 DSN de Diego en dsn.d/ ahora llevan connect_timeout=15
      (backup en ~/.config/pgcli/dsn.d.bak-<ts>). 3 de ellos estaban SIN comillas y necesitaron
      un segundo pase; los 104 parsean OK
# HALLAZGO de test isolation: tests que dejan correr connect() con PGExecute mockeado ESCRIBEN en el
# keyring REAL del sistema (auth.keyring_set_password tras "conectar" con exito). Encontradas y
# borradas 2 entradas basura: 'bar@baz.com@' y 'b_user@b_host@5435' (esta ultima con la password de
# test 'very_secure'). Ademas test_pg_service_file setea PGPASSWORD y solo lo borra si PASA: si falla,
# la variable se filtra y hace fallar tests posteriores en cascada. Vale aislar esto en algun momento

## RELEASE v4.5.8 (2026-08-19) - PUBLICADO
# tag v4.5.8 + GitHub release con wheel, pusheado a fork/main, instalado en nb.
# 37 commits y 16 items de changelog desde v4.5.7. Destacados: --timeout/connect_timeout,
# namedqueries versionadas, sqlparse 0.6.x (4 CVEs), UPDATE incondicional con sqlparse,
# fix de explain mode, fix de -l, fix de -o, error amigable de \cmd, log_truncate_on_rotation.
# 8 PRs abiertos en upstream: #1542 #1543 #1544 #1545 #1619 #1620 #1621 #1622

## ESTADO CI DE NUESTROS PRs (18/08) - leer antes de asustarse por rojos
# 1) `codex-review` FALLA EN TODOS los PRs del repo, incluso en los YA MERGEADOS (#1616). Es su bot,
#    no es nuestro. Ignorar ese check.
# 2) `gh pr checks` muestra los jobs CANCELLED (por fail-fast) como "fail". Siempre mirar las
#    conclusiones reales por job: gh run view <id> --json jobs
# 3) #1619 y #1621: TODOS los builds en verde
# 4) #1620: el unico fallo real fue build 3.14 -> el flake del editor externo (iocommands.feature:3).
#    En el MISMO commit, 3.10 dio 22 scenarios passed / 0 failed. Justamente lo que arregla #1619.
#    Comentado en el PR; si #1619 entra primero, rebasar #1620 encima

## PLAN DE PRs A UPSTREAM (armado 2026-08-18, de SIMPLE a COMPLEJO)
# Regla de siempre: 1 PR = 1 cosa, con tests y changelog, rama limpia desde original/main.
# Anotar cada uno en la discussion #1603 a medida que se mandan.
#
# TIER 1 - bugs de UPSTREAM, chicos y aislados (mayor chance de merge rapido)
- [x] 1. behave de-flake -> PR #1619 MANDADO 2026-08-18 (rama upstream/deflake-editor-timeouts).
        Solo tests + changelog, 2 archivos. Linkeado a la pregunta de j-bennet en #1543
- [x] 2. explain mode rompe special commands -> PR #1620 MANDADO 2026-08-18
        (rama upstream/explain-mode-special-commands). 4 tests (3 fallan sin el fix), suite 2734
- [x] 3. -l/--ping descartan el connection string -> PR #1621 MANDADO 2026-08-18
        (rama upstream/list-keeps-connection-string). Salio MAS SIMPLE que en nuestro fork: upstream
        ya importa make_conninfo, asi que el fallback de dbname se resuelve ahi mismo sin tocar
        connect_uri. 5 tests (4 fallan sin el fix), suite 2735
- [ ] 4. error amigable para \comandos desconocidos (hoy se mandan al server y vuelve
        'syntax error at or near "\"'). ~15 lineas + tests
- [x] EXTRA. --timeout + connect_timeout config -> PR #1622 MANDADO 2026-08-19
        (rama upstream/connect-timeout). Item 23 de la #1603. 7 tests, suite 2737
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
- [ ] #1518 / #1484 (crash de arranque con client encoding SQL_ASCII: `TypeError: replace() argument 2 must be str, not bytes`)
      # REPRODUCIDO e2e 2026-08-31 (ver seccion de fecha). Root cause: con SQL_ASCII el TextLoader de
      # psycopg (3.2.3 Y 3.3.4) devuelve los text como bytes; pgcli los mete en self.host
      # (get_socket_directory, SOLO cuando se conecta por socket sin -h), en el mensaje de timezone
      # (show time zone) y en search_path. La 4.3.0 cae en los DOS paths (prompt + completion thread).
      # NUESTRA FORK: el path de completion YA esta guardado (escape_name decodifica bytes, PR #1612),
      # pero SIGUE cayendo en el prompt por get_socket_directory() (pgexecute.py:759, sin defensa) y
      # get_timezone() (pgexecute.py ~992) hace str(bytes) -> mensaje verde con b'...' (no crashea, feo).
      # PENDIENTE (decision Diego): fix de decodificacion defensiva en esas dos funciones + tests.
      # El PR upstream queda en cola segun el PLAN DE PRs (WAIT hasta 3-4 abiertos)
- [ ] #1590 (completion_refresh KeyError) - YA lo arreglamos via cherry-pick #1591 (v4.4.8). El PR #1591 (de shgol) esta abierto pero CONFLICTING. Postear comentario confirmando el fix + empujar rebase (texto listo en la conversacion)
- [ ] #1497 (log/history a $XDG_STATE_HOME en vez de $XDG_CONFIG_HOME) - NO AHORA. Requiere: nueva funcion state_location() en config.py + cambiar resolucion de "default" de log_file/history_file (main.py:700-701 y 1169-1170) + fallback de log_destination (main.py:708-715) + MIGRACION (mover archivos viejos al arrancar, camino recomendado (a)) + tests + changelog + bump
- [ ] #1489 (alias dsn no detectado) - NO se reproduce en 4.5.1. Era 4.1.0 en Windows. El fix vive en NUESTRO refactor de DsnAliases (dsn.d/), no upstreameado: item #5 de la discussion #1603. Comentar pidiendo retest y/o linkear al #5
- [ ] #1398 [easy] (respetar PSQL_EDITOR env var) - quick win ajeno, por si sumamos PRs

### Discussion #1603 (features sobre upstream)
- [x] 2026-08-19: CROSS-LINK hecho en las dos direcciones. En la #1603: item 4 marcado como MERGED
      (#1546), item 7 -> #1542 + #1543, item 9 -> #1544, item 10 -> #1545, y una seccion de status al
      final que ademas lista los 3 PRs de bugs de upstream (#1619/#1620/#1621) aclarando que NO son
      items del catalogo. En cada PR: footer diciendo que item es. Ojo: `gh pr edit` NO sirve
      (rompe por el deprecation de Projects classic); usar `gh api -X PATCH repos/.../pulls/N`
- [x] Numeracion arreglada: habia DOS items 18 (stream_results y EXPLAIN summary). stream_results
      paso a ser el 22 para que los numeros sean unicos (la discussion dice "pick a number")
- [ ] Eventual: sumar ssh_tunnel_save_password como feature a ofrecer en la lista

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

2026-09-16
===================

### AUDITORIA 2026-09-16: seguridad, obsoletos y updates pendientes (pedido "quemate los tokens")
- [x] METODO: el workflow de 8 finders en paralelo murio entero por limite de sesion (reset 13:00);
      se rehizo solo y secuencial ("de a uno", pedido de Diego). Un unico agente revisor adversarial
      al final sobre los commits del dia
- [x] CI DEL FORK ROJA en cada push desde el 09-10 (tres escenarios behave): los de "query invalida"
      esperaban exit 0 pero el fork sale con 1 desde cede0f6 (medido: psql -c sale 1; -f sale 0 sin
      ON_ERROR_STOP y 3 con), y `-t -c '\\dt'` pasaba DOS backslashes (antes el error salia 0 y el
      escenario no probaba nada). Arreglado en da977fa + rename `test_diego_column` -> `test_column`
- [x] FUGA DE IDENTIDAD en el repo publico: `todo.md` (trackeado) nombraba la cuenta de trabajo y
      paths `/home/daf`; limpiado en 9a709eb. Los tests con `@gmail.com`/`amazonaws` son data de upstream
- [x] PERMISOS: `~/.local/state/pgcli/history`, `log` y los `pgcli-*.log` rotados estaban 0664 (umask)
      mientras `.psql_history` y `.pgpass` son 0600; el history guarda cada statement (passwords de
      `alter role` incluidas). `ensure_private_file()` en config.py: crea 0600 y ajusta existentes.
      Aplicado a config por defecto, history y cada log al abrirlo (c0fe4e4, 4 tests)
- [x] DEPS: OSV sobre los 68 paquetes del .venv: solo `cryptography 48.0.0` (7 avisos, 3 HIGH) en el
      venv de DESARROLLO; el tool instalado ya tenia 50.0.1. Actualizado el venv. `sqlparse >=0.5.0`
      permitia 0.5.x con 11 avisos (asi `d` quedo en 0.5.3): piso subido a 0.6.0 y `click < 9` como
      upstream (49aa999). Sin DeprecationWarnings al importar ni en los tests unitarios
- [x] CI CONFIG: `codeql-action@v2` + `checkout@v3` deprecados (annotation "failure" de GitHub):
      v4/v5 (0ad4f8e); `permissions: contents: read` en ci.yml (no usa secrets). Python 3.14 sumado a
      la matriz tras correr la suite completa con 3.14.0: 3222 passed (0c850e4). Sin dependabot (ni
      upstream): se decidio no agregarlo, las deps se refrescan a mano por release
- [x] SYNC UPSTREAM: solo faltaba 2de6387 (merge de #1614); `parseutils` del fork era identico,
      merge -> 16e6e7c (solo AUTHORS y la linea de test). Cuidado: mi assert `"=======" not in s`
      matcheaba subrayados RST y commiteo marcadores de conflicto; corregido con amend
- [x] TESTS DE ENTORNO: `test_isready.py` leia PGHOST/PGPORT del entorno (fixture autouse, fa839d3);
      los steps de `pgcli_dump` capturaban con text=True y `pg_dump -F c` es binario (f45abb8);
      `@dbtest` faltante + `itertools.product` en parametrize + `click.get_text_stream` (ff5c696, y sig.)
- [x] DOCS: README regenerado desde `--help` (faltaban --on-error y --timeout, 5227c9d); CLAUDE.md
      del proyecto: version 4.6.2, bloque DSSKey marcado OBSOLETO, tabla de releases completa,
      proxima revision 2026-10-17. pgclirc vs codigo: sin drift (casing_file/destructive_warning/
      use_local_timezone se leen por otras vias). Keyring: la clave ya incluye el puerto (#1536 cubierto)
- [x] LIMPIO (revisado, sin hallazgo): subprocess solo con listas (sin shell=True) en dump/dumpall/
      isready; ProxyCommand via shlex.join; logs no vuelcan kwargs/DSN con password; ~/.ssh/config
      no se lee salvo el host del tunel; bind del forwarder en 127.0.0.1; ruff extendido: solo
      B904/ARG (callbacks de prompt_toolkit y params de API, no bugs)
- [x] CORREO/UPSTREAM: nada nuevo salvo #1614 mergeado y Vincent explicando el rebase; PRs abiertos
      sin cambios respecto al barrido anterior
- [x] REVISION ADVERSARIAL (un agente, al final) sobre los commits del dia: 11 puntos, 8 aplicados:
      `ProxyCommand none` en un host especifico debe anular el ProxyJump de `Host *` (paramiko lo
      guarda como None y lo salteabamos; idioma estandar de exclusion), comentario `#` al final de
      la linea, `ssh://` con puerto invalido perdia los IdentityFile, hint del error de DNS honesto,
      `ensure_private_file` con fstat+fchmod (antes `history_file = /dev/null` como root habria
      dejado /dev/null 0600 y un FIFO colgaba el arranque), log asegurado ANTES del FileHandler,
      changelog sin prometer el chmod del config existente, codeql.yml pinneado por SHA.
      Paridad con OpenSSH re-medida: 10/10. Sin aplicar (limitaciones de paramiko, anotadas abajo)

### FIX: el tunel SSH ignoraba ProxyJump del ~/.ssh/config (encontrado 2026-09-16 en el proyecto vps, arreglado el mismo dia)
- [x] HECHO: `_proxy_command_from_proxyjump()` + `_proxy_command_from_host_config()` en `pgcli/ssh_tunnel.py`
      replican `ssh.c` ("Setting implicit ProxyCommand from ProxyJump"): el ULTIMO salto se marca con
      `-W '[host]:port'` y los anteriores van en `-J a,b`; `user@host:port`, IPv6 `[v6]:port`, URI `ssh://`
      y `none` contemplados. Precedencia igual a OpenSSH, medida con `ssh -v`: gana la PRIMERA directiva
      leida (ProxyCommand o ProxyJump), y `ProxyJump none` NO bloquea un ProxyCommand posterior. El dict
      de `SSHConfig.lookup()` conserva ese orden, asi que se itera por orden de aparicion
- [x] PARIDAD MEDIDA: 8/8 configs de prueba dan el mismo comando que imprime `ssh -v` (OpenSSH 9.6)
- [x] ERROR MEJORADO: `socket.gaierror` ahora dice `could not resolve SSH host 'X' from this machine` y,
      si no habia proxy, `(no ProxyJump/ProxyCommand applies to it in ~/.ssh/config, so it was dialed directly)`
- [x] TESTS: `TestProxyJump` en `tests/test_ssh_tunnel.py` (12 tests: helpers, precedencia, regresion del
      tunel construyendo `paramiko.ProxyCommand` desde ProxyJump, y los dos mensajes de error)
- [x] CHANGELOG: entrada en Upcoming (sale en la proxima release, la 4.6.2 ya estaba publicada)
- [ ] PENDIENTE: cuando se instale una version con el fix, volver el `~/.ssh/config` de `t` a `ProxyJump`
      (backup `~/.ssh/config.bak-20260916-1139`). Hasta entonces el workaround con ProxyCommand sigue andando
- [ ] UPSTREAM: dbcli/pgcli usa la libreria `sshtunnel` (no nuestro modulo), que tiene el MISMO defecto
      (`_read_ssh_config` solo mira `proxycommand`). El fix alla seria un PR a pahaz/sshtunnel, no a pgcli
- [x] DIAGNOSTICO ORIGINAL (sesion vps):
- [x] SINTOMA: `pgcli --dsn <alias>` contra un host que en `~/.ssh/config` se alcanza por `ProxyJump`
      falla con `SSH tunnel failed: [Errno -2] Name or service not known`, mientras que `ssh <host>`
      con el MISMO config conecta sin problema. O sea que el config esta bien y el que no lo honra
      somos nosotros
- [x] CAUSA RAIZ, verificada con paramiko 5.0.0: `pgcli/ssh_tunnel.py` linea ~389 hace
      `proxycommand = host_config.get("proxycommand")`, pero `paramiko.SSHConfig.lookup()` devuelve
      la clave **`proxyjump` cruda y NO la traduce** a `proxycommand`. Comprobado imprimiendo las
      claves del lookup: aparece `proxyjump`, no aparece `proxycommand`. Con lo cual `proxycommand`
      queda en None, no se pasa `sock` y paramiko intenta resolver el hostname final LOCALMENTE.
      Si ese nombre solo existe en el DNS del otro lado del jump, no resuelve y muere ahi
- [x] POR QUE NO SE VEIA HASTA AHORA: el caso tapaba el bug. Mientras la maquina tenia una VPN
      propia hacia esa red, resolvia el nombre interno y conectaba DIRECTO, sin usar el jump nunca.
      El bug estuvo siempre, latente. Aparecio al mudar la VPN a otra maquina
- [x] FIX PROPUESTO, chico y acotado a esa funcion: si `host_config` trae `proxyjump` y NO trae
      `proxycommand`, sintetizar `ssh -W %h:%p <valor de proxyjump>`, que es exactamente lo que hace
      OpenSSH por dentro. El resto de la cadena ya funciona: `_base_connect_kwargs()` construye
      `paramiko.ProxyCommand(...)` y lo pasa como `sock`
- [x] VALIDADO A MANO ANTES DE ESCRIBIR CODIGO: armando el `SSHClient` con ese ProxyCommand
      sintetizado, el tunel establece y el canal `direct-tcpip` a `localhost:5432` abre, sin VPN
      y sin resolver el nombre interno en la maquina local
- [x] CONTEMPLAR que `ProxyJump` admite varios saltos separados por coma (`a,b,c`) y la forma
      `user@host:port`. El caso de un salto es el comun; para la cadena, OpenSSH anida. Decidir si
      se soporta solo el primer salto o la cadena entera, y si no se soporta, que avise claro
      en vez de fallar con un error de DNS que no dice nada
- [x] TEST: fixture de `~/.ssh/config` con `ProxyJump`, y assert de que el tunel recibe un
      `ssh_proxy_command` no vacio. Hoy ese caso no esta cubierto, por eso paso
- [x] OJO CON EL MENSAJE DE ERROR, que es la mitad del problema: `Name or service not known` mando
      la investigacion a la red y al DNS cuando el defecto estaba en el parseo del config. Vale
      agregar contexto al error diciendo QUE nombre no resolvio y si habia un proxy configurado
- [x] PUEDE SER PR AL REPO OFICIAL: no depende de nada nuestro, el codigo de arriba es comun
- [x] WORKAROUND YA APLICADO en `t` el 2026-09-16, y no es el fix: en `~/.ssh/config` se cambio
      `ProxyJump X` por `ProxyCommand ssh -W %h:%p X` en los tres bloques de esa red. Backup en
      `~/.ssh/config.bak-20260916-1139`. Cuando el fork tenga el fix, se puede volver atras

2026-08-31
===================
- [x] Triage upstream: REPRODUCCION e2e de #1518/#1484 (crash de bytes con SQL_ASCII). Resumen en la seccion de triage de Upcoming
  - Setup: venv con el stack exacto del reporter (pgcli 4.3.0 + psycopg 3.2.3 en /tmp/repro430) + cluster PG 18.6 SQL_ASCII desechable en /tmp/repro-sqla (puerto 15532, socket en /var/run/postgresql, corriendo como postgres). pty via `script -qc` porque 4.3.0 no tiene -c/-f
  - 4.3.0: los DOS tracebacks del issue, identicos (prompt: main.py:1323 get_prompt en el replace de \\H; completion thread: escape_name en set_search_path). Captura en /tmp/repro430_e2e.out
  - Fork instalada contra el mismo cluster: MISMO crash de prompt (main.py:1908) + mensaje de timezone con b'...' (get_timezone hace str() sobre bytes). Captura en /tmp/fork_e2e.out
  - El mensaje verde que no aparecia en NUESTRO repro: la config real de Diego trae `use_local_timezone = False` y la 4.3.0 mergea la config real via load_config aun con --pgclirc apuntando a vacio, asi que el bloque entero se saltea. El reporter tenia el default True. En la prueba de la fork se uso XDG_CONFIG_HOME aislado con el flag en True
  - Descartado red herring: ConnectionInfo.get_parameters() decodifica todo a str, no es la fuente de los bytes
  - Cluster detenido; scratch en /tmp (repro430, repro-sqla, forkcfg, los dos .out) sin limpiar

2026-08-21
===================
- [x] PR #1622 (--timeout): review de dbaty ("Nice addition, thanks!") con 3 pedidos, los 3 aplicados y pusheados (97696ea) + respuesta en el PR
  - (1) leer el config con `c["main"].as_int("connect_timeout")` en __init__ en vez de `.get()` + try/except int en connect(): es el patron del proyecto (row_limit, min_num_menu_lines), la clave siempre existe porque `connect_timeout = 30` viene en el pgclirc default, y un valor mal tipeado se reporta en vez de ignorarse en silencio
  - (2) extraido `get_connect_timeout(explicit, dsn, kwargs, default)`: la tabla de precedencia es ahora un test parametrizado que lo llama directo, mas 2 tests end-to-end que prueban que esta enganchado en connect()
  - (3) comentario desactualizado en pgexecute (solo aplicaba upstream; nuestro fork ya lo tenia correcto porque su implementacion es distinta)
  - Portado (1) y (2) a nuestro fork. Suite: 3142 + 27 passed, 0 failed. Verificacion adversarial: el test de validacion falla con el codigo viejo
  - OJO: exportar PGHOST/PGPORT/PGUSER para habilitar los @dbtest rompe 2 tests de test_isready.py (leen el entorno real). Correr esa suite aparte con `env -u PGHOST -u PGPORT -u PGUSER`. Es aislamiento flojo preexistente, no un bug nuestro
- [x] Deploy de 4.5.8 en la maquina `d` (Linux Mint 22.2), dejandola identica a `t`. Estado inicial: ya tenia el CODIGO 4.5.8 pero con dependencias VIEJAS (paramiko 3.5.1, sqlparse 0.5.3 con los 4 CVEs, psycopg 3.2.13, cli_helpers 2.7.0) porque `uv tool install --force` NO refresca deps que sigan satisfaciendo el rango: hace falta `--reinstall`
  - Reinstalado con `uv tool install --force --reinstall --python 3.12 "dist/pgcli-4.5.8-py3-none-any.whl[sshtunnel,keyring]"` (el venv corria 3.10.17, ahora 3.12 como en `t`). Las 28 deps coinciden una a una
  - Instalado el bash completion (no estaba): copia estable en ~/.local/share/bash-completion/completions/pgcli + source en ~/.bashrc. 103 alias en el tab de --dsn
  - Copiados los 11 ~/.psqlrc* (en `d` solo estaba ~/.psqlrc y desactualizado: le faltaban 44 \set). Backup previo
  - dsn.d: `d` tenia un alias que no estaba en `t` (creado el 20/08 ahi mismo). Le agregue connect_timeout=15 y lo copie a `t`. Ahora 104 archivos, md5 identico en las dos
  - Repo: HEAD estaba en ef9c378 con 8 archivos "modificados" que en realidad YA eran el contenido de 9d6d114 (syncthing sincroniza el working tree pero NO el .git). Verificado archivo por archivo contra fork/main y realineado con `git reset --mixed fork/main` (no toca el working tree). Limpio en v4.5.8
  - Test funcional en ambas maquinas contra un DSN de dev: PONG, query real, 110 named queries cargadas de 116 (filtro por version del server andando), --timeout, error amigable de \cmd invalido, y el fix de -l con connection string. Salida identica

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
