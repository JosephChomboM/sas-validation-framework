# Diseño del Framework (SAS Viya / CAS)

## 1) Alcance

Este documento describe:
- Componentes del framework y responsabilidades.
- Steps como frontend de configuración del usuario.
- Ejecución orientada por contexto: primero seleccionar data (troncal/scope/split), luego módulos.
- Contratos de rutas y naming.
- Contrato de configuración vía `config.sas` (tablas CAS) y `steps/*.sas` (parámetros).
- Orden de ejecución con contexto unificado: un solo step de contexto + módulos.

---

## 2) Arquitectura lógica

### 2.1 Capas

1) **Steps (Frontend)**
- Archivos `steps/*.sas` que actúan como formularios de configuración.
- El usuario edita estos archivos para definir parámetros del run.
- Se ejecutan secuencialmente al inicio de `runner/main.sas`.
- Flujo de steps:
  - `01_setup_project.sas` ? rutas del proyecto
  - `02_load_config.sas` ? carga/validación de `config.sas`, promote tablas config
  - `03_create_folders.sas` ? creación de estructura de carpetas (incluye `troncal_X/train/oot/` por cada troncal en config)
  - `04_import_raw_data.sas` ? importación ADLS (una vez por proyecto)
  - `05_partition_data.sas` ? materialización processed (universo + segmentos)
  - **Contexto + módulos (unificado):**
    - `context_and_modules.sas` ? seleccionar scope (UNIVERSO|SEGMENTO), troncal, split, segmento, y módulos a ejecutar
    - `methods/metod_N/step_<modulo>.sas` ? ejecución (lee `ctx_scope` para iterar base o segmentos)

2) **Configuración**
- Fuente: `config.sas` (generado desde HTML).
- Contiene DATA steps que crean `casuser.cfg_troncales` y `casuser.cfg_segmentos`.
- Los parámetros de usuario (rutas, ADLS, métodos) viven en `steps/*.sas`, no en config.

3) **Common**
- Utilidades reutilizables:
  - paths
  - logging
  - validaciones genéricas
  - utilidades CAS (existence, nobs, load/save)
  - preparación de data raw ? processed

4) **Dispatch**
- Orquestación de ejecución:
  - `run_module.sas`: ejecuta un módulo en un contexto dado (troncal/split/segmento). Resuelve path, promueve input, llama al módulo, limpia.

5) **Modules**
- Implementación por control:
  - API pública (`*_run.sas`)
  - Validaciones (`*_contract.sas`)
  - Implementación interna (`impl/`)
- Módulos implementados: `correlacion` (referencia), `psi`, `universe`, `target`. Pendientes: `gini`.
- `run_module.sas` incluye dinámicamente `<modulo>_run.sas` y ejecuta `%<modulo>_run(...)`.

6) **Runner**
- `runner/main.sas`: entrypoint único, reemplaza `.flw`.
- Ejecuta:
  - **Frontend**: incluye steps de setup, data prep, promoción de contexto y configuración de métodos.
  - **Backend**: CAS init ? prepare/promote por contexto ? ejecutar subflow de módulos ? cleanup.

---

## 3) Contratos de rutas y naming

### 3.1 Raw
- `data/raw/mydataset.sashdat` (dataset maestro)

### 3.2 Processed (inputs de controles)
Cada troncal se materializa por split:

- Universo:
  - `data/processed/troncal_X/train/base.sashdat`
  - `data/processed/troncal_X/oot/base.sashdat`

- Segmentos (numéricos):
  - `data/processed/troncal_X/train/segNNN.sashdat`
  - `data/processed/troncal_X/oot/segNNN.sashdat`

Reglas:
- `base.sashdat` es siempre el universo.
- `segNNN.sashdat` usa padding de 3 dígitos (001..999).
- No se incluyen “train/oot” ni “troncal” en el nombre del archivo (ya están en la ruta).

### 3.3 Outputs por run
- `outputs/runs/<run_id>/reports`
- `outputs/runs/<run_id>/reports/METOD1.1` - universe
- `outputs/runs/<run_id>/reports/METOD2.1` - target
- `outputs/runs/<run_id>/reports/METOD4.2` - PSI
- `outputs/runs/<run_id>/reports/METOD4.3` - correlación
- `outputs/runs/<run_id>/images`
- `outputs/runs/<run_id>/images/METOD1.1` - universe charts
- `outputs/runs/<run_id>/images/METOD2.1` - target charts
- `outputs/runs/<run_id>/images/METOD4.2` - PSI charts
- `outputs/runs/<run_id>/tables`
- `outputs/runs/<run_id>/tables/METOD4.2` - PSI tables
- `outputs/runs/<run_id>/tables/METOD4.3` - correlacion tables
- `outputs/runs/<run_id>/experiments` - outputs de análisis exploratorio (modo CUSTOM de módulos)

---

## 4) Contrato de configuración: `config.sas`

### 4.1 Principio
El `config.sas` declara parámetros; el framework ejecuta. Se evita lógica de orquestación en el config.

**Las tablas de configuración (`casuser.cfg_troncales`, `casuser.cfg_segmentos`) residen en `casuser`.** Además, `casuser` se usa como librería para tablas temporales/intermedias de los módulos (reemplazando `work`). Todas las tablas temporales se eliminan al finalizar cada módulo. Todo dato operativo persistente (raw, processed, outputs) usa CASLIBs PATH-based dedicados.

### 4.2 Parámetros por troncal
Se recomienda declarar, por troncal:
- Identificadores:
  - `troncal_id` (ej. 1, 2)
- Variables:
  - target
  - pd / xb (según aplique)
  - monto
  - mes_var (variable de corte temporal, por ejemplo YYYYMM)
- Rango train/oot:
  - train_min_mes, train_max_mes
  - oot_min_mes, oot_max_mes
- Fecha de cierre para controles con target:
  - def_cld - fecha maxima (YYYYMM) para controles que usan target, PD o XB (ratio de default). Para controles que solo analizan variables (ej. correlacion, PSI), usar `oot_max_mes` en su lugar.
- Listas de variables:
  - var_num_list, var_cat_list
  - drv_num_list, drv_cat_list
- Segmentación (opcional):
  - var_seg (variable segmentadora)
  - n_segments (N)

Nota:
- Estas “listas” se almacenan como strings (separadas por espacio) para permitir iteración fácil.

### 4.3 Parámetros por segmento (opcional)
Si el usuario define overrides por segmento, se recomienda declarar por (troncal, seg_id):
- var_num_list, var_cat_list
- drv_num_list, drv_cat_list

Regla:
- Si no hay override, el segmento hereda las listas del troncal.

---

## 5) Steps como frontend (reemplazo de .step)

### 5.1 Concepto
Los archivos `steps/*.sas` actúan como el **frontend** del framework: son el punto de entrada donde el usuario configura todos los parámetros antes de ejecutar el pipeline.

En SAS Viya Studio, un `.step` ofrece un formulario gráfico. Como no se utilizan `.step`, los archivos `steps/*.sas` simulan esa experiencia:
- Cada archivo es un **formulario editable** con variables `_id_*` documentadas.
- El usuario modifica los valores `%let` según su caso de uso.
- Al ejecutar `runner/main.sas`, los steps se incluyen secuencialmente y setean las macro variables.
- Algunos steps ejecutan acciones automáticas (ej. Step 03 crea carpetas).
- El contexto de ejecución se define antes de seleccionar módulos.

**Independencia de steps:** cada step es autónomo y carga sus propias dependencias:
- Todo step que use macros del framework incluye `%include "&fw_root./src/common/common_public.sas";` al inicio (es idempotente).
- Todo step que use CASLIBs operativos (RAW, PROC, OUT) los crea al inicio y los dropea al final, siguiendo `caslib_lifecycle.md`.
- Las tablas promovidas se eliminan al finalizar el step (junto con el CASLIB que las contiene).
- `casuser` es la excepción: es el CASLIB de sesión para config y no se dropea entre steps.

### 5.2 Flujo de steps

| Step | Archivo                                      | Qué configura                                                               | Macro vars que setea                                                                                                                                                                                                  |
| ---- | -------------------------------------------- | --------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 01   | `steps/01_setup_project.sas`                 | Rutas del proyecto                                                          | `&fw_root`, `&fw_sas_dataset_name`                                                                                                                                                                                    |
| 02   | `steps/02_load_config.sas`                   | Cargar/validar `config.sas` + promote config + crear dirs de output del run | `cfg_troncales`, `cfg_segmentos`, `&run_id`                                                                                                                                                                           |
| 03   | `steps/03_create_folders.sas`                | Carpetas de data + troncal dirs (solo data prep)                            | (N/A)                                                                                                                                                                                                                 |
| 04   | `steps/04_import_raw_data.sas`               | Importación ADLS                                                            | `&adls_import_enabled`, `&adls_*`, `&raw_table`                                                                                                                                                                       |
| 05   | `steps/05_partition_data.sas`                | Particiones universo/segmento                                               | (N/A)                                                                                                                                                                                                                 |
| -    | `steps/context_and_modules.sas`              | Contexto (scope + troncal + split + seg) + módulos habilitados              | `&ctx_scope`, `&ctx_troncal_id`, `&ctx_split`, `&ctx_seg_id`, `&ctx_n_segments`, `&run_universe`, `&run_estabilidad`, `&run_fillrate`, `&run_missings`, `&run_psi`, `&run_bivariado`, `&run_correlacion`, `&run_gini` |
| -    | `steps/methods/metod_1/step_universe.sas`    | Config + ejecución universe (1.1)                                           | (dual_input)                                                                                                                                                                                                          |
| -    | `steps/methods/metod_2/step_target.sas`      | Config + ejecución target (2.1)                                             | (dual_input)                                                                                                                                                                                                          |
| -    | `steps/methods/metod_4/step_correlacion.sas` | Config + ejecución correlación                                              | `&corr_mode`, `&corr_custom_vars`                                                                                                                                                                                     |
| -    | `steps/methods/metod_4/step_psi.sas`         | Config + ejecución PSI                                                      | `&psi_mode`, `&psi_n_buckets`, `&psi_mensual`                                                                                                                                                                         |
| -    | `steps/methods/metod_4/step_gini.sas`        | Config + ejecución gini (futuro)                                            | -                                                                                                                                                                                                                     |

**Step 02** genera `run_id`, carga `config.sas`, promueve `cfg_troncales` y `cfg_segmentos` (necesario para background submit), y crea las carpetas de output del run (`outputs/runs/<run_id>/logs|reports|images|tables|experiments`). Las subcarpetas por método (`METOD1.1/`, `METOD4.2/`, `METOD4.3/`) dentro de `reports/`, `images/` y `tables/` se crean dinámicamente por cada módulo cuando genera archivos.

**Step 03** crea las carpetas de data (`data/raw`, `data/processed`) y las subcarpetas `troncal_X/train/` y `troncal_X/oot/` por cada troncal en `casuser.cfg_troncales`. Solo se ejecuta durante data prep (`data_prep_enabled=1`).

**`context_and_modules.sas`** unifica la selección de contexto (scope, troncal, split, segmento) y la habilitación de módulos en un solo step. Los steps de módulos (en `steps/methods/`) leen `&ctx_scope` y los flags `&run_<modulo>` para decidir qué ejecutar.

### 5.3 Convención de IDs `_id_*`

Los IDs de UI se nombran como: `_id_<campo>`

Ejemplos:
- `_id_project_root` (Step 01)
- `_id_import_enabled`, `_id_adls_storage`, `_id_adls_container`, `_id_adls_parquet_path`, `_id_raw_table_name` (Step 04)
- Contexto: `_id_scope`, `_id_troncal_id`, `_id_split`, `_id_seg_id`, `_id_seg_num` (context_and_modules)
- Módulos: `_id_run_estabilidad`, `_id_run_fillrate`, `_id_run_missings`, `_id_run_psi`, `_id_run_bivariado`, `_id_run_correlacion`, `_id_run_gini` (context_and_modules)

### 5.4 Relación entre steps y config.sas
- **Steps**: parámetros simples (rutas, flags, listas). El usuario los edita como un formulario.
- **config.sas**: configuración compleja (DATA steps que generan tablas CAS por troncal/segmento). Generado desde HTML o editado manualmente.
- Ambos se cargan por `runner/main.sas`; la ejecución de módulos depende del contexto promovido por los steps de contexto.

### 5.5 Contrato de Métodos (agrupación lógica)
- Cada Método (`Metodo 1..4`) es una agrupación lógica de módulos.
- Los módulos de cada método van en `steps/methods/metod_N/`.
- En el `.flw`, cada módulo es un **nodo independiente** que puede ejecutarse via background submit.
- Cada step de módulo es auto-contenido: tiene su config, crea CASLIBs, itera seg+unv, limpia.

| Método   | Sub-método | Carpeta                  | Módulos                                  |
| -------- | ---------- | ------------------------ | ---------------------------------------- |
| Metodo 1 | 1.1        | `steps/methods/metod_1/` | **universe**                             |
| Metodo 2 | 2.1        | `steps/methods/metod_2/` | **target**                               |
| Metodo 3 | -          | `steps/methods/metod_3/` | segmentacion (futuro)                    |
| Metodo 4 | 4.2        | `steps/methods/metod_4/` | estabilidad, fillrate, missings, **psi** |
| Metodo 4 | 4.3        | `steps/methods/metod_4/` | bivariado, **correlacion**, gini         |

Los sub-métodos definen la agrupación lógica para la selección en el UI y la organización de carpetas de output (`reports/METOD1.1/`, `reports/METOD4.2/`, `reports/METOD4.3/`). Los step files viven en la carpeta de su método correspondiente.

---

## 6) Orden de ejecución

### 6.1 Fases del pipeline

El pipeline se divide en dos fases con diferente frecuencia de ejecución:

**Fase A - Data Prep (una vez por proyecto, `data_prep_enabled=1`)**
1. Setup de rutas (Step 01).
2. Carga de `config.sas` + creación de dirs del run (Step 02).
3. Creación de carpetas de data + troncal dirs (Step 03).
4. Importación ADLS (Step 04, opcional).
5. Partición y persistencia processed (Step 05).

**Fase B - Ejecución (cada corrida, siempre)**
1. Setup de rutas (Step 01).
2. Carga de `config.sas` + promote config + creación de dirs del run (Step 02).
3. **Contexto + módulos** (`steps/context_and_modules.sas`):
   - Define `ctx_scope` (UNIVERSO|SEGMENTO), `ctx_troncal_id`, `ctx_split`, `ctx_seg_id`.
   - Habilita flags `run_<modulo>`.
   - Valida troncal, split y seg_id.
4. **Ejecución de steps de módulos** (`steps/methods/metod_N/step_<modulo>.sas`):
   - Cada step checa su flag `run_<modulo>`.
   - Lee `ctx_scope` para decidir iteración:
     - SEGMENTO ? itera segmentos via `ctx_n_segments`, `ctx_seg_id`
     - UNIVERSO ? ejecuta sobre base/troncal
   - Crea CASLIBs PROC/OUT, ejecuta, y limpia.

El flag `data_prep_enabled` (en `runner/main.sas`) controla si se ejecutan los Steps 03–05.
- Primera corrida: `data_prep_enabled=1` (crear carpetas de data, importar, particionar).
- Corridas posteriores: `data_prep_enabled=0` (los datos ya existen en disco; Steps 01–02 siempre corren para generar el `run_id` y crear dirs de output del run).

**Nota SAS:** `%if`/`%do` no se permiten en open code. **Todo** archivo `.sas` que necesite lógica condicional debe encapsularla dentro de un `%macro ... %mend;`. Esto aplica tanto a `runner/main.sas` (`%macro _main_pipeline`) como a cualquier step que use `%if` (ej. `_step02_load`, `_step04_import`, `_step05_partition`, `_ctx_seg_validate`, `_ctx_unv_validate`).

### 6.2 Ciclo de vida de CASLIBs (create ? promote ? work ? drop)

Todo bloque que usa CASLIBs sigue estrictamente este patrón:

```
1. %_create_caslib(...)       - crear CASLIB PATH-based
2. %_promote_castable(...)    - cargar .sashdat y promover tabla en CAS (idempotente: hace drop previo)
3. <trabajo>                  - ejecutar módulos / data prep
4. proc cas; table.dropTable  - eliminar tabla promovida (scope=session)
5. %_drop_caslib(... del_prom_tables=1) - eliminar CASLIB + tablas de CAS
```

**Nota:** `_promote_castable` es idempotente - ejecuta `table.dropTable` antes de load+promote para evitar colisiones en llamadas iterativas (múltiples splits/segmentos).

**Ningún CASLIB debe sobrevivir más allá de la fase que lo creó.**

Aplicación por fase:

| Fase                              | CASLIBs        | Crea                      | Dropea                             |
| --------------------------------- | -------------- | ------------------------- | ---------------------------------- |
| Data Prep - ADLS import (Step 04) | LAKEHOUSE, RAW | `fw_import_adls_to_cas`   | `fw_import_adls_to_cas` (al final) |
| Data Prep - Partición (Step 05)   | RAW, PROC      | `fw_prepare_processed`    | `fw_prepare_processed` (al final)  |
| Ejecución - módulo (step_*.sas)   | PROC, OUT      | inicio del step de módulo | final del step de módulo           |

**Regla de promote en ejecucion:** `run_module.sas` soporta dos modos:
- `dual_input=0` (default): promueve un solo input como `_active_input`. El modulo recibe `input_table=_active_input` + `split=<train|oot>`. Para correlacion, gini, etc.
- `dual_input=1`: promueve train + oot como `_train_input` y `_oot_input`. El modulo recibe `train_table=_train_input` + `oot_table=_oot_input`. Para PSI y futuros modulos que comparen ambos splits. El parametro `split=` se ignora.

En ambos modos, `run_module` dropea las tablas promovidas al finalizar.

**Validación post-promote:** `run_module.sas` verifica existencia del input promovido vía `proc sql` contra `dictionary.tables` (no usa `table.tableExists`).

### 6.3 Motivo de diseño

- Separar configuración de datos de la ejecución de módulos reduce ambigüedad operativa.
- Mantener Métodos independientes permite re-ejecutar análisis sin acoplamiento.
- Un solo step de contexto unificado simplifica el flujo: el usuario elige scope (UNIVERSO|SEGMENTO) y los steps de módulos internamente iteran según corresponda.
- Módulos que solo aplican a un scope (ej. segmentación solo UNIVERSO) auto-saltan verificando `ctx_scope` internamente.
- El patrón create?promote?work?drop garantiza que no queden CASLIBs o tablas huérfanas en CAS.

---

## 7) Patrones de implementación recomendados

### 7.1 Resolver único de paths
Implementar `src/common/fw_paths.sas` con una macro pública que construya rutas de processed, evitando hardcode:
- `%fw_path_processed(outvar=, troncal_id=, split=, seg_id=)`
  - si `seg_id` vacío: devuelve `troncal_X/<split>/base`
  - si `seg_id` presente: devuelve `troncal_X/<split>/segNNN`
- Las rutas **NO incluyen extensión** (`.sashdat` lo agrega el consumidor, ej. `_promote_castable`).
- Estas rutas son **relativas al CASLIB `PROC`** (con subdirs habilitado), no a `casuser`.

### 7.2 CAS utility macros
Implementar `src/common/cas_utils.sas` con las macros baseline definidas en `docs/caslib_lifecycle.md`:
- `%_create_caslib(...)` - crea CASLIB PATH-based
- `%_drop_caslib(...)` - dropea CASLIB y opcionalmente sus tablas
- `%_load_cas_data(...)` - carga .sashdat desde CASLIB
- `%_save_into_caslib(...)` - guarda tabla CAS como .sashdat
- `%_promote_castable(...)` - promueve tabla (temporal; el caller debe limpiar)

Estas macros se incluyen vía `src/common/common_public.sas`.

### 7.3 Importación opcional desde ADLS
`fw_import_adls_to_cas` (en `src/common/preparation/`) permite:
- Crear CASLIB temporal `LAKEHOUSE` apuntando a Azure Data Lake Storage (parquet).
- Crear CASLIB `RAW` (PATH?`data/raw/`).
- Cargar tabla parquet ? CAS ? persistir como `.sashdat` en `data/raw/`.
- Cleanup: dropear CASLIBs `LAKEHOUSE` **y** `RAW` al finalizar (archivos en disco persisten).
- Controlado por `&adls_import_enabled` (seteado en `steps/04_import_raw_data.sas`); si vale `0` se salta completamente.

### 7.4 Preparación idempotente
`fw_prepare_processed` debe:
- Crear CASLIB `RAW` (PATH?`data/raw/`) y CASLIB `PROC` (PATH?`data/processed/`, subdirs=1)
- Leer raw desde CASLIB `RAW`, filtrar por ventanas mes, guardar como `.sashdat` en CASLIB `PROC`
- Sobrescribir outputs processed de manera controlada
- Limpiar tablas temporales CAS
- Loggear conteos (nobs) para auditoría mínima
- **No dejar tablas operativas en `casuser`**; solo temporales que se dropean al final
- **Cleanup al finalizar**: dropear CASLIBs `RAW` y `PROC` (los `.sashdat` en disco persisten)

### 7.5 Contratos y validaciones
Cada módulo debe fallar temprano con mensajes claros si:
- faltan columnas
- el input está vacío
- el split/segmento no existe

**Método de validación de existencia:** usar `proc sql` contra `dictionary.tables` o `count(*)` directo sobre la tabla. **Nunca usar `proc cas; table.tableExists`** (no es confiable en todos los entornos SAS Viya).

### 7.6 Límite de 32 caracteres en nombres de datasets SAS
SAS impone un máximo de **32 bytes** para nombres de datasets (`.sas7bdat`). Los nombres largos generan `ERROR 307-185`.

**Convención compacta para nombres de tablas .sas7bdat:**
```
<mod_abbr>_t<N>_<spl>_<scope>_<tipo>
```

| Componente    | Abreviatura | Ejemplo                             |
| ------------- | ----------- | ----------------------------------- |
| Módulo        | 4 chars max | `corr`, `gini`, `psi`               |
| Troncal       | `t<N>`      | `t1`, `t2`                          |
| Split         | 3 chars     | `trn` (train), `oot`                |
| Scope         | variable    | `base`, `seg001`                    |
| Tipo          | 4 chars max | `prsn` (pearson), `sprm` (spearman) |
| CUSTOM prefix | `cx_`       | `cx_corr_t1_trn_base_prsn`          |

Ejemplo: `corr_t1_trn_seg001_prsn` = 24 chars (? = 32).

**Reportes (.html, .xlsx)** no tienen límite de 32 chars (nombres de archivo del filesystem). Usan nombres descriptivos completos: `correlacion_troncal_1_train_seg001.html`.

### 7.7 Outputs tabulares: .sas7bdat vía libname (no CAS)
Los módulos persisten tablas de resultados como **`.sas7bdat`** usando `libname` + DATA step directo:
```sas
libname _outlib "&_tables_path.";
data _outlib.&_tbl_prefix._prsn;
  set casuser._corr_pearson;
run;
libname _outlib clear;
```
**No usar `_save_into_caslib` ni CAS para outputs tabulares de módulos.** CAS se usa para inputs (load/promote de `.sashdat` vía `_promote_castable`) y para tablas temporales/intermedias.

### 7.9 Convenciones ODS para reportes
Todos los módulos siguen estas convenciones para generación de reportes:

- **Formato de imagen**: JPEG (`imagefmt=jpeg`). No usar SVG ni PNG.
- **HTML5**: siempre con `options(bitmap_mode="inline")` para embeber imágenes directamente.
- **Imágenes en Excel**: los gráficos deben ir **tanto en el Excel** (hoja dedicada) **como en archivos JPEG independientes** (vía `ods listing gpath`). Abrir ambos destinos ODS simultáneamente.
- **Reset**: después de cerrar cada destino ODS, ejecutar `ods graphics / reset=all;` seguido de `ods graphics off;`.
- **ODS graphics on**: usar `ods graphics on;` sin `outputfmt=` (sin forzar SVG).
- **Subcarpetas por método**: los reportes van a `reports/METOD<N.M>/`, imágenes a `images/METOD<N.M>/`.

Patrón estándar:
```sas
ods graphics on;
ods listing gpath="&images_path.";

ods html5 file="..." options(bitmap_mode="inline");
ods excel file="..." options(sheet_name="Data" ...);

ods graphics / imagename="..." imagefmt=jpeg;
/* ... proc sgplot / proc print ... */

ods excel options(sheet_name="Graficos" sheet_interval="now");
/* ... proc sgplot (va al Excel Y al listing/JPEG) ... */

ods excel close;
ods html5 close;
ods graphics / reset=all;
ods graphics off;
```

### 7.10 Persistencia de tablas: política de mínimos
Persistir **solo las tablas más importantes** como `.sas7bdat` para no saturar de archivos innecesarios.

- **PSI**: cubo, cubo_wide, resumen (3 tablas por ejecución).
- **Correlación**: pearson, spearman (2 tablas por split).
- **Universe**: **no persiste tablas** (análisis visual; resultados solo en HTML/Excel).
- Si un módulo genera muchas tablas intermedias, **no** persistirlas todas. Solo las que aporten valor para auditoría o downstream.

---

## 8) Decisiones explícitas del proyecto

- No se usa JSON como fuente de configuración; solo `config.sas` + `steps/*.sas`.
- No se usan `.flw` ni `.step` como artefactos ejecutables; se reemplaza con `runner/main.sas` + `steps/*.sas` como frontend.
- CASLIB/LIBNAME de salida: **`OUT`** (fijo, =8 chars). Segregación por path físico `outputs/runs/<run_id>/`.
- CASLIBs operativos: solo `RAW`, `PROC`, `OUT`. No usar aliases alternos.
- **`casuser`**: config (`cfg_troncales`, `cfg_segmentos`) + tablas temporales de módulos. Cada módulo limpia al finalizar.
- **Ciclo de vida**: create ? promote ? work ? drop. Ningún CASLIB sobrevive entre steps.
- **Independencia de steps**: cada step carga `common_public.sas` y gestiona sus propios CASLIBs.
- **Restricción open code**: `%if`/`%do` solo dentro de `%macro ... %mend;`.
- **Sub-métodos**: M1.1 (universe), M2.1 (target), M4.2 (estabilidad, fillrate, missings, psi), M4.3 (bivariado, correlacion, gini). Carpetas: `METOD1.1/`, `METOD2.1/`, `METOD4.2/`, `METOD4.3/`.
- **Modo AUTO/CUSTOM**: AUTO resuelve vars desde config; CUSTOM usa vars manuales y outputs van a `experiments/`.
- **ODS**: JPEG, bitmap_mode=inline, imágenes embebidas en Excel, `reset=all` (ver §7.9).
- **Tablas**: persistir solo las esenciales por módulo (ver §7.10).
- **CAS interop**: ver §7.11 para restricciones PROC FEDSQL / work.

---

## 9) Restricciones CAS y patrones de interoperabilidad

### 9.1 Operaciones NO soportadas directamente en CAS

CAS tiene limitaciones cuando tanto source como destination son CAS librefs:

| Operación | Error | Alternativa |
|---|---|---|
| `INSERT INTO casuser.x VALUES(...)` | `Update access is not supported` | Usar `work` para acumular, copiar a casuser al final |
| `DATA casuser.x; SET casuser.y;` | `Both source and destination include CAS libname` | Usar `PROC FEDSQL SESSREF=conn` |
| `PROC SORT data=casuser.x;` | No soportado in-place | Hacer sort en `work` o usar ORDER BY en FEDSQL |
| `PROC TRANSPOSE ... out=casuser.x` | Output to CAS no soportado | Transponer en `work`, copiar resultado a casuser |
| `PROC DATASETS lib=casuser; CHANGE` | Rename no soportado en CAS | Renombrar en `work` |
| `PROC FREQ out=casuser.x` | Output to CAS no confiable | Usar FEDSQL con count/group by |
| `PROC MEANS output out=casuser.x` | Output to CAS no confiable | Usar FEDSQL con avg/count |
| HAVING con alias (ej. `having N > 1`) | Alias no permitido en HAVING | Usar `having count(*) > 1` |

### 9.2 Dos patrones CAS-compatible

**Patron A - PROC FEDSQL (CAS-to-CAS):**
Para copias, filtros, agregaciones, JOINs donde source y destination son CAS:
```sas
proc fedsql sessref=conn;
    create table casuser._resultado {options replace=true} as
    select * from casuser._origen;
quit;
```
Usado por: `universe` (todo el computo se queda en CAS).

**Patron B - work como staging (iterativo):**
Para operaciones iterativas (INSERT INTO loops, PROC SORT, PROC TRANSPOSE, acumulacion):
1. Copiar CAS → work al inicio
2. Toda la iteracion/computo en work
3. Copiar los resultados finales de work → casuser al terminar
4. Limpiar work

Usado por: `psi` (cubo se acumula via INSERT INTO en work).

### 9.3 Operaciones que SI funcionan en CAS
- `PROC SQL; CREATE TABLE casuser.x AS SELECT ... FROM casuser.y;` (SELECT/CREATE)
- `PROC SGPLOT data=casuser.x;` (lectura)
- `PROC PRINT data=casuser.x;` (lectura)
- `PROC CORR data=casuser.x outp=casuser.y;` (lectura + output)
- `DATA work.x; SET casuser.y;` (CAS → work, unidireccional)
- `DATA casuser.x; SET work.y;` (work → CAS, unidireccional)

