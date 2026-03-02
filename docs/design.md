# Diseño del Framework (SAS Viya / CAS)

## 1) Alcance

Este documento describe:
- Componentes del framework y responsabilidades.
- Steps como frontend de configuración del usuario.
- Ejecución orientada por contexto: primero seleccionar data (troncal/scope/split), luego módulos.
- Contratos de rutas y naming.
- Contrato de configuración vía `config.sas` (tablas CAS) y `steps/*.sas` (parámetros).
- Orden de ejecución por bloques: **segmento** y luego **universo/troncal**.

---

## 2) Arquitectura lógica

### 2.1 Capas

1) **Steps (Frontend)**
- Archivos `steps/*.sas` que actúan como formularios de configuración.
- El usuario edita estos archivos para definir parámetros del run.
- Se ejecutan secuencialmente al inicio de `runner/main.sas`.
- Flujo de steps:
  - `01_setup_project.sas` → rutas del proyecto
  - `02_load_config.sas` → carga/validación de `config.sas`
  - `03_create_folders.sas` → creación de estructura de carpetas (incluye `troncal_X/train/oot/` por cada troncal en config)
  - `04_import_raw_data.sas` → importación ADLS (una vez por proyecto)
  - `05_partition_data.sas` → materialización processed (universo + segmentos)
  - `06_promote_segment_context.sas` → seleccionar contexto segmento
  - `07_config_methods_segment.sas` → seleccionar módulos por Método (segmento)
  - `08_run_methods_segment.sas` → ejecutar subflow de módulos (segmento)
  - `09_promote_universe_context.sas` → seleccionar contexto universo
  - `10_config_methods_universe.sas` → seleccionar módulos por Método (universo)
  - `11_run_methods_universe.sas` → ejecutar subflow de módulos (universo)

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
  - preparación de data raw → processed

4) **Dispatch**
- Orquestación de ejecución:
  - `run_method.sas`: ejecuta un método (conjunto de módulos)
  - `run_module.sas`: ejecuta un módulo en un contexto dado (troncal/split/segmento)

5) **Modules**
- Implementación por control:
  - API pública (`*_run.sas`)
  - Validaciones (`*_contract.sas`)
  - Implementación interna (`impl/`)
- Módulos implementados: `correlacion` (referencia). Pendientes: `gini`, `psi`.
- `run_module.sas` incluye dinámicamente `<modulo>_run.sas` y ejecuta `%<modulo>_run(...)`.

6) **Runner**
- `runner/main.sas`: entrypoint único, reemplaza `.flw`.
- Ejecuta:
  - **Frontend**: incluye steps de setup, data prep, promoción de contexto y configuración de métodos.
  - **Backend**: CAS init → prepare/promote por contexto → ejecutar subflow de módulos → cleanup.

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
- `outputs/runs/<run_id>/logs`
- `outputs/runs/<run_id>/reports`
- `outputs/runs/<run_id>/images`
- `outputs/runs/<run_id>/tables`
- `outputs/runs/<run_id>/manifests`
- `outputs/runs/<run_id>/experiments` — outputs de análisis exploratorio (modo CUSTOM de módulos)

---

## 4) Contrato de configuración: `config.sas`

### 4.1 Principio
El `config.sas` declara parámetros; el framework ejecuta. Se evita lógica de orquestación en el config.

**Las tablas de configuración (`casuser.cfg_troncales`, `casuser.cfg_segmentos`) son las únicas tablas que residen en `casuser`.** Todo dato operativo (raw, processed, outputs) usa CASLIBs PATH-based dedicados.

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

| Step | Archivo | Qué configura | Macro vars que setea |
|------|---------|---------------|---------------------|
| 01 | `steps/01_setup_project.sas` | Rutas del proyecto | `&fw_root` |
| 02 | `steps/02_load_config.sas` | Cargar/validar `config.sas` + crear dirs de output del run | `cfg_troncales`, `cfg_segmentos`, `&run_id` |
| 03 | `steps/03_create_folders.sas` | Carpetas de data + troncal dirs (solo data prep) | (N/A) |
| 04 | `steps/04_import_raw_data.sas` | Importación ADLS | `&adls_import_enabled`, `&adls_*`, `&raw_table` |
| 05 | `steps/05_partition_data.sas` | Particiones universo/segmento | (N/A) |
| 06 | `steps/06_promote_segment_context.sas` | Contexto segmento | `&ctx_troncal_id`, `&ctx_split`, `&ctx_seg_id` |
| 07 | `steps/07_config_methods_segment.sas` | Métodos (segmento) + params módulos | `&metodo_*_modules`, `&metodo_*_enabled`, `&corr_mode`, `&corr_custom_vars` |
| 08 | `steps/08_run_methods_segment.sas` | Ejecutar subflow segmento | (N/A) |
| 09 | `steps/09_promote_universe_context.sas` | Contexto universo | `&ctx_troncal_id`, `&ctx_split=base` |
| 10 | `steps/10_config_methods_universe.sas` | Métodos (universo) + params módulos | `&metodo_*_modules`, `&metodo_*_enabled`, `&corr_mode`, `&corr_custom_vars` |
| 11 | `steps/11_run_methods_universe.sas` | Ejecutar subflow universo | (N/A) |

**Step 02** genera `run_id`, carga `config.sas`, y crea las carpetas de output del run (`outputs/runs/<run_id>/logs|reports|images|tables|manifests|experiments`). Estas carpetas se crean **siempre** (cada corrida) porque son específicas del run.

**Step 03** crea las carpetas de data (`data/raw`, `data/processed`) y las subcarpetas `troncal_X/train/` y `troncal_X/oot/` por cada troncal en `casuser.cfg_troncales`. Solo se ejecuta durante data prep (`data_prep_enabled=1`).

Los steps de promoción (`06` y `09`) son obligatorios antes de ejecutar módulos: primero se promueve el contexto de datos, luego se configura qué módulos correr.

### 5.3 Convención de IDs `_id_*`

Los IDs de UI se nombran como: `_id_<entidad>_<campo>`

Ejemplos:
- `_id_project_root` (Step 01)
- `_id_import_enabled`, `_id_adls_storage`, `_id_adls_container`, `_id_adls_parquet_path`, `_id_raw_table_name` (Step 04)
- `_id_ctx_troncal_id`, `_id_ctx_split`, `_id_ctx_seg_id` (Step 06)
- `_id_metodo_1_enabled`, `_id_metodo_1_modules`, ..., `_id_metodo_n_enabled`, `_id_metodo_n_modules` (Steps 07/10)

### 5.4 Relación entre steps y config.sas
- **Steps**: parámetros simples (rutas, flags, listas). El usuario los edita como un formulario.
- **config.sas**: configuración compleja (DATA steps que generan tablas CAS por troncal/segmento). Generado desde HTML o editado manualmente.
- Ambos se cargan por `runner/main.sas`; la ejecución de módulos depende del contexto promovido por los steps de contexto.

### 5.5 Contrato de Métodos (tabs/hojas)
- Cada Método (`Metodo 1..N`) es una agrupación lógica de módulos (ej. estabilidad, fillrate, missings, psi).
- Los Métodos son **independientes**: no tienen dependencia obligatoria entre sí.
- Cada Método define:
  - `enabled` (0/1)
  - `module_list` (lista de módulos seleccionados)
  - `scope` de ejecución (segmento o universo según bloque actual)
- Regla operativa: por defecto, la selección de módulos se hace sobre **segmento**; universo se configura y ejecuta en su bloque dedicado.

---

## 6) Orden de ejecución

### 6.1 Fases del pipeline

El pipeline se divide en dos fases con diferente frecuencia de ejecución:

**Fase A — Data Prep (una vez por proyecto, `data_prep_enabled=1`)**
1. Setup de rutas (Step 01).
2. Carga de `config.sas` + creación de dirs del run (Step 02).
3. Creación de carpetas de data + troncal dirs (Step 03).
4. Importación ADLS (Step 04, opcional).
5. Partición y persistencia processed (Step 05).

**Fase B — Ejecución (cada corrida, siempre)**
1. Setup de rutas (Step 01).
2. Carga de `config.sas` + creación de dirs del run (Step 02).
6. Promoción de contexto de **segmento** (Step 06).
7. Configuración de Métodos y módulos para segmento.
8. Ejecución de subflow de módulos para segmento.
9. Promoción de contexto de **universo**.
10. Configuración de Métodos y módulos para universo.
11. Ejecución de subflow de módulos para universo.

El flag `data_prep_enabled` (en `runner/main.sas`) controla si se ejecutan los Steps 03–05.
- Primera corrida: `data_prep_enabled=1` (crear carpetas de data, importar, particionar).
- Corridas posteriores: `data_prep_enabled=0` (los datos ya existen en disco; Steps 01–02 siempre corren para generar el `run_id` y crear dirs de output del run).

**Nota SAS:** `%if`/`%do` no se permiten en open code. **Todo** archivo `.sas` que necesite lógica condicional debe encapsularla dentro de un `%macro ... %mend;`. Esto aplica tanto a `runner/main.sas` (`%macro _main_pipeline`) como a cualquier step que use `%if` (ej. `_step02_load`, `_step04_import`, `_step05_partition`, `_step06_validate`, `_step09_validate`).

### 6.2 Ciclo de vida de CASLIBs (create → promote → work → drop)

Todo bloque que usa CASLIBs sigue estrictamente este patrón:

```
1. %_create_caslib(...)       — crear CASLIB PATH-based
2. %_promote_castable(...)    — cargar .sashdat y promover tabla en CAS
3. <trabajo>                  — ejecutar módulos / data prep
4. proc cas; table.dropTable  — eliminar tabla promovida
5. %_drop_caslib(... del_prom_tables=1) — eliminar CASLIB + tablas de CAS
```

**Ningún CASLIB debe sobrevivir más allá de la fase que lo creó.**

Aplicación por fase:

| Fase | CASLIBs | Crea | Dropea |
|------|---------|------|--------|
| Data Prep — ADLS import (Step 04) | LAKEHOUSE, RAW | `fw_import_adls_to_cas` | `fw_import_adls_to_cas` (al final) |
| Data Prep — Partición (Step 05) | RAW, PROC | `fw_prepare_processed` | `fw_prepare_processed` (al final) |
| Ejecución segmento (Step 08) | PROC, OUT | inicio de `run_methods_segment_context` | final de `run_methods_segment_context` |
| Ejecución universo (Step 11) | PROC, OUT | inicio de `run_methods_universe_context` | final de `run_methods_universe_context` |

**Regla de promote en ejecución:** `run_module.sas` promueve el input específico (vía `%_promote_castable`) antes de ejecutar el módulo, y dropea la tabla promovida (`_active_input`) después. Los módulos reciben `input_table=_active_input` en vez de una ruta.

### 6.3 Motivo de diseño

- Separar contexto de datos y selección de módulos reduce ambigüedad operativa.
- Mantener Métodos independientes permite re-ejecutar análisis sin acoplamiento.
- Ejecutar primero segmento y luego universo mantiene diagnóstico granular antes del agregado.
- El patrón create→promote→work→drop garantiza que no queden CASLIBs o tablas huérfanas en CAS.

---

## 7) Patrones de implementación recomendados

### 7.1 Resolver único de paths
Implementar `src/common/fw_paths.sas` con una macro pública que construya rutas de processed, evitando hardcode:
- `%fw_path_processed(outvar=, troncal_id=, split=, seg_id=)`
  - si `seg_id` vacío: devuelve `troncal_X/<split>/base.sashdat`
  - si `seg_id` presente: devuelve `troncal_X/<split>/segNNN.sashdat`
- Estas rutas son **relativas al CASLIB `PROC`** (con subdirs habilitado), no a `casuser`.

### 7.2 CAS utility macros
Implementar `src/common/cas_utils.sas` con las macros baseline definidas en `docs/caslib_lifecycle.md`:
- `%_create_caslib(...)` — crea CASLIB PATH-based
- `%_drop_caslib(...)` — dropea CASLIB y opcionalmente sus tablas
- `%_load_cas_data(...)` — carga .sashdat desde CASLIB
- `%_save_into_caslib(...)` — guarda tabla CAS como .sashdat
- `%_promote_castable(...)` — promueve tabla (temporal; el caller debe limpiar)

Estas macros se incluyen vía `src/common/common_public.sas`.

### 7.3 Importación opcional desde ADLS
`fw_import_adls_to_cas` (en `src/common/preparation/`) permite:
- Crear CASLIB temporal `LAKEHOUSE` apuntando a Azure Data Lake Storage (parquet).
- Crear CASLIB `RAW` (PATH→`data/raw/`).
- Cargar tabla parquet → CAS → persistir como `.sashdat` en `data/raw/`.
- Cleanup: dropear CASLIBs `LAKEHOUSE` **y** `RAW` al finalizar (archivos en disco persisten).
- Controlado por `&adls_import_enabled` (seteado en `steps/04_import_raw_data.sas`); si vale `0` se salta completamente.

### 7.4 Preparación idempotente
`fw_prepare_processed` debe:
- Crear CASLIB `RAW` (PATH→`data/raw/`) y CASLIB `PROC` (PATH→`data/processed/`, subdirs=1)
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

---

## 8) Decisiones explícitas del proyecto

- No se usa JSON como fuente de configuración; solo `config.sas` + `steps/*.sas`.
- No se usan `.flw` ni `.step` como artefactos ejecutables; se reemplaza con `runner/main.sas` + `steps/*.sas` como frontend.
- Los parámetros de usuario (rutas, ADLS, métodos) viven en `steps/*.sas`; la configuración de troncales/segmentos en `config.sas`.
- El CASLIB/LIBNAME de salida del framework es **`OUT`** (fijo) para respetar el límite de 8 caracteres en `LIBNAME` de SAS.
- La segregación por corrida se mantiene por path físico `outputs/runs/<run_id>/`, no por nombre de CASLIB.
- Para evitar ambigüedad operativa, en data del framework solo se permiten estos nombres de CASLIB/LIBNAME:
  - **`RAW`** para `data/raw/`
  - **`PROC`** para `data/processed/`
- No usar variantes como `RAWDATA`, `PROCESSED` o nombres alternos para esas dos capas.
- Se mantienen archivos al mismo nivel en `train/` y `oot/` (no carpetas por segmento).
- **`casuser` es exclusivo para tablas de configuración** (`cfg_troncales`, `cfg_segmentos`). Todo dato operativo usa CASLIBs PATH-based (ver `docs/caslib_lifecycle.md`).
- Cada paso que crea un CASLIB o promueve tablas es responsable de su cleanup.
- **Parámetros específicos de módulos** (`threshold`, `num_rounds`, `num_bins`, etc.) **no** se declaran en `config.sas`. Se configuran en los steps de métodos o en la invocación del módulo. `config.sas` solo contiene parámetros estructurales de troncales/segmentos (identificadores, variables, rangos, listas, segmentación).
- **Step 02 crea las carpetas de output del run** (`outputs/runs/<run_id>/...`) en cada corrida, independientemente de `data_prep_enabled`. Step 03 solo crea dirs de data.
- **Step 03 crea automáticamente** las subcarpetas `data/processed/troncal_X/train/` y `data/processed/troncal_X/oot/` iterando `casuser.cfg_troncales`, garantizando que la estructura de directorios exista antes de la partición (Step 05).
- **Steps 03–05 (data prep) se ejecutan una sola vez** por proyecto (o cuando se quiera regenerar data). El flag `data_prep_enabled` en `runner/main.sas` controla este comportamiento.
- **Ciclo de vida estricto de CASLIBs**: todo CASLIB creado se dropea al final del mismo step (`create → promote → work → drop`). Ningún CASLIB sobrevive entre steps. Las tablas promovidas se eliminan al dropear el CASLIB.
- **Independencia de steps**: cada step es autónomo. Si usa macros del framework, incluye `common_public.sas` al inicio de su archivo. Si usa CASLIBs operativos, los crea y dropea dentro de sí mismo. `casuser` (config) es la única excepción — persiste en la sesión CAS.
- **Restricción SAS open code**: `%if`/`%do` no se permiten fuera de una macro. Todo archivo `.sas` que use lógica condicional la encapsula en `%macro _stepNN_xxx; ... %mend; %_stepNN_xxx;`. Esto aplica a `runner/main.sas` (`%macro _main_pipeline`), a steps individuales (`_step02_load`, `_step04_import`, `_step05_partition`, `_step06_validate`, `_step09_validate`) y a cualquier futuro `.sas` que necesite `%if`/`%do`.
- **`run_module.sas` promueve el input** desde CASLIB `PROC` como tabla `_active_input`, ejecuta el módulo, y dropea la tabla promovida al finalizar. Los módulos reciben `input_table=_active_input` en vez de un path.
- **Modo AUTO / CUSTOM por módulo**: los módulos que soportan personalización de variables exponen `<module>_mode` (AUTO | CUSTOM) y `<module>_custom_vars` en los steps de métodos (07/10). En modo AUTO, el módulo resuelve variables desde config; en modo CUSTOM, usa las variables definidas por el usuario.
- **Carpeta `experiments/`**: los outputs generados en modo CUSTOM se rutan a `outputs/runs/<run_id>/experiments/` en lugar de `reports/` + `tables/`. Esto separa resultados oficiales de validación de análisis exploratorios ad-hoc. El prefijo `custom_` se añade a los archivos para identificarlos.
