# Framework SAS Viya (CAS) para Validación y Controles Automáticos

Este repositorio contiene un framework modular en **SAS Viya / CAS** para preparar data (train/oot), ejecutar controles de validación (por ejemplo **Gini, PSI**) y generar artefactos (reportes, tablas, logs) de forma **estandarizada y automatizable**.

El diseño prioriza:
- Convenciones determinísticas de rutas y nombres para facilitar loops y paralelización.
- Separación clara entre **data preparada** (`data/processed`) y **artefactos de ejecución** (`outputs/runs`).
- Módulos con API pública estable (`*_run.sas`) y validaciones explícitas (`*_contract.sas`).
- Orquestación centralizada en un **runner** SAS (reemplazo de `.flw/.step`).

---

## 1) Qué hace el proyecto

### 1.1 Flujo funcional (alto nivel)

El usuario configura el framework editando los **steps** (`steps/*.sas`) y luego ejecuta `runner/main.sas`:

1. **Setup del proyecto**: definir ruta raíz.
2. **Carga de configuración + creación de dirs del run**: leer `config.sas`, generar `run_id`, crear `outputs/runs/<run_id>/...`.
3. **Creación de carpetas de data**: `data/raw/`, `data/processed/`, subcarpetas `troncal_X/train/oot/`.
4. **Importación de datos desde ADLS** (opcional, una vez por proyecto): generar raw `.sashdat`.
5. **Partición de data**: por troncal + split (`train/oot`) + scope (`universo/segmento`).

> **Steps 03–05 se ejecutan una sola vez por proyecto** (o cuando se quiera regenerar data). En corridas posteriores, setear `data_prep_enabled=0` en `runner/main.sas` para saltar de Step 02 directo a Step 06.

6. **Promoción de contexto segmento**: elegir `troncal_id`, `split`, `seg_id` y promover ese input a ejecución.
7. **Configuración de métodos para segmento**: cada “Método” (tabs/hojas) define su lista de módulos.
8. **Ejecución subflow de análisis para segmento**: correr controles según selección.
9. **Promoción de contexto universo (troncal/base)**: elegir `troncal_id`, `split=base`.
10. **Configuración de métodos para universo**.
11. **Ejecución subflow de análisis para universo**.

Reglas clave:
- La selección de contexto (qué data correr) ocurre **antes** de seleccionar módulos.
- Los métodos (`Metodo 1..N`) son **independientes** entre sí.
- Por defecto, la selección de módulos corre sobre **segmento**; universo se ejecuta en su bloque propio.

---

## 2) Estructura del repositorio

Estructura recomendada (resumen):

```
project_root/
  data/
    raw/
      mydataset.sashdat
    processed/
      troncal_1/
        train/
          base.sashdat
          seg001.sashdat
          seg002.sashdat
        oot/
          base.sashdat
          seg001.sashdat
          seg002.sashdat
      troncal_2/
        train/
          base.sashdat
          seg001.sashdat
        oot/
          base.sashdat
          seg001.sashdat

  src/
    common/
      common_public.sas
      fw_paths.sas
      cas_utils.sas
      preparation/
        fw_import_adls_to_cas.sas
        fw_prepare_processed.sas
    dispatch/
      run_method.sas
      run_module.sas
    modules/
      correlacion/
        correlacion_run.sas
        correlacion_contract.sas
        impl/
          correlacion_compute.sas
          correlacion_report.sas
      gini/...
      psi/...

  runner/
    main.sas                        # entrypoint — incluye steps y orquesta pipeline

  steps/                             # FRONTEND — configuración previa a ejecutar controles
    01_setup_project.sas             # rutas del proyecto
    02_load_config.sas               # carga/validación de config.sas
    03_create_folders.sas            # creación de carpetas base + troncal_X/train/oot
    04_import_raw_data.sas           # importación ADLS (una vez por proyecto)
    05_partition_data.sas            # particiones troncal/train/oot + universo/segmento
    06_promote_segment_context.sas   # seleccionar troncal/split/seg_id a promover
    07_config_methods_segment.sas    # tabs Metodo 1..N para segmento
    08_run_methods_segment.sas       # ejecución subflow segmento
    09_promote_universe_context.sas  # seleccionar troncal/split base a promover
    10_config_methods_universe.sas   # tabs Metodo 1..N para universo
    11_run_methods_universe.sas      # ejecución subflow universo

  outputs/
    runs/
      <run_id>/
        logs/
        reports/
        images/
        tables/
        manifests/
        experiments/           # outputs de análisis exploratorio (modo CUSTOM)
```

Notas:
- `config.sas` define troncales/segmentos (DATA steps CAS). `casuser.cfg_troncales` y `casuser.cfg_segmentos` son las únicas tablas persistentes en `casuser`.
- `steps/*.sas` modelan el frontend del flujo: primero contexto de datos, luego selección de módulos por método.
- El subflow de módulos se puede adjuntar al flujo principal y se ejecuta con el contexto promovido.
- Todo dato operativo (raw, processed, outputs) usa CASLIBs PATH-based (ver `docs/caslib_lifecycle.md`).
- Step 02 crea las carpetas de output del run (`outputs/runs/<run_id>/...` incluyendo `experiments/`) en cada corrida, independientemente de `data_prep_enabled`.
- Step 03 crea `data/raw/`, `data/processed/`, y subcarpetas `troncal_X/train/` y `troncal_X/oot/` por cada troncal. Solo se ejecuta durante data prep.
- Parámetros específicos de módulos de análisis (`threshold`, `num_rounds`, `num_bins`, etc.) **no** viven en `config.sas`; se configuran en los steps de métodos o dentro del módulo correspondiente.

### 3.0a Ciclo de vida de CASLIBs

Todo bloque que usa CASLIBs sigue estrictamente: **create → promote → work → drop**.
- Cada fase (data prep, ejecución segmento, ejecución universo) crea sus CASLIBs al inicio y los dropea al final.
- `run_module.sas` promueve el input específico (`_active_input`) desde CASLIB `PROC`, ejecuta el módulo, y dropea la tabla promovida.
- Ningún CASLIB sobrevive entre fases; los `.sashdat` en disco persisten.

---

## 3) Convenciones y estándares (para automatización)

### 3.0 CASLIBs y casuser
- **`casuser`** se usa **únicamente** para las tablas de configuración (`cfg_troncales`, `cfg_segmentos`) generadas por `config.sas`.
- Todo dato operativo (raw, processed, outputs) se accede mediante **PATH-based CASLIBs** (GLOBAL) mapeados a carpetas del filesystem, siguiendo `docs/caslib_lifecycle.md`.
- CASLIBs estándar del framework:
  - `RAW` → `data/raw/` (subdirs=0)
  - `PROC` → `data/processed/` (subdirs=1, para acceder subcarpetas troncal/split)
  - `OUT` → `outputs/runs/<run_id>/` (subdirs=1, creado por el runner)
- Los módulos pueden crear CASLIBs scoped adicionales (ej. `MOD_GINI_<run_id>`) y son responsables de su cleanup.

**Restricción SAS open code:** `%if`/`%do` no se permiten fuera de una macro. Todo archivo `.sas` que use lógica condicional debe encapsularla en `%macro _stepNN_xxx; ... %mend; %_stepNN_xxx;`. Esto aplica a `runner/main.sas` (`%macro _main_pipeline`) y a steps individuales como `02`, `04`, `05`, `06`, `09`.

**Independencia de steps:** cada step carga sus propias dependencias (`%include "&fw_root./src/common/common_public.sas";`) y gestiona su propio ciclo de vida de CASLIBs (create → promote → work → drop). Ningún CASLIB operativo sobrevive entre steps. `casuser` (config) es la única excepción.
- Se usa CASLIB/LIBNAME fijo `OUT` para outputs porque `LIBNAME` en SAS admite máximo 8 caracteres.
- La separación por corrida se mantiene vía path físico `outputs/runs/<run_id>/`.
- Convención estricta de naming operativo: usar solo `RAW` y `PROC` para capas de datos del framework (no usar `RAWDATA` ni `PROCESSED`).

### 3.1 Data preparada: naming determinístico
Dentro de cada `troncal_X/{train|oot}`:
- Universo (troncal completo): `base.sashdat`
- Segmentos numéricos: `seg001.sashdat`, `seg002.sashdat`, ..., `segNNN.sashdat`

Reglas:
- Padding de 3 dígitos para segmentos: `seg%sysfunc(putn(seg_id,z3.))`.
- El split (`train/oot`) y la troncal se expresan por carpeta, no por el nombre de archivo.
- Se accede vía CASLIB `PROC` con la subruta relativa (ej. `troncal_1/train/base.sashdat`).

### 3.2 Artefactos de ejecución por run
Todo output va en:
- `outputs/runs/<run_id>/logs`
- `outputs/runs/<run_id>/reports`
- `outputs/runs/<run_id>/images`
- `outputs/runs/<run_id>/tables`
- `outputs/runs/<run_id>/manifests`
- `outputs/runs/<run_id>/experiments` — outputs exploratorios (modo CUSTOM de módulos)

Reglas:
- Ningún módulo escribe en `data/processed` (solo en `outputs/...`).
- Los outputs se persisten vía CASLIB `OUT` o un CASLIB scoped del módulo.

### 3.3 API pública de módulos
Cada módulo implementa:
- `src/modules/<modulo>/<modulo>_run.sas`  (macro pública `%<modulo>_run(...)`)
- `src/modules/<modulo>/<modulo>_contract.sas` (validaciones)
- `src/modules/<modulo>/impl/*` (cómputo, reportes, plots, utilidades internas)

### 3.4 Validaciones obligatorias
Antes de ejecutar un módulo:
- existencia del input (tabla/archivo)
- columnas requeridas para el módulo
- tipos/formatos mínimos (cuando aplique)

---

## 4) Orden de ejecución (segmentos primero)

Si una troncal tiene segmentación activa (variable segmentadora y N segmentos):
1. Ejecutar módulos en **cada segmento** (train y oot).
2. Ejecutar módulos en el **universo de la troncal** (train y oot).

Motivo:
- Permite detectar problemas segmentados temprano (calidad de data, drift, métricas) antes del agregado troncal.
- Favorece paralelización natural por segmento.

---

## 5) Steps como frontend del framework

Los archivos `steps/*.sas` actúan como el **frontend** del framework. El usuario edita estos archivos para configurar su run, y luego ejecuta `runner/main.sas`.

### 5.1 Flujo de steps

| Step | Archivo | Configura |
|------|---------|-----------|
| 01 | `steps/01_setup_project.sas` | Rutas del proyecto |
| 02 | `steps/02_load_config.sas` | Carga `config.sas` + dirs de output del run |
| 03 | `steps/03_create_folders.sas` | Carpetas de data + `troncal_X/train/oot/` (solo data prep) |
| 04 | `steps/04_import_raw_data.sas` | Importación ADLS (una vez por proyecto) |
| 05 | `steps/05_partition_data.sas` | Particiones por troncal/split/scope |
| 06 | `steps/06_promote_segment_context.sas` | Contexto de ejecución para segmento |
| 07 | `steps/07_config_methods_segment.sas` | Selección de módulos por Método (segmento) + params módulos |
| 08 | `steps/08_run_methods_segment.sas` | Ejecutar subflow de módulos (segmento) |
| 09 | `steps/09_promote_universe_context.sas` | Contexto de ejecución para universo |
| 10 | `steps/10_config_methods_universe.sas` | Selección de módulos por Método (universo) + params módulos |
| 11 | `steps/11_run_methods_universe.sas` | Ejecutar subflow de módulos (universo) |

### 5.2 Cómo usar
1. Configurar rutas/config (Steps 01–02). Siempre se ejecutan.
2. **Primera corrida**: `data_prep_enabled=1` → ejecutar Steps 03–05 (carpetas, ADLS, partición).
   **Corridas posteriores**: `data_prep_enabled=0` → saltar Steps 03–05.
3. Elegir y promover contexto de segmento (Step 06).
4. Definir Métodos (`Metodo 1..N`) y módulos para segmento (Step 07), ejecutar (Step 08).
5. Elegir y promover contexto de universo (Step 09).
6. Definir Métodos y módulos para universo (Step 10), ejecutar (Step 11).

### 5.3 Convención de IDs `_id_*`
Cada step documenta variables `_id_*` que representan campos de un formulario de UI:
- Contexto de segmento: `_id_ctx_troncal_id`, `_id_ctx_split`, `_id_ctx_seg_id`
- Contexto de universo: `_id_ctx_troncal_id`, `_id_ctx_split=base`
- Métodos: `_id_metodo_1_modules`, `_id_metodo_1_enabled`, ..., `_id_metodo_n_modules`

Ver `design.md §5` para el contrato completo.

---

## 6) Cómo agregar un módulo nuevo

1. Crear carpeta `src/modules/<nuevo_modulo>/`.
2. Implementar:
   - `<nuevo_modulo>_run.sas`
   - `<nuevo_modulo>_contract.sas`
   - `impl/<nuevo_modulo>_compute.sas`
   - `impl/<nuevo_modulo>_report.sas`
3. Registrar el módulo en `configs/registry/modules_registry.sas`.
4. Documentar inputs/outputs en `docs/module_catalog.md`.

Ver `src/modules/correlacion/` como implementación de referencia.

---

## 7) Documentación adicional

- Diseño y contratos: `docs/design.md`
- Catálogo de módulos: `docs/module_catalog.md`
