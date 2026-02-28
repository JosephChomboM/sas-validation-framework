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
2. **Carga de configuración**: leer `config.sas` (troncales/segmentos).
3. **Creación de carpetas**: estructura base de data y outputs.
4. **Importación de datos desde ADLS** (opcional, una vez por proyecto): generar raw `.sashdat`.
5. **Partición de data**: por troncal + split (`train/oot`) + scope (`universo/segmento`).
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
      gini/...
      psi/...

  runner/
    main.sas                        # entrypoint — incluye steps y orquesta pipeline

  steps/                             # FRONTEND — configuración previa a ejecutar controles
    01_setup_project.sas             # rutas del proyecto
    02_load_config.sas               # carga/validación de config.sas
    03_create_folders.sas            # creación de carpetas base
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
```

Notas:
- `config.sas` define troncales/segmentos (DATA steps CAS). `casuser.cfg_troncales` y `casuser.cfg_segmentos` son las únicas tablas persistentes en `casuser`.
- `steps/*.sas` modelan el frontend del flujo: primero contexto de datos, luego selección de módulos por método.
- El subflow de módulos se puede adjuntar al flujo principal y se ejecuta con el contexto promovido.
- Todo dato operativo (raw, processed, outputs) usa CASLIBs PATH-based (ver `docs/caslib_lifecycle.md`).

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

Nota técnica:
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
| 02 | `steps/02_load_config.sas` | Carga y validación de `config.sas` |
| 03 | `steps/03_create_folders.sas` | Creación de carpetas base |
| 04 | `steps/04_import_raw_data.sas` | Importación ADLS (una vez por proyecto) |
| 05 | `steps/05_partition_data.sas` | Particiones por troncal/split/scope |
| 06 | `steps/06_promote_segment_context.sas` | Contexto de ejecución para segmento |
| 07 | `steps/07_config_methods_segment.sas` | Selección de módulos por Método (segmento) |
| 08 | `steps/08_run_methods_segment.sas` | Ejecutar subflow de módulos (segmento) |
| 09 | `steps/09_promote_universe_context.sas` | Contexto de ejecución para universo |
| 10 | `steps/10_config_methods_universe.sas` | Selección de módulos por Método (universo) |
| 11 | `steps/11_run_methods_universe.sas` | Ejecutar subflow de módulos (universo) |

### 5.2 Cómo usar
1. Configurar rutas/config/data prep (Steps 01–05).
2. Elegir y promover contexto de segmento (Step 06).
3. Definir Métodos (`Metodo 1..N`) y módulos para segmento (Step 07), ejecutar (Step 08).
4. Elegir y promover contexto de universo (Step 09).
5. Definir Métodos y módulos para universo (Step 10), ejecutar (Step 11).

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

---

## 7) Documentación adicional

- Diseño y contratos: `docs/design.md`
- Catálogo de módulos: `docs/module_catalog.md`
