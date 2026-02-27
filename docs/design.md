# Diseño del Framework (SAS Viya / CAS)

## 1) Alcance

Este documento describe:
- Componentes del framework y responsabilidades.
- Contratos de rutas y naming.
- Contrato de configuración vía `configs/config.sas`.
- “Pseudo-steps” SAS (simulación de `.step` por comentarios).
- Orden de ejecución: **segmentos primero**, luego troncal.

---

## 2) Arquitectura lógica

### 2.1 Capas

1) **Configuración**
- Fuente: `configs/config.sas` (generado desde HTML).
- Contiene parámetros por troncal y, opcionalmente, por segmento.

2) **Common**
- Utilidades reutilizables:
  - paths
  - logging
  - validaciones genéricas
  - utilidades CAS (existence, nobs, load/save)
  - preparación de data raw → processed

3) **Dispatch**
- Orquestación de ejecución:
  - `run_method.sas`: ejecuta un método (conjunto de módulos)
  - `run_module.sas`: ejecuta un módulo en un contexto dado (troncal/split/segmento)

4) **Modules**
- Implementación por control:
  - API pública (`*_run.sas`)
  - Validaciones (`*_contract.sas`)
  - Implementación interna (`impl/`)

5) **Runner**
- `runner/main.sas`: entrypoint único, reemplaza `.flw`.
- Ejecuta: CAS init → config → (opcional) ADLS import → prepare_processed → run_methods → cleanup.
- La importación ADLS es condicional: se activa con `adls_import_enabled=1` en `config.sas`.

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

## 5) Pseudo-steps SAS (reemplazo de .step)

### 5.1 Motivación
`.step` ofrece un formulario gráfico que produce variables disponibles en la sección “program”. Como no se utilizará `.step`, se crean scripts `.sas` que documentan el contrato de UI mediante comentarios.

Estos scripts no reemplazan al HTML (que genera el `config.sas`). Son un estándar interno para:
- mantener consistencia de naming de IDs de UI
- documentar entradas esperadas por un formulario equivalente
- facilitar mantenimiento y generación futura de UI

### 5.2 Convención de IDs
Los IDs de UI se nombran como:
- `_id_<entidad>_<campo>`

Ejemplos:
- `_id_troncal_select`
- `_id_segment_var`
- `_id_n_segments`
- `_id_methods_select`

Los archivos sugeridos:
- `steps/select_troncal_segment.sas`
- `steps/select_methods.sas`

Contenido:
- Comentario de encabezado con todos los `_id_*`.
- (Opcional) mapeo a macrovariables internas del framework.

---

## 6) Orden de ejecución

Regla:
- Si existe segmentación en una troncal:
  1. Ejecutar todos los módulos para cada segmento (train y oot).
  2. Ejecutar los módulos para el troncal (universo: base) en train y oot.

Motivo:
- Permite detectar issues por subpoblación de forma temprana.
- Habilita paralelización natural por segmento.

---

## 7) Patrones de implementación recomendados

### 7.1 Resolver único de paths
Implementar `src/common/fw_paths.sas` con una macro pública que construya rutas de processed, evitando hardcode:
- `%fw_path_processed(outvar=, troncal_id=, split=, seg_id=)`
  - si `seg_id` vacío: devuelve `troncal_X/<split>/base.sashdat`
  - si `seg_id` presente: devuelve `troncal_X/<split>/segNNN.sashdat`
- Estas rutas son **relativas al CASLIB `PROCESSED`** (con subdirs habilitado), no a `casuser`.

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
- Cleanup: dropear CASLIB `LAKEHOUSE` al finalizar.
- Controlado por `adls_import_enabled` en `config.sas`; si vale `0` se salta completamente.

### 7.4 Preparación idempotente
`fw_prepare_processed` debe:
- Crear CASLIB `RAW` (PATH→`data/raw/`) y CASLIB `PROCESSED` (PATH→`data/processed/`, subdirs=1)
- Leer raw desde CASLIB `RAW`, filtrar por ventanas mes, guardar en CASLIB `PROCESSED`
- Sobrescribir outputs processed de manera controlada
- Limpiar tablas temporales CAS (en `casuser`)
- Loggear conteos (nobs) para auditoría mínima
- **No dejar tablas operativas en `casuser`**; solo temporales que se dropean al final

### 7.5 Contratos y validaciones
Cada módulo debe fallar temprano con mensajes claros si:
- faltan columnas
- el input está vacío
- el split/segmento no existe

---

## 8) Decisiones explícitas del proyecto

- No se usa JSON como fuente de configuración; solo `config.sas`.
- No se usan `.flw` ni `.step` como artefactos ejecutables; se reemplaza con `runner/main.sas`.
- Se mantienen archivos al mismo nivel en `train/` y `oot/` (no carpetas por segmento).
- **`casuser` es exclusivo para tablas de configuración** (`cfg_troncales`, `cfg_segmentos`). Todo dato operativo usa CASLIBs PATH-based (ver `docs/caslib_lifecycle.md`).
- Cada paso que crea un CASLIB o promueve tablas es responsable de su cleanup.
