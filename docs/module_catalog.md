# Catalogo de modulos

Este archivo describe solo contratos funcionales de modulos.

No repetir aqui:
- arquitectura general
- reglas de CASLIB lifecycle
- snippets PROC CAS

Ver:
- `docs/design.md`
- `docs/caslib_lifecycle.md`
- `docs/cas_first_patterns.md`

---

## 1) Convenciones comunes

### 1.1 Contexto minimo

Todo modulo debe aceptar o derivar:
- `troncal_id`
- `scope` = `segmento` o `universo`
- `split` cuando aplique
- `seg_id` si `scope=segmento`
- `run_id`

### 1.2 Inputs

- los inputs se leen desde CASLIB `PROC`
- `casuser` se usa para temporales e intermedias
- `run_module.sas` promueve el input segun contexto

### 1.3 Modos de input

| Modo | Uso | Tablas promovidas |
| --- | --- | --- |
| `dual_input=0` | una sola tabla | `_active_input` |
| `dual_input=1` | comparacion train vs oot | `_train_input`, `_oot_input` |

### 1.4 Cortes temporales

| Regla | Uso |
| --- | --- |
| `def_cld` | modulos con target, PD o XB |
| `oot_max_mes` | modulos que solo analizan variables |

### 1.5 Destino de outputs

| Modo | Destino |
| --- | --- |
| `AUTO` | `reports/`, `images/`, `tables/`, `models/` |
| `CUSTOM` | `experiments/` |

### 1.6 Resumen de implementacion

| Modulo | Metodo | Estado | Input |
| --- | --- | --- | --- |
| `universe` | M1.1 | implementado | dual |
| `target` | M2.1 | implementado | dual |
| `fillrate` | M4.2 | implementado | dual |
| `psi` | M4.2 | implementado | dual |
| `correlacion` | M4.3 | implementado | single |
| `gini` | M4.3 | implementado | dual |
| `bootstrap` | M4.3 | implementado | dual |
| `calibracion` | M8 | implementado | dual |
| `challenge` | M9 | implementado | dual + registries |
| `gradient_boosting` | M9 | worker de challenge | dual |
| `decision_tree` | M9 | worker de challenge | dual |
| `random_forest` | M9 | worker de challenge | dual |
| `segmentacion` | M3 | scaffold |
| `estabilidad` | M4.2 | scaffold |
| `missings` | M4.2 | scaffold |
| `similitud` | M4.2 | scaffold |
| `bivariado` | M4.3 | scaffold |
| `replica` | M5 | scaffold |
| `precision` | M6 | scaffold |
| `monotonicidad` | M7 | scaffold |
| `svm` | M9 | placeholder |
| `neural_network` | M9 | placeholder |

---

## 2) Modulos implementados

### 2.1 Universe

| Campo | Valor |
| --- | --- |
| Metodo | M1.1 |
| Objetivo | comparar composicion TRAIN vs OOT |
| Input | `dual_input=1` |
| Scope | segmento y universo |
| Corte | `oot_max_mes` |
| Variables clave | `byvar`, `id_var`, `monto` opcional |
| Persistencia | no persiste `.sas7bdat` |
| Salidas | HTML, XLSX, JPEG |

Notas:
- calcula evolutivo de cuentas, duplicados y metricas de monto si existe `monto`
- usa `run_module` con `_train_input` y `_oot_input`

### 2.2 Target

| Campo | Valor |
| --- | --- |
| Metodo | M2.1 |
| Objetivo | comparar comportamiento del target en TRAIN vs OOT |
| Input | `dual_input=1` |
| Scope | segmento y universo |
| Corte | `def_cld` |
| Variables clave | `byvar`, `target`, `monto` opcional, `def_cld` |
| Persistencia | no persiste `.sas7bdat` |
| Salidas | HTML, XLSX, JPEG |

Notas:
- genera metricas de RD, bandas y variantes ponderadas si existe `monto`

### 2.3 Fillrate

| Campo | Valor |
| --- | --- |
| Metodo | M4.2 |
| Objetivo | fillrate general y mensual; Gini de variables numericas |
| Input | `dual_input=1` |
| Scope | segmento y universo |
| Corte | `def_cld` |
| Modos | `AUTO`, `CUSTOM` |
| Variables clave | listas num/cat, `byvar`, `target`, `def_cld` |
| Persistencia | `fill_tX_<scope>_gnrl`, `fill_tX_<scope>_mnth` |
| Salidas | HTML, XLSX, JPEG, tablas SAS |

### 2.4 PSI

| Campo | Valor |
| --- | --- |
| Metodo | M4.2 |
| Objetivo | comparar distribucion TRAIN vs OOT |
| Input | `dual_input=1` |
| Scope | segmento y universo |
| Corte | `oot_max_mes` |
| Modos | `AUTO`, `CUSTOM` |
| Variables clave | listas num/cat, `byvar` |
| Persistencia | `psi_tX_<scope>_cubo`, `psi_tX_<scope>_wide`, `psi_tX_<scope>_rsmn` |
| Salidas | HTML, XLSX, JPEG, tablas SAS |

Notas:
- es el modulo canonico para cubo, pivot y resumen de drift

### 2.5 Correlacion

| Campo | Valor |
| --- | --- |
| Metodo | M4.3 |
| Objetivo | matrices de Pearson y Spearman |
| Input | `dual_input=0` |
| Scope | segmento y universo |
| Corte | `oot_max_mes` |
| Modos | `AUTO`, `CUSTOM` |
| Variables clave | lista numerica |
| Persistencia | `corr_tX_<spl>_<scope>_prsn`, `corr_tX_<spl>_<scope>_sprm` |
| Salidas | HTML, XLSX, tablas SAS |

Notas:
- el modulo opera sobre una sola tabla por corrida de `run_module`

### 2.6 Gini

| Campo | Valor |
| --- | --- |
| Metodo | M4.3 |
| Objetivo | Gini de modelo y variables |
| Input | `dual_input=1` |
| Scope | segmento y universo |
| Corte | `def_cld` |
| Modos | `AUTO`, `CUSTOM` |
| Variables clave | `target`, `score`, `byvar`, `def_cld` |
| Persistencia | `mdlg`, `mdlm`, `varg`, `vcmp`, `vsum`, `vdet` |
| Salidas | HTML, XLSX, JPEG, tablas SAS |

Notas:
- el score puede venir de `pd`, `xb` o variable custom
- soporta calculo con y sin missings

### 2.7 Bootstrap

| Campo | Valor |
| --- | --- |
| Metodo | M4.3 |
| Objetivo | estabilidad de coeficientes y pesos por variable |
| Input | `dual_input=1` |
| Scope | segmento y universo |
| Corte | `def_cld` |
| Modos | `AUTO`, `CUSTOM` |
| Variables clave | `target`, `byvar`, `def_cld`, lista numerica |
| Persistencia | `boot_tX_<scope>_rpt`, `boot_tX_<scope>_cubo` |
| Salidas | HTML, XLSX, JPEG, tablas SAS |

Notas:
- usa `PROC LOGISTIC`
- si la variante ponderada no aplica, corre el flujo estandar

### 2.8 Calibracion

| Campo | Valor |
| --- | --- |
| Metodo | M8 |
| Objetivo | comparar target vs score por driver |
| Input | `dual_input=1` |
| Scope | segmento y universo |
| Corte | `def_cld` |
| Modos | `AUTO`, `CUSTOM` |
| Variables clave | drivers num/cat, `target`, `score`, `monto`, `byvar`, `def_cld` |
| Persistencia | `calb_tX_<scope>_detl`, `calb_tX_<scope>_cuts` |
| Salidas | HTML, XLSX, JPEG, tablas SAS |

Notas:
- si `monto` no existe, la parte ponderada se omite con warning
- usa bucketizacion train y reaplica cortes en oot

### 2.9 Challenge

| Campo | Valor |
| --- | --- |
| Metodo | M9 |
| Objetivo | consolidar registries y elegir champion final |
| Input | `dual_input=1` + registries de algoritmos |
| Scope | segmento y universo |
| Corte | depende del algoritmo worker |
| Modos | `AUTO`, `CUSTOM` |
| Variables clave | registries `gb`, `dt`, `rf` y benchmark |
| Persistencia | artifacts `gb_*`, `dt_*`, `rf_*` y `chall_*` |
| Salidas | HTML, XLSX, JPEG mensual, modelos ASTORE |

Notas:
- `gradient_boosting`, `decision_tree` y `random_forest` generan champion local por scope
- `challenge` consolida y elige champion final por `Gini_Penalizado`
- `svm` y `neural_network` existen solo como placeholders

---

## 3) Modulos referenciados pero no detallados aqui

| Modulo | Metodo | Nota |
| --- | --- | --- |
| `segmentacion` | M3 | estructura de step y modulo existente, sin contrato funcional cerrado |
| `estabilidad` | M4.2 | scaffold |
| `missings` | M4.2 | scaffold |
| `similitud` | M4.2 | scaffold |
| `bivariado` | M4.3 | scaffold |
| `replica` | M5 | scaffold |
| `precision` | M6 | scaffold |
| `monotonicidad` | M7 | scaffold |

---

## 4) Reglas para agregar un modulo

Minimo requerido:
- `src/modules/<modulo>/<modulo>_run.sas`
- `src/modules/<modulo>/<modulo>_contract.sas`
- `src/modules/<modulo>/impl/<modulo>_compute.sas`
- `src/modules/<modulo>/impl/<modulo>_report.sas` si aplica
- `steps/methods/metod_N/step_<modulo>.sas`

Contrato esperado:
- validar input y columnas al inicio
- declarar si usa `dual_input=0` o `dual_input=1`
- declarar corte temporal (`def_cld` u `oot_max_mes`)
- limpiar temporales en `casuser`
- persistir solo tablas realmente utiles
- usar `AUTO` para flujo estandar y `CUSTOM` para exploracion
