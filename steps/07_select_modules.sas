/* =========================================================================
steps/segmento/select_modules.sas - Selección de módulos
=========================================================================
Habilitar (1) o deshabilitar (0) cada módulo.
Los módulos deshabilitados se saltan automáticamente.

UI: checkbox por módulo (0 = no seleccionado, 1 = seleccionado).
Se usa checkbox en vez de list_control porque:
- Siempre resuelve a 0|1, sin WARNING de macro vars inexistentes.
- Mapeo directo a flag run_<modulo>.

Agrupación por sub-método (para organización de outputs):
Método 1.1: universe (describe universo)
Método 4.2: estabilidad, fillrate, missings, psi
Método 4.3: bivariado, correlación, gini
========================================================================= */
/* ========= Método 1.1 - Describe Universo ============================== */
%let run_universe=&_id_run_universe.;

/* ========= Método 4.2 - Estabilidad / Distribución ==================== */
/* %let run_estabilidad = 1; */
/* %let run_fillrate    = 1; */
/* %let run_missings    = 1; */
%let run_psi=&_id_run_psi.;
%let run_monotonicidad=%sysfunc(coalescec(%superq(_id_run_monotonicidad), 0));

/* ========= Método 4.3 - Asociación / Discriminación =================== */
*%let run_bivariado   = 1;
%let run_correlacion=&_id_run_corr;
*%let run_gini        = 1;
%put NOTE: [select_modules] Módulos habilitados:;
%put NOTE: 1.1 → universe=&run_universe.;
%put NOTE: 4.2 → estabilidad=&run_estabilidad. fillrate=&run_fillrate.
  missings=&run_missings. psi=&run_psi.;
%put NOTE: 4.3 → bivariado=&run_bivariado. correlacion=&run_correlacion.
  gini=&run_gini.;
%put NOTE: 7.0 → monotonicidad=&run_monotonicidad.;
