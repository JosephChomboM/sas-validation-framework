/* =========================================================================
steps/07_select_modules.sas - Selección de módulos
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
Método 5.2.1: replica
Método 6: precision
Método 7: monotonicidad
Método 8: calibracion
Método 9: challenge
========================================================================= */
/* ========= Método 1.1 - Describe Universo ============================== */
%let run_universe=&_id_run_universe.;

/* ========= Método 4.2 - Estabilidad / Distribución ==================== */
/* %let run_estabilidad = 1; */
%let run_fillrate=%sysfunc(coalescec(%superq(_id_run_fillrate), 0));
/* %let run_missings    = 1; */
%let run_psi=&_id_run_psi.;
%let run_monotonicidad=%sysfunc(coalescec(%superq(_id_run_monotonicidad), 0));

/* ========= Método 4.3 - Asociación / Discriminación =================== */
*%let run_bivariado   = 1;
%let run_correlacion=&_id_run_corr;
%let run_gini=%sysfunc(coalescec(%superq(_id_run_gini), 0));

/* ========= Método 5.2.1 - Replica ===================================== */
%let run_replica=%sysfunc(coalescec(%superq(_id_run_replica), 0));

/* ========= Método 6 - Precision ======================================= */
%let run_precision=%sysfunc(coalescec(%superq(_id_run_precision), 0));

/* ========= Método 8 - Calibracion ===================================== */
%let run_calibracion=%sysfunc(coalescec(%superq(_id_run_calibracion), 0));

/* ========= Método 9 - Challenge ======================================= */
%let run_challenge=%sysfunc(coalescec(%superq(_id_run_challenge), 0));
%let run_gradient_boosting=%sysfunc(coalescec(%superq(_id_run_gradient_boosting), 0));
%let run_random_forest=%sysfunc(coalescec(%superq(_id_run_random_forest), 0));

%put NOTE: [select_modules] Módulos habilitados:;
%put NOTE: 1.1 → universe=&run_universe.;
%put NOTE: 4.2 → estabilidad=&run_estabilidad. fillrate=&run_fillrate.
  missings=&run_missings. psi=&run_psi.;
%put NOTE: 4.3 → bivariado=&run_bivariado. correlacion=&run_correlacion.
  gini=&run_gini.;
%put NOTE: 5.2.1 → replica=&run_replica.;
%put NOTE: 6.0 → precision=&run_precision.;
%put NOTE: 7.0 → monotonicidad=&run_monotonicidad.;
%put NOTE: 8.0 → calibracion=&run_calibracion.;
%put NOTE: 9.0 → challenge=&run_challenge.
    gradient_boosting=&run_gradient_boosting.
    random_forest=&run_random_forest.;
