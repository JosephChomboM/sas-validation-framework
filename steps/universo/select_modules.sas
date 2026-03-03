/* =========================================================================
   steps/universo/select_modules.sas — Selección de módulos (UNIVERSO)
   =========================================================================
   Habilitar (1) o deshabilitar (0) cada módulo para el swimlane de
   universo/troncal. Los módulos deshabilitados se saltan automáticamente.

   UI: checkbox por módulo (0 = no seleccionado, 1 = seleccionado).
   Se usa checkbox en vez de list_control porque:
     - Siempre resuelve a 0|1, sin WARNING de macro vars inexistentes.
     - Mapeo directo a flag run_<modulo>.

   Variables UI (.flw checkbox):
     _id_run_estabilidad : 0|1
     _id_run_fillrate    : 0|1
     _id_run_missings    : 0|1
     _id_run_psi         : 0|1
     _id_run_bivariado   : 0|1
     _id_run_correlacion : 0|1
     _id_run_gini        : 0|1

   Agrupación por sub-método (para organización de outputs):
     Método 4.2: estabilidad, fillrate, missings, psi
     Método 4.3: bivariado, correlación, gini
   ========================================================================= */

/* ========= Método 4.2 — Estabilidad / Distribución ==================== */
%let run_estabilidad = 1;
%let run_fillrate    = 1;
%let run_missings    = 1;
%let run_psi         = 1;

/* ========= Método 4.3 — Asociación / Discriminación =================== */
%let run_bivariado   = 1;
%let run_correlacion = 1;
%let run_gini        = 1;

%put NOTE: [universo/select_modules] Módulos habilitados:;
%put NOTE:   4.2 → estabilidad=&run_estabilidad. fillrate=&run_fillrate. missings=&run_missings. psi=&run_psi.;
%put NOTE:   4.3 → bivariado=&run_bivariado. correlacion=&run_correlacion. gini=&run_gini.;
