/* =========================================================================
   steps/segmento/select_modules.sas — Selección de módulos (SEGMENTO)
   =========================================================================
   Habilitar (1) o deshabilitar (0) cada módulo para el swimlane de
   segmento. Los módulos deshabilitados se saltan automáticamente.

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

%put NOTE: [segmento/select_modules] Módulos habilitados:;
%put NOTE:   4.2 → estabilidad=&run_estabilidad. fillrate=&run_fillrate. missings=&run_missings. psi=&run_psi.;
%put NOTE:   4.3 → bivariado=&run_bivariado. correlacion=&run_correlacion. gini=&run_gini.;
