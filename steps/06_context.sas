/* =========================================================================
   steps/context_and_modules.sas — Contexto de ejecución + Selección de módulos
   =========================================================================
   Define el scope de ejecución (UNIVERSO o SEGMENTO), la troncal,
   el split, y los módulos a ejecutar.

   Este archivo unifica la selección de contexto y módulos en un solo step.
   El usuario elige si quiere correr a nivel de troncal (UNIVERSO) o a
   nivel de segmentos (SEGMENTO), y habilita los módulos deseados.

   Variables de contexto:
     ctx_scope       : UNIVERSO | SEGMENTO
     ctx_troncal_id  : número de troncal a analizar
     ctx_split       : TRAIN | OOT | BOTH
     ctx_seg_id      : ALL | N (solo si scope=SEGMENTO, se ignora en UNIVERSO)
     ctx_n_segments  : (auto-resuelto desde config si scope=SEGMENTO)

   Flags de módulos (0=deshabilitado, 1=habilitado):
     run_estabilidad, run_fillrate, run_missings, run_psi,
     run_bivariado, run_correlacion, run_gini

   Variables UI (.flw):
     _id_scope            : UNIVERSO | SEGMENTO (radio button)
     _id_troncal_id       : número de troncal
     _id_split            : TRAIN | OOT | BOTH (radio button)
     _id_seg_id           : ALL | CUSTOM (radio button, solo SEGMENTO)
     _id_seg_num          : número específico (solo si seg_id=CUSTOM)
     _id_run_estabilidad  : 0|1 (checkbox)
     _id_run_fillrate     : 0|1 (checkbox)
     _id_run_missings     : 0|1 (checkbox)
     _id_run_psi          : 0|1 (checkbox)
     _id_run_bivariado    : 0|1 (checkbox)
     _id_run_correlacion  : 0|1 (checkbox)
     _id_run_gini         : 0|1 (checkbox)

   Nota sobre compatibilidad de scope:
     La mayoría de módulos corren tanto para UNIVERSO como SEGMENTO.
     Algunos módulos solo aplican a un scope (ej. segmentacion solo UNIVERSO).
     Cada step de módulo verifica internamente si es compatible con ctx_scope.
   ========================================================================= */

/* ==== CONTEXTO DE EJECUCIÓN ============================================ */

/* ctx_scope:
     UNIVERSO → corre solo la base/universo del troncal (sin segmentos)
     SEGMENTO → corre segmento(s) del troncal                             */
%let ctx_scope = UNIVERSO;

/* Troncal a analizar                                                      */
%let ctx_troncal_id = 1;

/* ctx_split:
     TRAIN → solo train
     OOT   → solo oot
     BOTH  → train y oot                                                   */
%let ctx_split = BOTH;

/* ctx_seg_id (solo aplica si ctx_scope=SEGMENTO):
     ALL → correr TODOS los segmentos del troncal
     <N> → correr solo el segmento N (ej. 1, 2, 3...)
     Se ignora si ctx_scope=UNIVERSO.                                      */
%let ctx_seg_id = ALL;

%put NOTE: [context_and_modules] scope=&ctx_scope. troncal=&ctx_troncal_id. split=&ctx_split. seg=&ctx_seg_id.;

/* ==== SELECCIÓN DE MÓDULOS ============================================= */

/* ========= Método 4.2 — Estabilidad / Distribución ==================== */
%let run_estabilidad = 1;
%let run_fillrate    = 1;
%let run_missings    = 1;
%let run_psi         = 1;

/* ========= Método 4.3 — Asociación / Discriminación =================== */
%let run_bivariado   = 1;
%let run_correlacion = 1;
%let run_gini        = 1;

%put NOTE: [context_and_modules] Módulos habilitados:;
%put NOTE:   4.2 → estabilidad=&run_estabilidad. fillrate=&run_fillrate. missings=&run_missings. psi=&run_psi.;
%put NOTE:   4.3 → bivariado=&run_bivariado. correlacion=&run_correlacion. gini=&run_gini.;

/* ==== VALIDACIÓN ======================================================= */
%macro _ctx_validate;
    %global ctx_n_segments;
    %local _ctx_tr_exists;

    /* ---- 1) scope válido ---------------------------------------------- */
    %if %upcase(&ctx_scope.) ne UNIVERSO and
        %upcase(&ctx_scope.) ne SEGMENTO %then %do;
        %put ERROR: [context_and_modules] ctx_scope=&ctx_scope. no válido. Debe ser UNIVERSO o SEGMENTO.;
        %return;
    %end;

    /* ---- 2) split válido ---------------------------------------------- */
    %if %upcase(&ctx_split.) ne TRAIN and
        %upcase(&ctx_split.) ne OOT and
        %upcase(&ctx_split.) ne BOTH %then %do;
        %put ERROR: [context_and_modules] split=&ctx_split. no válido. Debe ser TRAIN, OOT o BOTH.;
        %return;
    %end;

    /* ---- 3) troncal existe en config ---------------------------------- */
    proc sql noprint;
        select count(*) into :_ctx_tr_exists trimmed
        from casuser.cfg_troncales
        where troncal_id = &ctx_troncal_id.;
    quit;

    %if &_ctx_tr_exists. = 0 %then %do;
        %put ERROR: [context_and_modules] troncal_id=&ctx_troncal_id. no existe en cfg_troncales.;
        %return;
    %end;

    /* ---- 4) Resolver n_segments y validar seg_id (solo SEGMENTO) ------ */
    %if %upcase(&ctx_scope.) = SEGMENTO %then %do;
        proc sql noprint;
            select n_segments into :ctx_n_segments trimmed
            from casuser.cfg_troncales
            where troncal_id = &ctx_troncal_id.;
        quit;
        %put NOTE: [context_and_modules] troncal &ctx_troncal_id. tiene &ctx_n_segments. segmentos.;

        /* Validar seg_id específico si no es ALL */
        %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            %if &ctx_seg_id. < 1 or &ctx_seg_id. > &ctx_n_segments. %then %do;
                %put ERROR: [context_and_modules] seg_id=&ctx_seg_id. fuera de rango (1..&ctx_n_segments.) para troncal=&ctx_troncal_id..;
                %return;
            %end;
        %end;
    %end;
    %else %do;
        /* UNIVERSO: n_segments no aplica */
        %let ctx_n_segments = 0;
    %end;

    %put NOTE: [context_and_modules] Validación OK — scope=&ctx_scope. troncal=&ctx_troncal_id. split=&ctx_split.;
%mend _ctx_validate;
%_ctx_validate;
