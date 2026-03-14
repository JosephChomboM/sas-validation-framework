/* =========================================================================
steps/methods/metod_3/step_segmentacion.sas
Step de modulo: Segmentacion (Metodo 3)

Flujo:
1) Check flag run_segmentacion (skip si deshabilitado)
2) Configuracion propia del modulo (seg_mode, min_obs, min_target, etc.)
3) Validar scope: solo UNIVERSO (segmentacion necesita datos completos)
4) Crear CASLIBs PROC + OUT
5) Iteracion segun ctx_split:
   - Si ctx_split=TRAIN o OOT -> ejecuta solo ese split
   - Si ctx_split no es individual -> ejecuta ambos splits
6) Cleanup CASLIBs

NOTA IMPORTANTE:
Segmentacion analiza segmentos DENTRO de un dataset completo.
Solo tiene sentido en scope UNIVERSO (base.sashdat) donde la variable
segmentadora esta presente. En scope SEGMENTO los datos ya estan
separados por segmento - no hay nada que analizar.

Segmentacion usa target -> fecha de corte es def_cld.

Single-input (dual_input=0): cada split se ejecuta independientemente.
run_module promueve base.sashdat como _active_input.

Funcionalidad:
- Materialidad global y por segmento (min obs + min target)
- Test KS de heterogeneidad entre pares de segmentos
- Test Kruskal-Wallis para diferencias entre segmentos
- Analisis de migracion de segmentos entre periodos
- Distribucion mensual por segmento (graficos)
- Reportes: Excel multi-hoja + HTML + JPEG

Dependencias:
- &ctx_scope (SEGMENTO | UNIVERSO) - seteado por context_and_modules.sas
- &run_segmentacion (0|1) - seteado por context_and_modules.sas
- &ctx_troncal_id, &ctx_split - contexto comun
- casuser.cfg_troncales (promovida en Step 02)
- &fw_root., &run_id (Steps 01 y 02)

Cada step es independiente: carga sus propias dependencias.
========================================================================= */
/* ---- Dependencias ----------------------------------------------------- */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";

/* ---- CONFIGURACION DEL MODULO (editar aqui) --------------------------- */
/* seg_mode:
    AUTO   -> resuelve target, var_seg, byvar, id_var_id desde config.
              Outputs van a reports/ + images/ + tables/ (validacion estandar).
    CUSTOM -> usa seg_custom_* overrides (los no definidos se toman de config).
              Outputs van a experiments/ (analisis exploratorio).            */
%let seg_mode = AUTO;

/* Minimo de observaciones por segmento para materialidad                  */
%let seg_min_obs = 1000;

/* Minimo de eventos target por segmento para materialidad                 */
%let seg_min_target = 450;

/* Modo de graficos: 0=combinado (vbar+vline), 1=separados                 */
%let seg_plot_sep = 0;

/* Variables personalizadas (solo si seg_mode=CUSTOM)
   Dejar en blanco para usar valores de config.                            */
%let seg_custom_target = ;
%let seg_custom_segvar = ;
%let seg_custom_byvar = ;
%let seg_custom_idvar = ;

/* ---- EJECUCION -------------------------------------------------------- */
%macro _step_segmentacion;
    %local _step_rc;
    %let _step_rc=0;

    %fw_log_start(step_name=step_segmentacion, run_id=&run_id.,
        fw_root=&fw_root., log_stem=metod_3_step_segmentacion);

    /* ---- 0) Check flag de habilitacion -------------------------------- */
    %if &run_segmentacion. ne 1 %then %do;
        %put NOTE: [step_segmentacion] Modulo deshabilitado
            (run_segmentacion=&run_segmentacion.). Saltando.;
        %goto _step_segmentacion_end;
    %end;

    %put NOTE: [step_segmentacion] Iniciando - scope=&ctx_scope.
        seg_mode=&seg_mode. min_obs=&seg_min_obs.
        min_target=&seg_min_target.;

    /* ---- 1) Validar scope: solo UNIVERSO ------------------------------ */
    %if %upcase(&ctx_scope.) = SEGMENTO %then %do;
        %put WARNING: [step_segmentacion] Segmentacion solo aplica a
            scope UNIVERSO. ctx_scope=SEGMENTO -> Saltando.;
        %goto _step_segmentacion_end;
    %end;

    %if %upcase(&ctx_scope.) ne UNIVERSO %then %do;
        %put ERROR: [step_segmentacion] ctx_scope=&ctx_scope. no reconocido.
            Debe ser UNIVERSO.;
        %let _step_rc=1;
        %goto _step_segmentacion_end;
    %end;

    /* ---- 2) Crear CASLIBs PROC + OUT --------------------------------- */
    %_create_caslib(cas_path=&fw_root./data/processed, caslib_name=PROC,
        lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);
    %_create_caslib(cas_path=&fw_root./outputs/runs/&run_id., caslib_name=OUT,
        lib_caslib=OUT, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);

    /* ---- 3) Resolver splits ------------------------------------------ */
    %local _sp1 _sp2;

    %if %upcase(&ctx_split.) = TRAIN %then %do;
        %let _sp1 = train;
        %let _sp2 = ;
    %end;
    %else %if %upcase(&ctx_split.) = OOT %then %do;
        %let _sp1 = oot;
        %let _sp2 = ;
    %end;
    %else %do;
        %let _sp1 = train;
        %let _sp2 = oot;
    %end;

    /* ---- 4) Ejecutar base (universo) del troncal por split ----------- */
    %put NOTE: [step_segmentacion] UNIVERSO: troncal=&ctx_troncal_id.
        split=&ctx_split.;

    %if %superq(_sp1) ne %then
        %run_module(module=segmentacion, troncal_id=&ctx_troncal_id.,
            split=&_sp1., seg_id=, run_id=&run_id., dual_input=0);
    %if %superq(_sp2) ne %then
        %run_module(module=segmentacion, troncal_id=&ctx_troncal_id.,
            split=&_sp2., seg_id=, run_id=&run_id., dual_input=0);

    /* ---- 5) Cleanup CASLIBs ------------------------------------------ */
    %_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE:======================================================;
    %put NOTE: [step_segmentacion] Completado (scope=&ctx_scope.
        mode=&seg_mode.);
    %put NOTE:======================================================;

%_step_segmentacion_end:
    %fw_log_stop(step_name=step_segmentacion, step_rc=&_step_rc.);

%mend _step_segmentacion;
%_step_segmentacion;
