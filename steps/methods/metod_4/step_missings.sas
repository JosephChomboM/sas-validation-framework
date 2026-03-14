/* =========================================================================
steps/methods/metod_4/step_missings.sas
Step de modulo: Missings / Dummies (Metodo 4.2)

Flujo:
1) Check flag run_missings (skip si deshabilitado)
2) Configuracion propia del modulo (miss_mode, threshold, variables)
3) Crear CASLIBs PROC + OUT
4) Iteracion segun ctx_scope:
- SEGMENTO -> itera segmentos via run_module(dual_input=1)
- UNIVERSO -> ejecuta base via run_module(dual_input=1)
5) Cleanup CASLIBs

NOTA IMPORTANTE:
Missings compara TRAIN vs OOT -> usa run_module con dual_input=1.
run_module en modo B promueve ambas tablas (_train_input, _oot_input),
ejecuta %missings_run, y dropea ambas tablas promovidas.

Dependencias:
- &ctx_scope (SEGMENTO | UNIVERSO) - seteado por context_and_modules.sas
- &run_missings (0|1) - seteado por context_and_modules.sas
- &ctx_troncal_id - contexto comun
- SEGMENTO: &ctx_n_segments, &ctx_seg_id (ALL|N)
- casuser.cfg_troncales / cfg_segmentos (promovidas en Step 02)
- &fw_root., &run_id (Steps 01 y 02)

Cada step es independiente: carga sus propias dependencias.
========================================================================= */
/* ---- Dependencias ----------------------------------------------------- */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";

/* ---- CONFIGURACION DEL MODULO (editar aqui) --------------------------- */
/* miss_mode:
AUTO   -> usa variables de cfg_segmentos/cfg_troncales
(var_num_list, var_cat_list).
Outputs van a reports/ + images/ (validacion estandar).
CUSTOM -> usa miss_custom_vars_num/cat.
Outputs van a experiments/ (analisis exploratorio).           */
%let miss_mode=AUTO;

/* Umbral de missing: variables con pct_miss >= threshold se marcan en rojo */
%let miss_threshold=0.1;

/* Variables personalizadas (solo si miss_mode=CUSTOM)                     */
%let miss_custom_vars_num= ;
%let miss_custom_vars_cat=;

/* ---- EJECUCION -------------------------------------------------------- */
%macro _step_missings;
    %local _step_rc _step_status;
    %let _step_rc=0;
    %let _step_status=OK;

    %fw_log_start(step_name=step_missings, run_id=&run_id.,
        fw_root=&fw_root., log_stem=metod_4_step_missings);

    /* ---- 0) Check flag de habilitacion -------------------------------- */
    %if &run_missings. ne 1 %then %do;
        %put NOTE: [step_missings] Modulo deshabilitado
            (run_missings=&run_missings.). Saltando.;
        %let _step_status=SKIP;
        %goto _step_missings_end;
    %end;

    %put NOTE: [step_missings] Iniciando - scope=&ctx_scope.
        miss_mode=&miss_mode. threshold=&miss_threshold.;

    /* ---- 1) Crear CASLIBs PROC + OUT --------------------------------- */
    %_create_caslib( cas_path=&fw_root./data/processed, caslib_name=PROC,
        lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1 );
    %_create_caslib( cas_path=&fw_root./outputs/runs/&run_id., caslib_name=OUT,
        lib_caslib=OUT, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1 );

    /* ---- 2) Iterar segun ctx_scope ----------------------------------- */
    %if %upcase(&ctx_scope.)=SEGMENTO %then %do;

        %put NOTE: [step_missings] SEGMENTO: troncal=&ctx_troncal_id.
            n_segments=&ctx_n_segments. seg_id=&ctx_seg_id.;

        %if &ctx_n_segments.=0 %then %do;
            %put WARNING: [step_missings] Troncal &ctx_troncal_id. tiene 0
                segmentos. Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            /* Segmento especifico */
            %run_module(module=missings, troncal_id=&ctx_troncal_id., split=,
                seg_id=&ctx_seg_id., run_id=&run_id., dual_input=1);
        %end;
        %else %do;
            /* Todos los segmentos */
            %do _sg=1 %to &ctx_n_segments.;
                %run_module(module=missings, troncal_id=&ctx_troncal_id.,
                    split=, seg_id=&_sg., run_id=&run_id., dual_input=1);
            %end;
        %end;

    %end; /* fin SEGMENTO */
    %else %if %upcase(&ctx_scope.)=UNIVERSO %then %do;

        %put NOTE: [step_missings] UNIVERSO: troncal=&ctx_troncal_id.;

        %run_module(module=missings, troncal_id=&ctx_troncal_id., split=,
            seg_id=, run_id=&run_id., dual_input=1);

    %end; /* fin UNIVERSO */
    %else %do;
        %put ERROR: [step_missings] ctx_scope=&ctx_scope. no reconocido. Debe
            ser SEGMENTO o UNIVERSO.;
    %end;

    /* ---- 3) Cleanup CASLIBs ------------------------------------------ */
    %_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE:======================================================;
    %put NOTE: [step_missings] Completado (scope=&ctx_scope. mode=&miss_mode.);
    %put NOTE:======================================================;

%_step_missings_end:
    %fw_log_stop(step_name=step_missings, step_rc=&_step_rc.,
        step_status=&_step_status);

%mend _step_missings;
%_step_missings;
