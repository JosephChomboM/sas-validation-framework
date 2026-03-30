/* =========================================================================
steps/methods/metod_6/step_similitud.sas
Step de modulo: Similitud de Muestras (Metodo 6)

Flujo:
1) Check flag run_similitud
2) Configuracion del modulo (AUTO/CUSTOM, n_groups)
3) Crear CASLIBs PROC + OUT
4) Iteracion por scope via run_module(scope_input=1)
5) Cleanup CASLIBs

Similitud usa scope_input=1:
- Recibe _scope_input (base/segmento unificada)
- Deriva TRAIN/OOT dentro del modulo con columna Split
========================================================================= */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";

/* ---- CONFIGURACION DEL MODULO ---------------------------------------- */
%let simil_mode=AUTO;
%let simil_n_groups=5;
%let simil_custom_vars_num=;
%let simil_custom_vars_cat=;

%macro _step_similitud;
    %local _step_rc _step_status;
    %let _step_rc=0;
    %let _step_status=OK;

    %fw_log_start(step_name=step_similitud, run_id=&run_id.,
        fw_root=&fw_root., log_stem=metod_6_step_similitud);

    %if &run_similitud. ne 1 %then %do;
        %put NOTE: [step_similitud] Modulo deshabilitado (run_similitud=&run_similitud.). Saltando.;
        %let _step_status=SKIP;
        %goto _step_similitud_end;
    %end;

    %put NOTE: [step_similitud] Iniciando - scope=&ctx_scope. simil_mode=&simil_mode. n_groups=&simil_n_groups.;
    %put NOTE: [step_similitud] Similitud usa scope_input=1 y deriva TRAIN/OOT dentro del modulo desde la base unificada.;

    %_create_caslib(cas_path=&fw_root./data/processed, caslib_name=PROC,
        lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);
    %_create_caslib(cas_path=&fw_root./outputs/runs/&run_id., caslib_name=OUT,
        lib_caslib=OUT, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);

    %if %upcase(&ctx_scope.) = SEGMENTO %then %do;
        %put NOTE: [step_similitud] SEGMENTO: troncal=&ctx_troncal_id. n_segments=&ctx_n_segments. seg_id=&ctx_seg_id.;

        %if &ctx_n_segments. = 0 %then %do;
            %put WARNING: [step_similitud] Troncal &ctx_troncal_id. tiene 0 segmentos. Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            %run_module(module=similitud, troncal_id=&ctx_troncal_id., split=,
                seg_id=&ctx_seg_id., run_id=&run_id., scope_input=1);
        %end;
        %else %do;
            %do _sg = 1 %to &ctx_n_segments.;
                %run_module(module=similitud, troncal_id=&ctx_troncal_id.,
                    split=, seg_id=&_sg., run_id=&run_id., scope_input=1);
            %end;
        %end;
    %end;
    %else %if %upcase(&ctx_scope.) = UNIVERSO %then %do;
        %put NOTE: [step_similitud] UNIVERSO: troncal=&ctx_troncal_id.;

        %run_module(module=similitud, troncal_id=&ctx_troncal_id., split=,
            seg_id=, run_id=&run_id., scope_input=1);
    %end;
    %else %do;
        %put ERROR: [step_similitud] ctx_scope=&ctx_scope. no reconocido. Debe ser SEGMENTO o UNIVERSO.;
    %end;

    %_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE:======================================================;
    %put NOTE: [step_similitud] Completado (scope=&ctx_scope. mode=&simil_mode.);
    %put NOTE:======================================================;

%_step_similitud_end:
    %fw_log_stop(step_name=step_similitud, step_rc=&_step_rc.,
        step_status=&_step_status);

%mend _step_similitud;
%_step_similitud;
