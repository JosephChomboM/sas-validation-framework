/* =========================================================================
steps/methods/metod_8/step_calibracion.sas
Step de modulo: Calibracion / Backtesting por driver (METOD8)
========================================================================= */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";

/* ---- CONFIGURACION DEL MODULO (editar aqui) --------------------------- */
%let cal_mode=AUTO;
%let cal_score_source=AUTO;
%let cal_use_weighted=1;
%let cal_groups=5;

/* Overrides CUSTOM */
%let cal_custom_vars_dri_num=;
%let cal_custom_vars_dri_cat=;
%let cal_custom_target=;
%let cal_custom_score_var=;
%let cal_custom_monto=;
%let cal_custom_def_cld=;

%macro _step_calibracion;

    %local _run_calibracion _step_rc _step_status;
    %let _step_rc=0;
    %let _step_status=OK;
    %if %symexist(run_calibracion)=1 %then
        %let _run_calibracion=&run_calibracion.;
    %else %let _run_calibracion=0;

    %fw_log_start(step_name=step_calibracion, run_id=&run_id.,
        fw_root=&fw_root., log_stem=metod_8_step_calibracion);

    %if &_run_calibracion. ne 1 %then %do;
        %put NOTE: [step_calibracion] Modulo deshabilitado
            (run_calibracion=&_run_calibracion.). Saltando.;
        %let _step_status=SKIP;
        %goto _step_calibracion_end;
    %end;

    %put NOTE: [step_calibracion] Iniciando - scope=&ctx_scope.
        mode=&cal_mode. score_source=&cal_score_source.
        weighted=&cal_use_weighted. groups=&cal_groups.;

    %_create_caslib(cas_path=&fw_root./data/processed, caslib_name=PROC,
        lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);
    %_create_caslib(cas_path=&fw_root./outputs/runs/&run_id., caslib_name=OUT,
        lib_caslib=OUT, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);

    %if %upcase(&ctx_scope.)=SEGMENTO %then %do;
        %put NOTE: [step_calibracion] SEGMENTO: troncal=&ctx_troncal_id.
            n_segments=&ctx_n_segments. seg_id=&ctx_seg_id.;

        %if &ctx_n_segments.=0 %then %do;
            %put WARNING: [step_calibracion] Troncal &ctx_troncal_id. tiene 0
                segmentos. Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            %run_module(module=calibracion, troncal_id=&ctx_troncal_id.,
                split=, seg_id=&ctx_seg_id., run_id=&run_id., scope_input=1);
        %end;
        %else %do;
            %do _sg=1 %to &ctx_n_segments.;
                %run_module(module=calibracion, troncal_id=&ctx_troncal_id.,
                    split=, seg_id=&_sg., run_id=&run_id., scope_input=1);
            %end;
        %end;
    %end;
    %else %if %upcase(&ctx_scope.)=UNIVERSO %then %do;
        %put NOTE: [step_calibracion] UNIVERSO: troncal=&ctx_troncal_id.;
        %run_module(module=calibracion, troncal_id=&ctx_troncal_id., split=,
            seg_id=, run_id=&run_id., scope_input=1);
    %end;
    %else %do;
        %put ERROR: [step_calibracion] ctx_scope=&ctx_scope. no reconocido.;
    %end;

    %_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE:======================================================;
    %put NOTE: [step_calibracion] Completado (scope=&ctx_scope.
        mode=&cal_mode.);
    %put NOTE:======================================================;

%_step_calibracion_end:
    %fw_log_stop(step_name=step_calibracion, step_rc=&_step_rc.,
        step_status=&_step_status);

%mend _step_calibracion;
%_step_calibracion;
