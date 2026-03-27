/* =========================================================================
steps/methods/metod_4/step_gini.sas
Step de modulo: Gini (Metodo 4.3)

Alcance:
- Gini del modelo y de variables
- General y mensual
- PROC FREQTAB con opcion configurable missing/no-missing
========================================================================= */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";

/* ---- CONFIGURACION DEL MODULO (editar aqui) --------------------------- */
/* gini_mode:
AUTO   -> vars de cfg_segmentos/cfg_troncales y score desde pd/xb de config.
CUSTOM -> permite override de vars, target, score y def_cld.
          Outputs van a experiments/. */
%let gini_mode=AUTO;

/* Score del modelo: AUTO | PD | XB | CUSTOM                              */
%let gini_score_source=AUTO;

/* PROC FREQTAB:
1 -> incluir missings
0 -> excluir missings
Default requerido: 1 */
%let gini_with_missing=1;

/* Overrides opcionales de umbrales                                       */
%let gini_threshold_model_low=;
%let gini_threshold_model_high=;
%let gini_threshold_var_low=0.05;
%let gini_threshold_var_high=0.15;

/* Reglas complementarias                                                 */
%let gini_min_n_valid=30;
%let gini_delta_warn=0.05;
%let gini_trend_delta=0.03;
%let gini_plot_top_n=10;

/* Overrides CUSTOM                                                       */
%let gini_custom_vars_num=%superq(_id_custom_vars_num);
%let gini_custom_target=;
%let gini_custom_score_var=;
%let gini_custom_def_cld=;

%macro _step_gini;
    %local _step_rc _step_status;
    %let _step_rc=0;
    %let _step_status=OK;

    %fw_log_start(step_name=step_gini, run_id=&run_id.,
        fw_root=&fw_root., log_stem=metod_4_step_gini);

    %if %sysfunc(coalescec(%superq(run_gini), 0)) ne 1 %then %do;
        %put NOTE: [step_gini] Modulo deshabilitado (run_gini=&run_gini.).
            Saltando.;
        %let _step_status=SKIP;
        %goto _step_gini_end;
    %end;

    %put NOTE: [step_gini] Iniciando - scope=&ctx_scope. mode=&gini_mode.
        score_source=&gini_score_source. with_missing=&gini_with_missing.;
    %put NOTE: [step_gini] Gini usa scope_input=1 y deriva TRAIN/OOT dentro
        del modulo desde la base persistente unificada.;

    %_create_caslib(cas_path=&fw_root./data/processed, caslib_name=PROC,
        lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);
    %_create_caslib(cas_path=&fw_root./outputs/runs/&run_id., caslib_name=OUT,
        lib_caslib=OUT, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);

    %if %upcase(&ctx_scope.)=SEGMENTO %then %do;
        %put NOTE: [step_gini] SEGMENTO: troncal=&ctx_troncal_id.
            n_segments=&ctx_n_segments. seg_id=&ctx_seg_id.;

        %if &ctx_n_segments.=0 %then %do;
            %put WARNING: [step_gini] Troncal &ctx_troncal_id. tiene 0
                segmentos. Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            %run_module(module=gini, troncal_id=&ctx_troncal_id., split=,
                seg_id=&ctx_seg_id., run_id=&run_id., scope_input=1);
        %end;
        %else %do;
            %do _sg=1 %to &ctx_n_segments.;
                %run_module(module=gini, troncal_id=&ctx_troncal_id., split=,
                    seg_id=&_sg., run_id=&run_id., scope_input=1);
            %end;
        %end;
    %end;
    %else %if %upcase(&ctx_scope.)=UNIVERSO %then %do;
        %put NOTE: [step_gini] UNIVERSO: troncal=&ctx_troncal_id.;
        %run_module(module=gini, troncal_id=&ctx_troncal_id., split=, seg_id=,
            run_id=&run_id., scope_input=1);
    %end;
    %else %do;
        %put ERROR: [step_gini] ctx_scope=&ctx_scope. no reconocido. Debe ser
            SEGMENTO o UNIVERSO.;
    %end;

    %_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE:======================================================;
    %put NOTE: [step_gini] Completado (scope=&ctx_scope. mode=&gini_mode.);
    %put NOTE:======================================================;

%_step_gini_end:
    %fw_log_stop(step_name=step_gini, step_rc=&_step_rc.,
        step_status=&_step_status);

%mend _step_gini;
%_step_gini;
