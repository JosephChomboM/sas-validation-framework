/* =========================================================================
steps/methods/metod_6/step_precision.sas
Step de modulo: Precision del Modelo (METOD6)

Compara promedio observado (target) vs score del modelo (PD/XB), con:
- precision total
- precision por segmento opcional
- precision ponderada por monto opcional
- filtro por default cerrado (def_cld)

Usa run_module con scope_input=1 (dataset unificado por scope).
========================================================================= */
/* ---- Dependencias ----------------------------------------------------- */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";

/* ---- CONFIGURACION DEL MODULO (editar aqui) --------------------------- */
/* prec_mode:
AUTO   -> usa target/pd|xb/monto/var_seg/byvar/def_cld desde config.sas.
CUSTOM -> permite overridear score, target, monto, segvar y def_cld.
          byvar sigue viniendo de config.sas.                             */
%let prec_mode=AUTO;

/* AUTO = prioriza PD y luego XB; tambien acepta PD o XB                 */
%let prec_score_source=AUTO;

/* 1 = calcula precision ponderada por monto si monto existe             */
%let prec_use_weighted=1;

/* AUTO = usa var_seg de config si existe; 1 = forzar; 0 = desactivar    */
%let prec_use_segmentation=AUTO;

/* Overrides para CUSTOM                                                 */
%let prec_custom_target= ;
%let prec_custom_score_var= ;
%let prec_custom_monto= ;
%let prec_custom_segvar= ;
%let prec_custom_def_cld= ;

/* ---- EJECUCION -------------------------------------------------------- */
%macro _step_precision;

    %local _run_precision _step_rc _step_status;
    %let _step_rc=0;
    %let _step_status=OK;
    %if %symexist(run_precision)=1 %then %let _run_precision=&run_precision.;
    %else %let _run_precision=0;

    %fw_log_start(step_name=step_precision, run_id=&run_id.,
        fw_root=&fw_root., log_stem=metod_6_step_precision);

    %if &_run_precision. ne 1 %then %do;
        %put NOTE: [step_precision] Modulo deshabilitado
            (run_precision=&_run_precision.). Saltando.;
        %let _step_status=SKIP;
        %goto _step_precision_end;
    %end;

    %put NOTE: [step_precision] Iniciando - scope=&ctx_scope.
        prec_mode=&prec_mode. score_source=&prec_score_source.
        weighted=&prec_use_weighted. segmentation=&prec_use_segmentation.;

    %_create_caslib(cas_path=&fw_root./data/processed, caslib_name=PROC,
        lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);
    %_create_caslib(cas_path=&fw_root./outputs/runs/&run_id., caslib_name=OUT,
        lib_caslib=OUT, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);

    %if %upcase(&ctx_scope.)=SEGMENTO %then %do;

        %if &ctx_n_segments.=0 %then %do;
            %put WARNING: [step_precision] Troncal &ctx_troncal_id. tiene 0
                segmentos. Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            %run_module(module=precision, troncal_id=&ctx_troncal_id., split=,
                seg_id=&ctx_seg_id., run_id=&run_id., scope_input=1);
        %end;
        %else %do;
            %do _sg=1 %to &ctx_n_segments.;
                %run_module(module=precision, troncal_id=&ctx_troncal_id.,
                    split=, seg_id=&_sg., run_id=&run_id., scope_input=1);
            %end;
        %end;

    %end;
    %else %if %upcase(&ctx_scope.)=UNIVERSO %then %do;

        %run_module(module=precision, troncal_id=&ctx_troncal_id., split=,
            seg_id=, run_id=&run_id., scope_input=1);

    %end;
    %else %do;
        %put ERROR: [step_precision] ctx_scope=&ctx_scope. no reconocido.;
    %end;

    %_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE:======================================================;
    %put NOTE: [step_precision] Completado (scope=&ctx_scope.
        mode=&prec_mode.);
    %put NOTE:======================================================;

%_step_precision_end:
    %fw_log_stop(step_name=step_precision, step_rc=&_step_rc.,
        step_status=&_step_status);

%mend _step_precision;
%_step_precision;
