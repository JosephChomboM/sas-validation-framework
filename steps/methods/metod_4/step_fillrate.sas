/* =========================================================================
steps/methods/metod_4/step_fillrate.sas
Step de modulo: Fillrate vs Gini (Metodo 4.2)

Flujo:
1) Check flag run_fillrate
2) Configuracion propia del modulo (AUTO/CUSTOM)
3) Crear CASLIBs PROC + OUT
4) Iteracion segun ctx_scope con run_module(dual_input=1)
5) Cleanup CASLIBs

NOTA IMPORTANTE:
- Fillrate compara TRAIN vs OOT.
- El Gini se calcula con PROC FREQTAB sin MISSING y usando _SMDCR_.
- byvar siempre se resuelve desde config.sas.
========================================================================= */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";

/* ---- CONFIGURACION DEL MODULO (editar aqui) --------------------------- */
/* fill_mode:
AUTO   -> usa variables de cfg_segmentos/cfg_troncales + target/byvar/def_cld
          desde config.sas. Outputs a reports/images/tables.
CUSTOM -> usa fill_custom_vars_num/cat y permite override de target/def_cld.
          Outputs a experiments/. */
%let fill_mode=AUTO;

/* Variables personalizadas (solo si fill_mode=CUSTOM) */
%let fill_custom_vars_num=;
%let fill_custom_vars_cat=;
%let fill_custom_target=;
%let fill_custom_def_cld=;

%macro _step_fillrate;
    %local _step_rc;
    %let _step_rc=0;

    %fw_log_start(step_name=step_fillrate, run_id=&run_id.,
        fw_root=&fw_root., log_stem=metod_4_step_fillrate);

    %if %sysfunc(coalescec(%superq(run_fillrate), 0)) ne 1 %then %do;
        %put NOTE: [step_fillrate] Modulo deshabilitado
            (run_fillrate=&run_fillrate.). Saltando.;
        %goto _step_fillrate_end;
    %end;

    %put NOTE: [step_fillrate] Iniciando - scope=&ctx_scope.
        fill_mode=&fill_mode.;

    %_create_caslib(cas_path=&fw_root./data/processed, caslib_name=PROC,
        lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);
    %_create_caslib(cas_path=&fw_root./outputs/runs/&run_id., caslib_name=OUT,
        lib_caslib=OUT, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);

    %if %upcase(&ctx_scope.)=SEGMENTO %then %do;
        %put NOTE: [step_fillrate] SEGMENTO: troncal=&ctx_troncal_id.
            n_segments=&ctx_n_segments. seg_id=&ctx_seg_id.;

        %if &ctx_n_segments.=0 %then %do;
            %put WARNING: [step_fillrate] Troncal &ctx_troncal_id. tiene 0
                segmentos. Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            %run_module(module=fillrate, troncal_id=&ctx_troncal_id., split=,
                seg_id=&ctx_seg_id., run_id=&run_id., dual_input=1);
        %end;
        %else %do;
            %do _sg=1 %to &ctx_n_segments.;
                %run_module(module=fillrate, troncal_id=&ctx_troncal_id.,
                    split=, seg_id=&_sg., run_id=&run_id., dual_input=1);
            %end;
        %end;
    %end;
    %else %if %upcase(&ctx_scope.)=UNIVERSO %then %do;
        %put NOTE: [step_fillrate] UNIVERSO: troncal=&ctx_troncal_id.;
        %run_module(module=fillrate, troncal_id=&ctx_troncal_id., split=,
            seg_id=, run_id=&run_id., dual_input=1);
    %end;
    %else %do;
        %put ERROR: [step_fillrate] ctx_scope=&ctx_scope. no reconocido. Debe
            ser SEGMENTO o UNIVERSO.;
    %end;

    %_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE:======================================================;
    %put NOTE: [step_fillrate] Completado (scope=&ctx_scope.
        mode=&fill_mode.);
    %put NOTE:======================================================;

%_step_fillrate_end:
    %fw_log_stop(step_name=step_fillrate, step_rc=&_step_rc.);

%mend _step_fillrate;
%_step_fillrate;
