/* =========================================================================
monotonicidad_run.sas - Macro publica del modulo Monotonicidad (METOD7)

API:
%monotonicidad_run(
    input_caslib  = PROC,
    input_table   = _scope_input,
    output_caslib = OUT,
    troncal_id    = <id>,
    scope         = base | segNNN,
    run_id        = <run_id>
)

Flujo interno:
1) Resolver pd, target, byvar, ventanas y def_cld desde cfg_troncales
2) Aplicar overrides exploratorios de target/def_cld si mono_mode=CUSTOM
3) Ejecutar contract sobre el input unificado
4) Construir detalle CAS-first y generar un reporte comparativo TRAIN/OOT

Regla de negocio:
- Monotonicidad aplica a una sola variable score: cfg_troncales.pd.
- Siempre usa default cerrado (def_cld) para filtrar el analisis.
- TRAIN/OOT se derivan dentro del modulo sobre una sola tabla de scope.
- byvar siempre se resuelve desde config.sas.

Compatibilidad: segmento y universo.
========================================================================= */
/* ---- Incluir componentes del modulo ----------------------------------- */
%include "&fw_root./src/modules/monotonicidad/monotonicidad_contract.sas";
%include "&fw_root./src/modules/monotonicidad/impl/monotonicidad_compute.sas";
%include "&fw_root./src/modules/monotonicidad/impl/monotonicidad_report.sas";

%macro monotonicidad_run(input_caslib=PROC, input_table=_scope_input,
    output_caslib=OUT, troncal_id=, scope=, run_id=);

    %global _mono_rc;
    %let _mono_rc=0;

    %local _mono_target _mono_pd _mono_byvar _mono_def_cld _mono_groups
        _scope_abbr _report_path _images_path _file_prefix _mono_is_custom
        _mono_train_min _mono_train_max _mono_oot_min _mono_oot_max;

    %put NOTE:======================================================;
    %put NOTE: [monotonicidad_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: input=&input_caslib..&input_table.;
    %put NOTE:======================================================;

    /* ==================================================================
       1) Resolver configuracion base
       ================================================================== */
    %let _mono_target= ;
    %let _mono_pd= ;
    %let _mono_byvar= ;
    %let _mono_def_cld= ;
    %let _mono_is_custom=0;
    %let _mono_train_min= ;
    %let _mono_train_max= ;
    %let _mono_oot_min= ;
    %let _mono_oot_max= ;

    proc sql noprint;
        select strip(target) into :_mono_target trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(pd) into :_mono_pd trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(byvar) into :_mono_byvar trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(put(def_cld, best.)) into :_mono_def_cld trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(put(train_min_mes, best.)) into :_mono_train_min trimmed
            from casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(put(train_max_mes, best.)) into :_mono_train_max trimmed
            from casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(put(oot_min_mes, best.)) into :_mono_oot_min trimmed
            from casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(put(oot_max_mes, best.)) into :_mono_oot_max trimmed
            from casuser.cfg_troncales where troncal_id=&troncal_id.;
    quit;

    /* ------ Modo CUSTOM: solo overrides exploratorios ------------------ */
    %if %upcase(&mono_mode.)=CUSTOM %then %do;
        %let _mono_is_custom=1;
        %if %length(%superq(mono_custom_target)) > 0 %then
            %let _mono_target=&mono_custom_target.;
        %if %length(%superq(mono_custom_def_cld)) > 0 %then
            %let _mono_def_cld=&mono_custom_def_cld.;
        %put NOTE: [monotonicidad_run] Modo CUSTOM activado.;
    %end;

    %if %symexist(mono_n_groups)=1 %then %let _mono_groups=&mono_n_groups.;
    %else %if %symexist(mono_groups)=1 %then %let _mono_groups=&mono_groups.;
    %else %let _mono_groups=5;
    %if %length(%superq(_mono_groups))=0 %then %let _mono_groups=5;

    %put NOTE: [monotonicidad_run] Configuracion resuelta:;
    %put NOTE: [monotonicidad_run] pd=&_mono_pd.;
    %put NOTE: [monotonicidad_run] byvar=&_mono_byvar.;
    %put NOTE: [monotonicidad_run] target=&_mono_target.;
    %put NOTE: [monotonicidad_run] def_cld=&_mono_def_cld. groups=&_mono_groups.;
    %put NOTE: [monotonicidad_run] train=&_mono_train_min.-&_mono_train_max.
        oot=&_mono_oot_min.-&_mono_oot_max.;

    /* ==================================================================
       2) Definir rutas de salida (METOD7)
       ================================================================== */
    %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
    %else %let _scope_abbr=base;

    %if &_mono_is_custom.=1 %then %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _images_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _file_prefix=custom_monotonicidad_troncal_&troncal_id._&_scope_abbr.;
        %put NOTE: [monotonicidad_run] Output -> experiments/ (exploratorio).;
    %end;
    %else %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./reports/METOD7;
        %let _images_path=&fw_root./outputs/runs/&run_id./images/METOD7;
        %let _file_prefix=monotonicidad_troncal_&troncal_id._&_scope_abbr.;
        %put NOTE: [monotonicidad_run] Output -> reports/METOD7 + images/METOD7.;
    %end;

    /* ==================================================================
       3) Contract - validaciones
       ================================================================== */
    %monotonicidad_contract(input_caslib=&input_caslib.,
        input_table=&input_table., score_var=&_mono_pd.,
        target=&_mono_target., byvar=&_mono_byvar., def_cld=&_mono_def_cld.,
        train_min_mes=&_mono_train_min., train_max_mes=&_mono_train_max.,
        oot_min_mes=&_mono_oot_min., oot_max_mes=&_mono_oot_max.);

    %if &_mono_rc. ne 0 %then %do;
        %put ERROR: [monotonicidad_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    /* ==================================================================
       4) Compute + Report - HTML + Excel + JPEG
       ================================================================== */
    %_monotonicidad_compute(input_caslib=&input_caslib.,
        input_table=&input_table., score_var=&_mono_pd.,
        target=&_mono_target., byvar=&_mono_byvar.,
        def_cld=&_mono_def_cld., groups=&_mono_groups.,
        train_min_mes=&_mono_train_min., train_max_mes=&_mono_train_max.,
        oot_min_mes=&_mono_oot_min., oot_max_mes=&_mono_oot_max.);

    %_monotonicidad_report(score_var=&_mono_pd., target=&_mono_target.,
        report_path=&_report_path., images_path=&_images_path.,
        file_prefix=&_file_prefix.);

    proc datasets library=casuser nolist nowarn;
        delete _mono_:;
    quit;

    %put NOTE:======================================================;
    %put NOTE: [monotonicidad_run] FIN - &_file_prefix. (mode=&mono_mode.);
    %put NOTE:======================================================;

%mend monotonicidad_run;
