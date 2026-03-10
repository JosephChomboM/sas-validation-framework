/* =========================================================================
monotonicidad_run.sas - Macro publica del modulo Monotonicidad (METOD7)

API:
%monotonicidad_run(
    input_caslib  = PROC,
    train_table   = _train_input,
    oot_table     = _oot_input,
    output_caslib = OUT,
    troncal_id    = <id>,
    scope         = base | segNNN,
    run_id        = <run_id>
)

Flujo interno:
1) Resolver variables desde cfg_troncales (byvar, pd/xb, target, def_cld)
2) Ejecutar contract (validaciones)
3) Generar reportes TRAIN/OOT con cortes compartidos
4) Cleanup

Regla de negocio:
- Usa default cerrado (def_cld) para filtrar analisis.
- pd es preferido; si pd esta vacio usa xb.

Dual-input: recibe train + oot promovidas por run_module(dual_input=1).
Compatibilidad: segmento y universo.
========================================================================= */
/* ---- Incluir componentes del modulo ----------------------------------- */
%include "&fw_root./src/modules/monotonicidad/monotonicidad_contract.sas";
%include "&fw_root./src/modules/monotonicidad/impl/monotonicidad_compute.sas";
%include "&fw_root./src/modules/monotonicidad/impl/monotonicidad_report.sas";

%macro monotonicidad_run(input_caslib=PROC, train_table=_train_input,
    oot_table=_oot_input, output_caslib=OUT, troncal_id=, scope=, run_id=);

    %global _mono_rc;
    %let _mono_rc=0;

    %local _mono_byvar _mono_target _mono_pd _mono_xb _mono_score
        _mono_def_cld _mono_groups _scope_abbr _report_path _images_path
        _file_prefix;

    %put NOTE:======================================================;
    %put NOTE: [monotonicidad_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: train=&input_caslib..&train_table.;
    %put NOTE: oot=&input_caslib..&oot_table.;
    %put NOTE:======================================================;

    /* ==================================================================
       1) Resolver variables desde cfg_troncales
       ================================================================== */
    proc sql noprint;
        select strip(byvar) into :_mono_byvar trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(target) into :_mono_target trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(pd) into :_mono_pd trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(xb) into :_mono_xb trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(put(def_cld, best.)) into :_mono_def_cld trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
    quit;

    %let _mono_score=&_mono_pd.;
    %if %length(%superq(_mono_score))=0 %then %let _mono_score=&_mono_xb.;

    %if %length(%superq(_mono_score))=0 %then %do;
        %put ERROR: [monotonicidad_run] No se pudo resolver score.
            Use pd o xb en cfg_troncales.;
        %return;
    %end;

    %if %symexist(mono_groups)=1 %then %let _mono_groups=&mono_groups.;
    %else %let _mono_groups=5;
    %if %length(%superq(_mono_groups))=0 %then %let _mono_groups=5;

    %put NOTE: [monotonicidad_run] Variables resueltas: byvar=&_mono_byvar.
        score=&_mono_score. target=&_mono_target. def_cld=&_mono_def_cld.
        groups=&_mono_groups.;

    /* ==================================================================
       2) Definir rutas de salida (METOD7)
       ================================================================== */
    %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
    %else %let _scope_abbr=base;

    %let _report_path=&fw_root./outputs/runs/&run_id./reports/METOD7;
    %let _images_path=&fw_root./outputs/runs/&run_id./images/METOD7;
    %let _file_prefix=monotonicidad_troncal_&troncal_id._&_scope_abbr.;

    %put NOTE: [monotonicidad_run] Output -> reports/METOD7 + images/METOD7.;

    /* ==================================================================
       3) Contract - validaciones
       ================================================================== */
    %monotonicidad_contract(input_caslib=&input_caslib.,
        train_table=&train_table., oot_table=&oot_table., byvar=&_mono_byvar.,
        score_var=&_mono_score., target_var=&_mono_target., def_cld=&_mono_def_cld.);

    %if &_mono_rc. ne 0 %then %do;
        %put ERROR: [monotonicidad_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    /* ==================================================================
       4) Report - HTML + Excel + JPEG
       ================================================================== */
    %_monotonicidad_report(input_caslib=&input_caslib.,
        train_table=&train_table., oot_table=&oot_table., byvar=&_mono_byvar.,
        score_var=&_mono_score., target_var=&_mono_target.,
        def_cld=&_mono_def_cld., groups=&_mono_groups.,
        report_path=&_report_path., images_path=&_images_path.,
        file_prefix=&_file_prefix.);

    %put NOTE:======================================================;
    %put NOTE: [monotonicidad_run] FIN - &_file_prefix.;
    %put NOTE:======================================================;

%mend monotonicidad_run;

