/* =========================================================================
target_run.sas - Macro publica del modulo Target (Metodo 2.1)

API:
%target_run(
    input_caslib  = PROC,
    train_table   = _train_input,
    oot_table     = _oot_input,
    output_caslib = OUT,
    troncal_id    = <id>,
    scope         = base | segNNN,
    run_id        = <run_id>
)

Flujo interno:
1) Resolver variables desde cfg_troncales (byvar, target, monto, def_cld)
2) Ejecutar contract (validaciones)
3) Generar reportes HTML + Excel (TRAIN + OOT en un solo reporte)
4) Cleanup

NOTA: No persiste tablas .sas7bdat (analisis visual solamente).
      Usa def_cld como fecha de cierre de default (default cerrado).

Dual-input: recibe train + oot promovidas por run_module(dual_input=1).

Compatibilidad: segmento y universo.
========================================================================= */
/* ---- Incluir componentes del modulo ----------------------------------- */
%include "&fw_root./src/modules/target/target_contract.sas";
%include "&fw_root./src/modules/target/impl/target_compute.sas";
%include "&fw_root./src/modules/target/impl/target_report.sas";

%macro target_run( input_caslib=PROC, train_table=_train_input, oot_table=
    _oot_input, output_caslib=OUT, troncal_id=, scope=, run_id=);

    /* ---- Return code ---------------------------------------------------- */
    %global _tgt_rc;
    %let _tgt_rc=0;

    %local _tgt_byvar _tgt_target _tgt_monto _tgt_def_cld _report_path
        _images_path _file_prefix _scope_abbr;

    %put NOTE:======================================================;
    %put NOTE: [target_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: train=&input_caslib..&train_table.;
    %put NOTE: oot=&input_caslib..&oot_table.;
    %put NOTE:======================================================;

    /* ==================================================================
       1) Resolver variables desde cfg_troncales
       ================================================================== */
    proc sql noprint;
        select strip(byvar) into :_tgt_byvar trimmed from casuser.cfg_troncales
            where troncal_id=&troncal_id.;
        select strip(target) into :_tgt_target trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(monto) into :_tgt_monto trimmed from casuser.cfg_troncales
            where troncal_id=&troncal_id.;
        select strip(put(def_cld, best.)) into :_tgt_def_cld trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
    quit;

    %put NOTE: [target_run] Variables resueltas: byvar=&_tgt_byvar.
        target=&_tgt_target. monto=&_tgt_monto. def_cld=&_tgt_def_cld.;

    /* ==================================================================
       Determinar rutas de salida (subcarpeta METOD2.1/)
       ================================================================== */
    %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
    %else %let _scope_abbr=base;

    %let _report_path=&fw_root./outputs/runs/&run_id./reports/METOD2.1;
    %let _images_path=&fw_root./outputs/runs/&run_id./images/METOD2.1;
    %let _file_prefix=target_troncal_&troncal_id._&_scope_abbr.;

    %put NOTE: [target_run] Output -> reports/METOD2.1/ + images/METOD2.1/;

    /* ==================================================================
       2) Contract - validaciones
       ================================================================== */
    %target_contract( input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., target=&_tgt_target., byvar=&_tgt_byvar.,
        monto_var=&_tgt_monto., def_cld=&_tgt_def_cld. );

    %if &_tgt_rc. ne 0 %then %do;
        %put ERROR: [target_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    /* ==================================================================
       3) Report - HTML + Excel (incluye computo inline)
       ================================================================== */
    %_target_report( input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., byvar=&_tgt_byvar., target=&_tgt_target.,
        monto_var=&_tgt_monto., def_cld=&_tgt_def_cld., report_path=
        &_report_path., images_path=&_images_path., file_prefix=
        &_file_prefix. );

    /* No se persisten tablas (analisis visual solamente) */
    %put NOTE:======================================================;
    %put NOTE: [target_run] FIN - &_file_prefix.;
    %put NOTE:======================================================;

%mend target_run;
