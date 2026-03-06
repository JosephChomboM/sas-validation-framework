/* =========================================================================
universe_run.sas - Macro pública del módulo Universe (Método 1.1)

API:
%universe_run(
input_caslib  = PROC,
train_table   = _train_input,
oot_table     = _oot_input,
output_caslib = OUT,
troncal_id    = <id>,
scope         = base | segNNN,
run_id        = <run_id>
)

Flujo interno:
1) Resolver variables desde cfg_troncales (byvar, id_var_id, monto)
2) Ejecutar contract (validaciones)
3) Generar reportes HTML + Excel (TRAIN + OOT en un solo reporte)
4) Cleanup

NOTA: No persiste tablas .sas7bdat (análisis visual solamente).

Dual-input: recibe train + oot promovidas por run_module(dual_input=1).

Compatibilidad: segmento y universo.
========================================================================= */
/* ---- Incluir componentes del módulo ----------------------------------- */
%include "&fw_root./src/modules/universe/universe_contract.sas";
%include "&fw_root./src/modules/universe/impl/universe_compute.sas";
%include "&fw_root./src/modules/universe/impl/universe_report.sas";

%macro universe_run( input_caslib=PROC, train_table=_train_input, oot_table=
    _oot_input, output_caslib=OUT, troncal_id=, scope=, run_id=);

    /* ---- Return code ---------------------------------------------------- */
    %global _univ_rc;
    %let _univ_rc=0;

    %local _univ_byvar _univ_id_var _univ_monto _report_path _file_prefix
        _scope_abbr;

    %put NOTE:======================================================;
    %put NOTE: [universe_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: train=&input_caslib..&train_table.;
    %put NOTE: oot=&input_caslib..&oot_table.;
    %put NOTE:======================================================;

    /* ==================================================================
    1) Resolver variables desde cfg_troncales
    ================================================================== */
    proc sql noprint;
        select strip(byvar) into :_univ_byvar trimmed from casuser.cfg_troncales
            where troncal_id=&troncal_id.;
        select strip(id_var_id) into :_univ_id_var trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(monto) into :_univ_monto trimmed from casuser.cfg_troncales
            where troncal_id=&troncal_id.;
    quit;

    %put NOTE: [universe_run] Variables resueltas: byvar=&_univ_byvar.
        id_var=&_univ_id_var. monto=&_univ_monto.;

    /* ==================================================================
    Determinar rutas de salida (subcarpeta metod_1_1/)
    ================================================================== */
    %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
    %else %let _scope_abbr=base;

    %let _report_path=&fw_root./outputs/runs/&run_id./reports/metod_1_1;
    %let _file_prefix=universe_troncal_&troncal_id._&_scope_abbr.;

    %put NOTE: [universe_run] Output → reports/metod_1_1/;

    /* ==================================================================
    2) Contract - validaciones
    ================================================================== */
    %universe_contract( input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., byvar=&_univ_byvar., id_var=&_univ_id_var.,
        monto_var=&_univ_monto. );

    %if &_univ_rc. ne 0 %then %do;
        %put ERROR: [universe_run] Contract fallido - módulo abortado.;
        %return;
    %end;

    /* ==================================================================
    3) Report - HTML + Excel (incluye cómputo inline)
    ================================================================== */
    %_universe_report( input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., byvar=&_univ_byvar., id_var=&_univ_id_var.,
        monto_var=&_univ_monto., report_path=&_report_path., file_prefix=
        &_file_prefix. );

    /* No se persisten tablas (análisis visual solamente) */
    %put NOTE:======================================================;
    %put NOTE: [universe_run] FIN - &_file_prefix.;
    %put NOTE:======================================================;

%mend universe_run;
