/* =========================================================================
universe_run.sas - Public macro for the Universe module (Method 1.1)

CAS-first flow:
1) Receive a unified scope table from run_module
2) Resolve variables and windows from casuser.cfg_troncales
3) Validate the scope table and TRAIN/OOT coverage
4) Generate HTML + Excel reports with TRAIN and OOT derived in-module

The physical input is always a single persisted scope:
- data/processed/troncal_X/base.sashdat
- data/processed/troncal_X/segNNN.sashdat
======================================================================== */
%include "&fw_root./src/modules/universe/universe_contract.sas";
%include "&fw_root./src/modules/universe/impl/universe_compute.sas";
%include "&fw_root./src/modules/universe/impl/universe_report.sas";

%macro universe_run(input_caslib=PROC, input_table=_scope_input,
    output_caslib=OUT, troncal_id=, scope=, run_id=);

    %global _univ_rc _univ_mean _univ_std;
    %let _univ_rc=0;
    %let _univ_mean=0;
    %let _univ_std=0;

    %local _univ_byvar _univ_id_var _univ_monto _univ_train_min
        _univ_train_max _univ_oot_min _univ_oot_max _report_path _images_path
        _file_prefix _scope_abbr;

    %put NOTE:======================================================;
    %put NOTE: [universe_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: input=&input_caslib..&input_table.;
    %put NOTE:======================================================;

    proc sql noprint;
        select strip(byvar),
               strip(id_var_id),
               strip(monto),
               strip(put(train_min_mes, best.)),
               strip(put(train_max_mes, best.)),
               strip(put(oot_min_mes, best.)),
               strip(put(oot_max_mes, best.))
          into :_univ_byvar trimmed,
               :_univ_id_var trimmed,
               :_univ_monto trimmed,
               :_univ_train_min trimmed,
               :_univ_train_max trimmed,
               :_univ_oot_min trimmed,
               :_univ_oot_max trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;
    quit;

    %if %length(%superq(_univ_byvar))=0 or %length(%superq(_univ_id_var))=0
        or %length(%superq(_univ_train_min))=0 or
        %length(%superq(_univ_train_max))=0 or
        %length(%superq(_univ_oot_min))=0 or
        %length(%superq(_univ_oot_max))=0 %then %do;
        %put ERROR: [universe_run] No se pudo resolver la configuracion de la troncal &troncal_id..;
        %let _univ_rc=1;
        %return;
    %end;

    %put NOTE: [universe_run] Variables resueltas: byvar=&_univ_byvar.
        id_var=&_univ_id_var. monto=&_univ_monto.;
    %put NOTE: [universe_run] Ventanas: TRAIN=&_univ_train_min.-&_univ_train_max.
        OOT=&_univ_oot_min.-&_univ_oot_max..;

    %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
    %else %let _scope_abbr=base;

    %let _report_path=&fw_root./outputs/runs/&run_id./reports/METOD1.1;
    %let _images_path=&fw_root./outputs/runs/&run_id./images/METOD1.1;
    %let _file_prefix=universe_troncal_&troncal_id._&_scope_abbr.;

    %put NOTE: [universe_run] Output -> reports/METOD1.1/ + images/METOD1.1/;

    %universe_contract(input_caslib=&input_caslib., input_table=&input_table.,
        byvar=&_univ_byvar., id_var=&_univ_id_var., monto_var=&_univ_monto.,
        train_min_mes=&_univ_train_min., train_max_mes=&_univ_train_max.,
        oot_min_mes=&_univ_oot_min., oot_max_mes=&_univ_oot_max.);

    %if &_univ_rc. ne 0 %then %do;
        %put ERROR: [universe_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    %_universe_report(input_caslib=&input_caslib., input_table=&input_table.,
        byvar=&_univ_byvar., id_var=&_univ_id_var., monto_var=&_univ_monto.,
        train_min_mes=&_univ_train_min., train_max_mes=&_univ_train_max.,
        oot_min_mes=&_univ_oot_min., oot_max_mes=&_univ_oot_max.,
        report_path=&_report_path., images_path=&_images_path.,
        file_prefix=&_file_prefix.);

    %put NOTE:======================================================;
    %put NOTE: [universe_run] FIN - &_file_prefix.;
    %put NOTE:======================================================;

%mend universe_run;
