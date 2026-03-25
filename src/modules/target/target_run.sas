/* =========================================================================
target_run.sas - Macro publica del modulo Target (Metodo 2.1)

Flujo:
1) Resolver byvar, target, monto y def_cld desde cfg_troncales
2) Validar contexto y disponibilidad de datos
3) Calcular metricas CAS-first y publicar temporales en casuser
4) Generar un reporte consolidado TRAIN + OOT
5) Limpiar temporales de casuser
========================================================================= */

%include "&fw_root./src/modules/target/target_contract.sas";
%include "&fw_root./src/modules/target/impl/target_compute.sas";
%include "&fw_root./src/modules/target/impl/target_report.sas";

%macro target_run(input_caslib=PROC, train_table=_train_input,
    oot_table=_oot_input, output_caslib=OUT, troncal_id=, scope=, run_id=);

    %global _tgt_rc _tgt_has_monto;
    %let _tgt_rc=0;
    %let _tgt_has_monto=0;

    %local _tgt_byvar _tgt_target _tgt_monto _tgt_def_cld _report_path
        _images_path _file_prefix _scope_abbr _dir_rc;

    %put NOTE:======================================================;
    %put NOTE: [target_run] INICIO;
    %put NOTE: [target_run] troncal=&troncal_id. scope=&scope.;
    %put NOTE: [target_run] train=&input_caslib..&train_table.;
    %put NOTE: [target_run] oot=&input_caslib..&oot_table.;
    %put NOTE:======================================================;

    proc sql noprint;
        select strip(byvar) into :_tgt_byvar trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;

        select strip(target) into :_tgt_target trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;

        select strip(monto) into :_tgt_monto trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;

        select strip(put(def_cld, best.)) into :_tgt_def_cld trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;
    quit;

    %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
    %else %let _scope_abbr=base;

    %let _report_path=&fw_root./outputs/runs/&run_id./reports/METOD2.1;
    %let _images_path=&fw_root./outputs/runs/&run_id./images/METOD2.1;
    %let _file_prefix=target_troncal_&troncal_id._&_scope_abbr.;

    %let _dir_rc=%sysfunc(dcreate(METOD2.1,
        &fw_root./outputs/runs/&run_id./reports));
    %let _dir_rc=%sysfunc(dcreate(METOD2.1,
        &fw_root./outputs/runs/&run_id./images));

    %put NOTE: [target_run] Variables resueltas:;
    %put NOTE: [target_run] byvar=&_tgt_byvar.;
    %put NOTE: [target_run] target=&_tgt_target.;
    %put NOTE: [target_run] monto=&_tgt_monto.;
    %put NOTE: [target_run] def_cld=&_tgt_def_cld.;

    %target_contract(input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., target=&_tgt_target., byvar=&_tgt_byvar.,
        monto_var=&_tgt_monto., def_cld=&_tgt_def_cld.);

    %if &_tgt_rc. ne 0 %then %do;
        %put ERROR: [target_run] Contract fallido. Se aborta target.;
        %return;
    %end;

    %_target_compute(input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., byvar=&_tgt_byvar., target=&_tgt_target.,
        monto_var=&_tgt_monto., def_cld=&_tgt_def_cld.,
        has_monto=&_tgt_has_monto.);

    %_target_report(input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., byvar=&_tgt_byvar., target=&_tgt_target.,
        monto_var=&_tgt_monto., def_cld=&_tgt_def_cld.,
        has_monto=&_tgt_has_monto., report_path=&_report_path.,
        images_path=&_images_path., file_prefix=&_file_prefix.);
    proc datasets library=casuser nolist nowarn;
        delete _tgt_:;
    quit;

    %put NOTE:======================================================;
    %put NOTE: [target_run] FIN - &_file_prefix.;
    %put NOTE:======================================================;

%mend target_run;
