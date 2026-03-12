/* =========================================================================
fillrate_run.sas - Macro publica del modulo Fillrate vs Gini (Metodo 4.2)

API:
%fillrate_run(
input_caslib  = PROC,
train_table   = _train_input,
oot_table     = _oot_input,
output_caslib = OUT,
troncal_id    = <id>,
scope         = base | segNNN,
run_id        = <run_id>
)

Flujo interno:
1) Resolver variables desde cfg_troncales/cfg_segmentos
   (byvar, target, vars num/cat, def_cld)
2) Ejecutar contract (validaciones)
3) Generar reportes HTML + Excel + JPEG
4) Persistir tablas resumen como .sas7bdat
5) Cleanup

NOTA:
- Fillrate compara TRAIN vs OOT y por eso usa dual_input=1.
- El Gini se calcula con PROC FREQTAB usando _SMDCR_ y SIN option MISSING.
- def_cld define la fecha maxima para el analisis (default cerrado).

Compatibilidad: segmento y universo.
========================================================================= */
%include "&fw_root./src/modules/fillrate/fillrate_contract.sas";
%include "&fw_root./src/modules/fillrate/impl/fillrate_compute.sas";
%include "&fw_root./src/modules/fillrate/impl/fillrate_report.sas";

%macro fillrate_run(input_caslib=PROC, train_table=_train_input,
    oot_table=_oot_input, output_caslib=OUT, troncal_id=, scope=, run_id=);

    %global _fill_rc;
    %let _fill_rc=0;

    %local _fill_vars_num _fill_vars_cat _fill_byvar _fill_target
        _fill_def_cld _fill_is_custom _scope_abbr _report_path _images_path
        _tables_path _file_prefix _tbl_prefix _seg_num _dir_rc;

    %put NOTE:======================================================;
    %put NOTE: [fillrate_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: train=&input_caslib..&train_table.;
    %put NOTE: oot=&input_caslib..&oot_table.;
    %put NOTE: mode=&fill_mode.;
    %put NOTE:======================================================;

    %let _fill_vars_num=;
    %let _fill_vars_cat=;
    %let _fill_byvar=;
    %let _fill_target=;
    %let _fill_def_cld=;
    %let _fill_is_custom=0;

    /* Resolver campos estructurales desde config */
    proc sql noprint;
        select strip(byvar) into :_fill_byvar trimmed from casuser.cfg_troncales
            where troncal_id=&troncal_id.;
        select strip(target) into :_fill_target trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(put(def_cld, best.)) into :_fill_def_cld trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
    quit;

    /* CUSTOM: variables manuales + target/def_cld opcionales */
    %if %upcase(&fill_mode.)=CUSTOM %then %do;
        %if %length(%superq(fill_custom_vars_num)) > 0 or
            %length(%superq(fill_custom_vars_cat)) > 0 %then %do;
            %let _fill_vars_num=&fill_custom_vars_num.;
            %let _fill_vars_cat=&fill_custom_vars_cat.;
            %let _fill_is_custom=1;

            %if %length(%superq(fill_custom_target)) > 0 %then
                %let _fill_target=&fill_custom_target.;
            %if %length(%superq(fill_custom_def_cld)) > 0 %then
                %let _fill_def_cld=&fill_custom_def_cld.;

            %put NOTE: [fillrate_run] Modo CUSTOM activado.;
        %end;
        %else %do;
            %put WARNING: [fillrate_run] fill_mode=CUSTOM pero sin variables
                custom. Fallback a AUTO.;
        %end;
    %end;

    /* AUTO (o fallback) */
    %if &_fill_is_custom.=0 %then %do;
        %put NOTE: [fillrate_run] Modo AUTO - resolviendo vars desde config.;

        %if %substr(&scope., 1, 3)=seg %then %do;
            %let _seg_num=%sysfunc(inputn(%substr(&scope., 4), best.));

            proc sql noprint;
                select strip(num_list) into :_fill_vars_num trimmed from
                    casuser.cfg_segmentos where troncal_id=&troncal_id. and
                    seg_id=&_seg_num.;
                select strip(cat_list) into :_fill_vars_cat trimmed from
                    casuser.cfg_segmentos where troncal_id=&troncal_id. and
                    seg_id=&_seg_num.;
            quit;
        %end;

        %if %length(%superq(_fill_vars_num))=0 %then %do;
            proc sql noprint;
                select strip(num_unv) into :_fill_vars_num trimmed from
                    casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;

        %if %length(%superq(_fill_vars_cat))=0 %then %do;
            proc sql noprint;
                select strip(cat_unv) into :_fill_vars_cat trimmed from
                    casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;
    %end;

    %put NOTE: [fillrate_run] Variables resueltas:;
    %put NOTE: [fillrate_run] num=&_fill_vars_num.;
    %put NOTE: [fillrate_run] cat=&_fill_vars_cat.;
    %put NOTE: [fillrate_run] byvar=&_fill_byvar.;
    %put NOTE: [fillrate_run] target=&_fill_target.;
    %put NOTE: [fillrate_run] def_cld=&_fill_def_cld.;

    %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
    %else %let _scope_abbr=base;

    %if &_fill_is_custom.=1 %then %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _images_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _tables_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _file_prefix=custom_fill_troncal_&troncal_id._&_scope_abbr.;
        %let _tbl_prefix=cx_fill_t&troncal_id._&_scope_abbr.;
        %put NOTE: [fillrate_run] Output -> experiments/ (exploratorio);
    %end;
    %else %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./reports/METOD4.2;
        %let _images_path=&fw_root./outputs/runs/&run_id./images/METOD4.2;
        %let _tables_path=&fw_root./outputs/runs/&run_id./tables/METOD4.2;
        %let _file_prefix=fillrate_troncal_&troncal_id._&_scope_abbr.;
        %let _tbl_prefix=fill_t&troncal_id._&_scope_abbr.;
        %let _dir_rc=%sysfunc(dcreate(METOD4.2,
            &fw_root./outputs/runs/&run_id./reports));
        %let _dir_rc=%sysfunc(dcreate(METOD4.2,
            &fw_root./outputs/runs/&run_id./images));
        %let _dir_rc=%sysfunc(dcreate(METOD4.2,
            &fw_root./outputs/runs/&run_id./tables));
        %put NOTE: [fillrate_run] Output -> reports/images/tables METOD4.2.;
    %end;

    %fillrate_contract(input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., vars_num=&_fill_vars_num.,
        vars_cat=&_fill_vars_cat., byvar=&_fill_byvar.,
        target=&_fill_target., def_cld=&_fill_def_cld.);

    %if &_fill_rc. ne 0 %then %do;
        %put ERROR: [fillrate_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    %_fillrate_report(input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., byvar=&_fill_byvar., target=&_fill_target.,
        def_cld=&_fill_def_cld., vars_num=&_fill_vars_num.,
        vars_cat=&_fill_vars_cat., report_path=&_report_path.,
        images_path=&_images_path., file_prefix=&_file_prefix.);

    %if %sysfunc(exist(casuser._fill_general_all)) %then %do;
        libname _fillout "&_tables_path.";

        data _fillout.&_tbl_prefix._gnrl;
            set casuser._fill_general_all;
        run;

        data _fillout.&_tbl_prefix._mnth;
            set casuser._fill_monthly_all;
        run;

        libname _fillout clear;

        %put NOTE: [fillrate_run] Tablas .sas7bdat guardadas en &_tables_path.;
        %put NOTE: [fillrate_run] &_tbl_prefix._gnrl;
        %put NOTE: [fillrate_run] &_tbl_prefix._mnth;
    %end;

    proc datasets library=casuser nolist nowarn;
        delete _fill_:;
    quit;

    proc datasets library=work nolist nowarn;
        delete _fill_:;
    quit;

    %put NOTE:======================================================;
    %put NOTE: [fillrate_run] FIN - &_file_prefix. (mode=&fill_mode.);
    %put NOTE:======================================================;

%mend fillrate_run;
