/* =========================================================================
missings_run.sas - API publica del modulo Missings (Metodo 4.2)

Patron:
- recibe _scope_input como flujo principal
- deriva TRAIN/OOT internamente
- ejecuta contract
- delega compute/report a macros internas
========================================================================= */
%include "&fw_root./src/modules/missings/missings_contract.sas";
%include "&fw_root./src/modules/missings/impl/missings_compute.sas";
%include "&fw_root./src/modules/missings/impl/missings_report.sas";

%macro missings_run(input_caslib=PROC, input_table=_scope_input, train_table=,
    oot_table=, output_caslib=OUT, troncal_id=, scope=, run_id=);

    %global _miss_rc;
    %let _miss_rc=0;

    %local _miss_vars_num _miss_vars_cat _miss_threshold _report_path
        _file_prefix _scope_abbr _miss_is_custom _seg_num _miss_byvar
        _miss_train_min _miss_train_max _miss_oot_min _miss_oot_max
        _miss_input_exists _miss_train_exists _miss_oot_exists
        _miss_source_table _miss_source_caslib _miss_split_mode _dir_rc;

    %put NOTE:======================================================;
    %put NOTE: [missings_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: input=&input_caslib..&input_table.;
    %put NOTE: train_legacy=&input_caslib..&train_table.;
    %put NOTE: oot_legacy=&input_caslib..&oot_table.;
    %put NOTE:======================================================;

    %let _miss_vars_num=;
    %let _miss_vars_cat=;
    %let _miss_is_custom=0;
    %let _miss_source_table=;
    %let _miss_source_caslib=&input_caslib.;
    %let _miss_split_mode=;

    %if %length(%superq(miss_threshold)) > 0 %then
        %let _miss_threshold=&miss_threshold.;
    %else %let _miss_threshold=0.1;

    proc sql noprint;
        select strip(byvar),
               strip(put(train_min_mes, best.)),
               strip(put(train_max_mes, best.)),
               strip(put(oot_min_mes, best.)),
               strip(put(oot_max_mes, best.))
          into :_miss_byvar trimmed,
               :_miss_train_min trimmed,
               :_miss_train_max trimmed,
               :_miss_oot_min trimmed,
               :_miss_oot_max trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;
    quit;

    %if %upcase(&miss_mode.)=CUSTOM %then %do;
        %if %length(%superq(miss_custom_vars_num)) > 0 or
            %length(%superq(miss_custom_vars_cat)) > 0 %then %do;
            %let _miss_vars_num=&miss_custom_vars_num.;
            %let _miss_vars_cat=&miss_custom_vars_cat.;
            %let _miss_is_custom=1;
            %put NOTE: [missings_run] Modo CUSTOM activado.;
        %end;
        %else %do;
            %put WARNING: [missings_run] miss_mode=CUSTOM pero sin variables
                custom. Fallback a AUTO.;
        %end;
    %end;

    %if &_miss_is_custom.=0 %then %do;
        %if %substr(&scope., 1, 3)=seg %then %do;
            %let _seg_num=%sysfunc(inputn(%substr(&scope., 4), best.));

            proc sql noprint;
                select strip(num_list) into :_miss_vars_num trimmed
                from casuser.cfg_segmentos
                where troncal_id=&troncal_id.
                  and seg_id=&_seg_num.;

                select strip(cat_list) into :_miss_vars_cat trimmed
                from casuser.cfg_segmentos
                where troncal_id=&troncal_id.
                  and seg_id=&_seg_num.;
            quit;
        %end;

        %if %length(%superq(_miss_vars_num))=0 %then %do;
            proc sql noprint;
                select strip(num_unv) into :_miss_vars_num trimmed
                from casuser.cfg_troncales
                where troncal_id=&troncal_id.;
            quit;
        %end;

        %if %length(%superq(_miss_vars_cat))=0 %then %do;
            proc sql noprint;
                select strip(cat_unv) into :_miss_vars_cat trimmed
                from casuser.cfg_troncales
                where troncal_id=&troncal_id.;
            quit;
        %end;
    %end;

    %put NOTE: [missings_run] Variables resueltas:;
    %put NOTE: [missings_run] num=&_miss_vars_num.;
    %put NOTE: [missings_run] cat=&_miss_vars_cat.;
    %put NOTE: [missings_run] byvar=&_miss_byvar.;
    %put NOTE: [missings_run] TRAIN=&_miss_train_min.-&_miss_train_max.;
    %put NOTE: [missings_run] OOT=&_miss_oot_min.-&_miss_oot_max.;

    %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
    %else %let _scope_abbr=base;

    %if &_miss_is_custom.=1 %then %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _file_prefix=custom_miss_troncal_&troncal_id._&_scope_abbr.;
    %end;
    %else %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./reports/METOD4.2;
        %let _file_prefix=miss_troncal_&troncal_id._&_scope_abbr.;
        %let _dir_rc=%sysfunc(dcreate(METOD4.2,
            &fw_root./outputs/runs/&run_id./reports));
    %end;

    %let _miss_input_exists=0;
    %let _miss_train_exists=0;
    %let _miss_oot_exists=0;

    %if %length(%superq(input_table)) > 0 %then %do;
        proc sql noprint;
            select count(*) into :_miss_input_exists trimmed
            from dictionary.tables
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&input_table.");
        quit;
    %end;

    %if %length(%superq(train_table)) > 0 %then %do;
        proc sql noprint;
            select count(*) into :_miss_train_exists trimmed
            from dictionary.tables
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&train_table.");
        quit;
    %end;

    %if %length(%superq(oot_table)) > 0 %then %do;
        proc sql noprint;
            select count(*) into :_miss_oot_exists trimmed
            from dictionary.tables
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&oot_table.");
        quit;
    %end;

    proc cas;
        session conn;
        table.dropTable / caslib="casuser" name="_miss_input_legacy"
            quiet=true;
    quit;

    %if &_miss_input_exists.=1 %then %do;
        %let _miss_source_table=&input_table.;
        %let _miss_split_mode=DERIVED;
    %end;
    %else %if &_miss_train_exists.=1 and &_miss_oot_exists.=1 %then %do;
        %let _miss_source_table=_miss_input_legacy;
        %let _miss_source_caslib=casuser;
        %let _miss_split_mode=PRELABELED;

        proc fedsql sessref=conn;
            create table casuser._miss_input_legacy {options replace=true} as
            select cast('TRAIN' as varchar(16)) as Split, a.*
            from &input_caslib..&train_table. a
            union all
            select cast('OOT' as varchar(16)) as Split, a.*
            from &input_caslib..&oot_table. a;
        quit;
    %end;
    %else %do;
        %put ERROR: [missings_run] No hay input valido para ejecutar el modulo.;
        %let _miss_rc=1;
        %return;
    %end;

    %missings_contract(input_caslib=&_miss_source_caslib.,
        input_table=&_miss_source_table., split_mode=&_miss_split_mode.,
        split_var=Split, byvar=&_miss_byvar.,
        train_min_mes=&_miss_train_min., train_max_mes=&_miss_train_max.,
        oot_min_mes=&_miss_oot_min., oot_max_mes=&_miss_oot_max.,
        vars_num=&_miss_vars_num., vars_cat=&_miss_vars_cat.);

    %if &_miss_rc. ne 0 %then %do;
        %put ERROR: [missings_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    %_missings_report(input_caslib=&_miss_source_caslib.,
        input_table=&_miss_source_table., split_mode=&_miss_split_mode.,
        split_var=Split, byvar=&_miss_byvar.,
        train_min_mes=&_miss_train_min., train_max_mes=&_miss_train_max.,
        oot_min_mes=&_miss_oot_min., oot_max_mes=&_miss_oot_max.,
        vars_num=&_miss_vars_num., vars_cat=&_miss_vars_cat.,
        threshold=&_miss_threshold., report_path=&_report_path.,
        file_prefix=&_file_prefix.);

    proc datasets library=casuser nolist nowarn;
        delete _miss_input_legacy;
    quit;

    %put NOTE:======================================================;
    %put NOTE: [missings_run] FIN - &_file_prefix. (mode=&miss_mode.);
    %put NOTE:======================================================;

%mend missings_run;
