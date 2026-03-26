/* =========================================================================
universe_report.sas - HTML + Excel + JPEG reporting for Universe

The module receives a single CAS scope table, derives TRAIN and OOT in CAS,
and reports them together in the same tables and charts through _univ_split.
======================================================================== */
%macro _univ_prepare_scope_data(input_caslib=, input_table=, byvar=,
    train_min_mes=, train_max_mes=, oot_min_mes=, oot_max_mes=,
    out_table=_univ_input, split_var=_univ_split);

    proc cas;
        session conn;
        table.dropTable / caslib="casuser" name="&out_table." quiet=true;
        table.dropTable / caslib="casuser" name="_univ_input_stage" quiet=true;
    quit;

    proc fedsql sessref=conn;
        create table casuser.&out_table. {options replace=true} as
        select 'TRAIN' as &split_var., a.*
        from &input_caslib..&input_table. a
        where a.&byvar. >= &train_min_mes.
          and a.&byvar. <= &train_max_mes.;
    quit;

    proc fedsql sessref=conn;
        create table casuser._univ_input_stage {options replace=true} as
        select 'OOT' as &split_var., a.*
        from &input_caslib..&input_table. a
        where a.&byvar. >= &oot_min_mes.
          and a.&byvar. <= &oot_max_mes.;
    quit;

    proc cas;
        session conn;
        table.append /
            source={caslib="casuser", name="_univ_input_stage"},
            target={caslib="casuser", name="&out_table."};
        table.dropTable / caslib="casuser" name="_univ_input_stage" quiet=true;
    quit;

%mend _univ_prepare_scope_data;

%macro _universe_report(input_caslib=, input_table=, byvar=, id_var=,
    monto_var=, train_min_mes=, train_max_mes=, oot_min_mes=, oot_max_mes=,
    report_path=, images_path=, file_prefix=);

    %local _dir_rc _has_monto;

    %put NOTE: [universe_report] Generando reportes...;
    %put NOTE: [universe_report] input=&input_caslib..&input_table.;
    %put NOTE: [universe_report] report_path=&report_path.;
    %put NOTE: [universe_report] images_path=&images_path.;
    %put NOTE: [universe_report] file_prefix=&file_prefix.;

    %let _dir_rc=%sysfunc(dcreate(METOD1.1, &report_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &report_path.));
    %let _dir_rc=%sysfunc(dcreate(METOD1.1, &images_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &images_path.));

    %let _has_monto=0;
    %if %length(%superq(monto_var)) > 0 %then %do;
        proc sql noprint;
            select count(*) into :_has_monto trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&input_table.")
              and upcase(name)=upcase("&monto_var.");
        quit;
    %end;

    %_univ_prepare_scope_data(input_caslib=&input_caslib.,
        input_table=&input_table., byvar=&byvar.,
        train_min_mes=&train_min_mes., train_max_mes=&train_max_mes.,
        oot_min_mes=&oot_min_mes., oot_max_mes=&oot_max_mes.,
        out_table=_univ_input, split_var=_univ_split);

    ods graphics on;
    ods listing gpath="&images_path.";
    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="DescribeUniverso" sheet_interval="none"
        embedded_titles="yes");

    ods graphics / imagename="&file_prefix._cuentas" imagefmt=jpeg;
    %_univ_describe_id(data=casuser._univ_input, split_var=_univ_split,
        byvar=&byvar., id_var=&id_var.);

    %if &_has_monto.=1 %then %do;
        ods graphics / imagename="&file_prefix._monto_mean" imagefmt=jpeg;
        %_univ_describe_monto(data=casuser._univ_input,
            split_var=_univ_split, monto_var=&monto_var., byvar=&byvar.);
    %end;

    ods graphics / imagename="&file_prefix._bandas" imagefmt=jpeg;
    %_univ_bandas_cuentas(data=casuser._univ_input, split_var=_univ_split,
        byvar=&byvar., id_var=&id_var.);

    %if &_has_monto.=1 %then %do;
        ods graphics / imagename="&file_prefix._monto_sum" imagefmt=jpeg;
        %_univ_evolutivo_monto(data=casuser._univ_input,
            split_var=_univ_split, monto_var=&monto_var., byvar=&byvar.);
    %end;

    ods html5 close;
    ods excel close;
    ods graphics / reset=all;
    ods graphics off;

    proc datasets library=casuser nolist nowarn;
        delete _univ_input;
    quit;

    %put NOTE: [universe_report] HTML=>
        &report_path./&file_prefix..html;
    %put NOTE: [universe_report] Excel=> &report_path./&file_prefix..xlsx;
    %put NOTE: [universe_report] Images=> &images_path./;

%mend _universe_report;
