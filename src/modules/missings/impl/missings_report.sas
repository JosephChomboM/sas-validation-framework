/* =========================================================================
missings_report.sas - Generacion de reportes HTML + Excel para Missings

Genera:
<report_path>/<prefix>.html   - HTML consolidado TRAIN/OOT
<report_path>/<prefix>.xlsx   - Excel consolidado (Resumen + Detalle)

Tablas temporales se crean en casuser (CAS) via PROC FEDSQL.
========================================================================= */

%macro _miss_prepare_scope_data(input_caslib=, input_table=, byvar=,
    train_min_mes=, train_max_mes=, oot_min_mes=, oot_max_mes=,
    out_table=_miss_input, split_var=_miss_split);

    proc cas;
        session conn;
        table.dropTable / caslib="casuser" name="&out_table." quiet=true;
        table.dropTable / caslib="casuser" name="_miss_input_stage" quiet=true;
    quit;

    proc fedsql sessref=conn;
        create table casuser.&out_table. {options replace=true} as
        select 'TRAIN' as &split_var., a.*
        from &input_caslib..&input_table. a
        where a.&byvar. >= &train_min_mes.
          and a.&byvar. <= &train_max_mes.;
    quit;

    proc fedsql sessref=conn;
        create table casuser._miss_input_stage {options replace=true} as
        select 'OOT' as &split_var., a.*
        from &input_caslib..&input_table. a
        where a.&byvar. >= &oot_min_mes.
          and a.&byvar. <= &oot_max_mes.;
    quit;

    proc cas;
        session conn;
        table.append /
            source={caslib="casuser", name="_miss_input_stage"},
            target={caslib="casuser", name="&out_table."};
        table.dropTable / caslib="casuser" name="_miss_input_stage" quiet=true;
    quit;

%mend _miss_prepare_scope_data;

%macro _missings_report(input_caslib=, input_table=, byvar=, train_min_mes=,
    train_max_mes=, oot_min_mes=, oot_max_mes=, vars_num=, vars_cat=,
    threshold=, report_path=, file_prefix=);

    %local _dir_rc;

    %put NOTE: [missings_report] Generando reportes...;
    %put NOTE: [missings_report] input=&input_caslib..&input_table.;
    %put NOTE: [missings_report] byvar=&byvar.;
    %put NOTE: [missings_report] report_path=&report_path.;
    %put NOTE: [missings_report] file_prefix=&file_prefix.;
    %put NOTE: [missings_report] threshold=&threshold.;

    /* Para AUTO, asegurar subcarpeta METOD4.2 bajo reports */
    %if %index(%upcase(&report_path.), METOD4.2) > 0 %then %do;
        %let _dir_rc=%sysfunc(dcreate(METOD4.2, &report_path./../));
    %end;

    %_miss_prepare_scope_data(input_caslib=&input_caslib.,
        input_table=&input_table., byvar=&byvar.,
        train_min_mes=&train_min_mes., train_max_mes=&train_max_mes.,
        oot_min_mes=&oot_min_mes., oot_max_mes=&oot_max_mes.,
        out_table=_miss_input, split_var=_miss_split);

    %_miss_compute(data=casuser._miss_input, split_var=_miss_split,
        vars_num=&vars_num., vars_cat=&vars_cat., detail_table=_miss_detail,
        summary_table=_miss_summary);

    /* Sort solo al final (presentacion) */
    %_miss_sort_cas(table_name=_miss_summary,
        orderby=%str({"split", "variable"}));
    %_miss_sort_cas(table_name=_miss_detail,
        orderby=%str({"split", "variable", "type", "dummy_value"}));

    proc format;
        value MissSignif low-<&threshold.='white' &threshold.-high='red';
    run;

    ods graphics on;

    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="Resumen" sheet_interval="none"
        embedded_titles="yes");

    title "Analisis de Missings - Resumen por Variable";
    proc print data=casuser._miss_summary noobs
        style(column)={backgroundcolor=MissSignif.};
        var split variable type nmiss pct_miss;
        format pct_miss percent8.2;
    run;

    ods excel options(sheet_name="Detalle" sheet_interval="now"
        embedded_titles="yes");
    title "Analisis de Missings - Detalle por Dummy";
    proc print data=casuser._miss_detail noobs;
        var split variable type dummy_value nmiss pct_miss;
        format pct_miss percent8.2;
    run;

    title;
    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    /* Cleanup tablas temporales CAS ------------------------------------- */
    proc datasets library=casuser nolist nowarn;
        delete _miss_input _miss_input_stage _miss_stage _miss_detail
            _miss_summary;
    quit;

    %put NOTE: [missings_report] HTML=>
        &report_path./&file_prefix..html;
    %put NOTE: [missings_report] Excel=> &report_path./&file_prefix..xlsx;

%mend _missings_report;
