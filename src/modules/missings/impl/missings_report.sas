/* =========================================================================
missings_report.sas - Reportes legacy sobre compute unificado CAS-first

Genera:
- <report_path>/<prefix>.html
- <report_path>/<prefix>.xlsx

El HTML consolidado presenta TRAIN y OOT en secciones separadas.
No persiste tablas en tables/.
========================================================================= */

%macro _miss_prepare_scope_data(input_caslib=, input_table=, split_mode=DERIVED,
    split_var=Split, byvar=, train_min_mes=, train_max_mes=, oot_min_mes=,
    oot_max_mes=, out_table=_miss_input);

    proc datasets library=casuser nolist nowarn;
        delete &out_table.;
    quit;

    %if %upcase(&split_mode.)=DERIVED %then %do;
        proc fedsql sessref=conn;
            create table casuser.&out_table. {options replace=true} as
            select 'TRAIN' as Split, *
            from &input_caslib..&input_table.
            where &byvar. >= &train_min_mes.
              and &byvar. <= &train_max_mes.
            union all
            select 'OOT' as Split, *
            from &input_caslib..&input_table.
            where &byvar. >= &oot_min_mes.
              and &byvar. <= &oot_max_mes.;
        quit;
    %end;
    %else %do;
        proc fedsql sessref=conn;
            create table casuser.&out_table. {options replace=true} as
            select *
            from &input_caslib..&input_table.
            where upcase(&split_var.) in ('TRAIN', 'OOT');
        quit;
    %end;

%mend _miss_prepare_scope_data;

%macro _miss_render_variable_reports(detail_data=, vars=);

    %local _i _var _has_rows;

    %let _i=1;
    %let _var=%scan(%superq(vars), &_i., %str( ));
    %do %while(%length(%superq(_var)) > 0);
        proc sql noprint;
            select count(*)
            into :_has_rows trimmed
            from &detail_data.
            where Variable="&_var.";
        quit;

        %if %sysevalf(%superq(_has_rows)=, boolean) %then %let _has_rows=0;

        %if &_has_rows. > 0 %then %do;
            proc print data=&detail_data.(where=(Variable="&_var.")) label noobs;
                var Dummy_Value Type Total_N NMiss Pct_Miss;
                label Dummy_Value="&_var."
                      Total_N='Total';
                format Pct_Miss percent8.2;
            run;
        %end;

        %let _i=%eval(&_i. + 1);
        %let _var=%scan(%superq(vars), &_i., %str( ));
    %end;

%mend _miss_render_variable_reports;

%macro _miss_render_split(detail_data=, summary_data=, split_label=, vars_num=,
    vars_cat=);

    title "&split_label.: Analisis de Missings";
    title2 "Missing summarize (variable/cases)";
    %_miss_render_variable_reports(detail_data=&detail_data., vars=&vars_num.);
    %_miss_render_variable_reports(detail_data=&detail_data., vars=&vars_cat.);

    title2 "Missing summarize (variables)";
    proc print data=&summary_data. noobs
        style(column)={backgroundcolor=MissSignif.};
        var Variable Type Total_Pct_Missing;
        format Total_Pct_Missing percent8.2;
    run;

    title;
    title2;

%mend _miss_render_split;

%macro _missings_report(input_caslib=, input_table=, split_mode=DERIVED,
    split_var=Split, byvar=, train_min_mes=, train_max_mes=, oot_min_mes=,
    oot_max_mes=, vars_num=, vars_cat=, threshold=, report_path=,
    file_prefix=);

    %local _dir_rc;

    %put NOTE: [missings_report] Generando reportes...;
    %put NOTE: [missings_report] input=&input_caslib..&input_table.;
    %put NOTE: [missings_report] split_mode=&split_mode.;
    %put NOTE: [missings_report] report_path=&report_path.;
    %put NOTE: [missings_report] file_prefix=&file_prefix.;
    %put NOTE: [missings_report] threshold=&threshold.;

    %if %index(%upcase(&report_path.), METOD4.2) > 0 %then %do;
        %let _dir_rc=%sysfunc(dcreate(METOD4.2, &report_path./../));
    %end;

    %_miss_prepare_scope_data(input_caslib=&input_caslib.,
        input_table=&input_table., split_mode=&split_mode.,
        split_var=&split_var., byvar=&byvar., train_min_mes=&train_min_mes.,
        train_max_mes=&train_max_mes., oot_min_mes=&oot_min_mes.,
        oot_max_mes=&oot_max_mes., out_table=_miss_input);

    %_miss_compute(data=casuser._miss_input, split_var=Split,
        vars_num=&vars_num., vars_cat=&vars_cat., detail_table=_miss_detail,
        summary_table=_miss_summary, var_catalog_table=_miss_var_catalog,
        split_totals_table=_miss_split_totals);

    proc fedsql sessref=conn;
        create table casuser._miss_train_detail_rpt {options replace=true} as
        select Variable, Dummy_Value, Type, Total_N, NMiss, Pct_Miss
        from casuser._miss_detail
        where Split='TRAIN';

        create table casuser._miss_oot_detail_rpt {options replace=true} as
        select Variable, Dummy_Value, Type, Total_N, NMiss, Pct_Miss
        from casuser._miss_detail
        where Split='OOT';

        create table casuser._miss_train_summary_rpt {options replace=true} as
        select Variable, Type, Pct_Miss as Total_Pct_Missing
        from casuser._miss_summary
        where Split='TRAIN';

        create table casuser._miss_oot_summary_rpt {options replace=true} as
        select Variable, Type, Pct_Miss as Total_Pct_Missing
        from casuser._miss_summary
        where Split='OOT';
    quit;

    %_miss_sort_cas(table_name=_miss_train_detail_rpt,
        orderby=%str({"Variable", "Dummy_Value"}));
    %_miss_sort_cas(table_name=_miss_oot_detail_rpt,
        orderby=%str({"Variable", "Dummy_Value"}));
    %_miss_sort_cas(table_name=_miss_train_summary_rpt,
        orderby=%str({"Variable"}));
    %_miss_sort_cas(table_name=_miss_oot_summary_rpt,
        orderby=%str({"Variable"}));

    proc format;
        value MissSignif low-<&threshold.='white' &threshold.-high='red';
    run;

    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    %_miss_render_split(detail_data=casuser._miss_train_detail_rpt,
        summary_data=casuser._miss_train_summary_rpt, split_label=TRAIN,
        vars_num=&vars_num., vars_cat=&vars_cat.);
    %_miss_render_split(detail_data=casuser._miss_oot_detail_rpt,
        summary_data=casuser._miss_oot_summary_rpt, split_label=OOT,
        vars_num=&vars_num., vars_cat=&vars_cat.);
    ods html5 close;

    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="TRAIN_Missings" sheet_interval="none"
        embedded_titles="yes");
    %_miss_render_split(detail_data=casuser._miss_train_detail_rpt,
        summary_data=casuser._miss_train_summary_rpt, split_label=TRAIN,
        vars_num=&vars_num., vars_cat=&vars_cat.);

    ods excel options(sheet_name="OOT_Missings" sheet_interval="now"
        embedded_titles="yes");
    %_miss_render_split(detail_data=casuser._miss_oot_detail_rpt,
        summary_data=casuser._miss_oot_summary_rpt, split_label=OOT,
        vars_num=&vars_num., vars_cat=&vars_cat.);

    ods excel close;

    proc datasets library=casuser nolist nowarn;
        delete _miss_input _miss_detail _miss_summary _miss_var_catalog
            _miss_split_totals _miss_detail_raw _miss_summary_stage
            _miss_train_detail_rpt _miss_oot_detail_rpt
            _miss_train_summary_rpt _miss_oot_summary_rpt;
    quit;

    %put NOTE: [missings_report] HTML=> &report_path./&file_prefix..html;
    %put NOTE: [missings_report] Excel=> &report_path./&file_prefix..xlsx;

%mend _missings_report;
