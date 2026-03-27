/* =========================================================================
fillrate_report.sas - Reportes para Fillrate vs Gini

Genera:
- <report_path>/<prefix>.html
- <report_path>/<prefix>.xlsx
- <images_path>/<prefix>_*.jpeg

Tambien deja en casuser:
- _fill_general_all
- _fill_monthly_all

Entrada esperada:
- input_table consolidada con columna Muestra (TRAIN/OOT)
========================================================================= */
%macro _fillrate_report(input_caslib=, input_table=, byvar=,
    target=, def_cld=, oot_max_mes=, vars_num=, vars_cat=, report_path=,
    images_path=, file_prefix=);

    %local _has_general _start_sheet;

    %put NOTE: [fillrate_report] Generando reportes...;
    %put NOTE: [fillrate_report] byvar=&byvar. target=&target.
        def_cld=&def_cld.;
    %put NOTE: [fillrate_report] oot_max_mes=&oot_max_mes.;

    proc fedsql sessref=conn;
        create table casuser._fill_train_gini {options replace=true} as
            select * from &input_caslib..&input_table.
            where upcase(Muestra)='TRAIN'
              and &byvar. <= &def_cld.;
    quit;

    proc fedsql sessref=conn;
        create table casuser._fill_oot_gini {options replace=true} as
            select * from &input_caslib..&input_table.
            where upcase(Muestra)='OOT'
              and &byvar. <= &def_cld.;
    quit;

    proc fedsql sessref=conn;
        create table casuser._fill_train_full {options replace=true} as
            select * from &input_caslib..&input_table.
            where upcase(Muestra)='TRAIN'
              and &byvar. <= &oot_max_mes.;
    quit;

    proc fedsql sessref=conn;
        create table casuser._fill_oot_full {options replace=true} as
            select * from &input_caslib..&input_table.
            where upcase(Muestra)='OOT'
              and &byvar. <= &oot_max_mes.;
    quit;

    %if %length(%superq(vars_num)) > 0 %then %do;
        %_fill_general_compute(data=casuser._fill_train_gini, vars_num=&vars_num.,
            target=&target., out=work._fill_general_train);
        %_fill_general_compute(data=casuser._fill_oot_gini, vars_num=&vars_num.,
            target=&target., out=work._fill_general_oot);
    %end;
    %else %do;
        data work._fill_general_train;
            length Variable $64 Var_Type $8 N_Total N_Filled N_Gini 8 Fillrate
                Gini Smdcr_Raw 8;
            format Fillrate 8.2 Gini 8.4 Smdcr_Raw 8.4;
            stop;
        run;
        data work._fill_general_oot;
            set work._fill_general_train(obs=0);
            stop;
        run;
    %end;

    %_fill_monthly_compute(data=casuser._fill_train_full, byvar=&byvar.,
        vars_num=&vars_num., vars_cat=&vars_cat., out=work._fill_monthly_train);
    %_fill_monthly_compute(data=casuser._fill_oot_full, byvar=&byvar.,
        vars_num=&vars_num., vars_cat=&vars_cat., out=work._fill_monthly_oot);

    data casuser._fill_general_all;
        length Muestra $5;
        set work._fill_general_train(in=_trn) work._fill_general_oot(in=_oot);
        if _trn then Muestra="TRAIN";
        else if _oot then Muestra="OOT";
    run;

    proc cas;
        session conn;
        table.partition /
            table={caslib="casuser", name="_fill_general_all",
                groupby={"Variable"}, orderby={"Muestra"}},
            casout={caslib="casuser", name="_fill_general_all", replace=true};
    quit;

    data casuser._fill_monthly_all;
        length Muestra $5;
        set work._fill_monthly_train(in=_trn) work._fill_monthly_oot(in=_oot);
        if _trn then Muestra="TRAIN";
        else if _oot then Muestra="OOT";
    run;

    proc cas;
        session conn;
        table.partition /
            table={caslib="casuser", name="_fill_monthly_all",
                groupby={"Variable"}, orderby={"&byvar.", "Muestra"}},
            casout={caslib="casuser", name="_fill_monthly_all", replace=true};
    quit;

    proc sql noprint;
        select count(*) into :_has_general trimmed from
            work._fill_general_train;
    quit;

    %if &_has_general. > 0 %then %let _start_sheet=Fillrate_Gini;
    %else %let _start_sheet=Fillrate_Completo;

    ods graphics on;
    ods listing gpath="&images_path.";
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="&_start_sheet." sheet_interval="none"
        embedded_titles="yes");

    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");

    %if &_has_general. > 0 %then %do;
        title "Fillrate vs Gini (default cerrado)";
        proc print data=casuser._fill_general_all noobs label;
            label Variable="Variable"
                Muestra="Muestra"
                Var_Type="Tipo"
                N_Total="N Total"
                N_Filled="N Filled"
                Fillrate="Fillrate (%)"
                N_Gini="N Gini"
                Smdcr_Raw="_SMDCR_"
                Gini="Gini";
        run;
        title;
        ods excel options(sheet_name="Fillrate_Completo" sheet_interval="now"
            embedded_titles="yes");
    %end;

    title "Fillrate completo hasta fecha maxima OOT";
    proc print data=casuser._fill_monthly_all noobs label;
        label Variable="Variable"
            Muestra="Muestra"
            Var_Type="Tipo"
            N_Total="N Total"
            N_Filled="N Filled"
            Fillrate="Fillrate (%)";
    run;
    %_fill_plot_monthly(data=casuser._fill_monthly_all, byvar=&byvar.,
        image_stub=&file_prefix._fill);
    title;

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    %put NOTE: [fillrate_report] HTML =>
        &report_path./&file_prefix..html;
    %put NOTE: [fillrate_report] Excel =>
        &report_path./&file_prefix..xlsx;

%mend _fillrate_report;
