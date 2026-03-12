/* =========================================================================
fillrate_report.sas - Reportes para Fillrate vs Gini

Genera:
- <report_path>/<prefix>_train.html
- <report_path>/<prefix>_oot.html
- <report_path>/<prefix>.xlsx
- <images_path>/<prefix>_*.jpeg

Tambien deja en casuser:
- _fill_general_all
- _fill_monthly_all
========================================================================= */
%macro _fillrate_report(input_caslib=, train_table=, oot_table=, byvar=,
    target=, def_cld=, vars_num=, vars_cat=, report_path=, images_path=,
    file_prefix=);

    %local _has_trn_general _has_oot_general _start_sheet;

    %put NOTE: [fillrate_report] Generando reportes...;
    %put NOTE: [fillrate_report] byvar=&byvar. target=&target.
        def_cld=&def_cld.;

    proc fedsql sessref=conn;
        create table casuser._fill_train {options replace=true} as
            select * from &input_caslib..&train_table.
            where &byvar. <= &def_cld.;
    quit;

    proc fedsql sessref=conn;
        create table casuser._fill_oot {options replace=true} as
            select * from &input_caslib..&oot_table.
            where &byvar. <= &def_cld.;
    quit;

    %if %length(%superq(vars_num)) > 0 %then %do;
        %_fill_general_compute(data=casuser._fill_train, vars_num=&vars_num.,
            target=&target., out=work._fill_general_train);
        %_fill_general_compute(data=casuser._fill_oot, vars_num=&vars_num.,
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

    %_fill_monthly_compute(data=casuser._fill_train, byvar=&byvar.,
        vars_num=&vars_num., vars_cat=&vars_cat., out=work._fill_monthly_train);
    %_fill_monthly_compute(data=casuser._fill_oot, byvar=&byvar.,
        vars_num=&vars_num., vars_cat=&vars_cat., out=work._fill_monthly_oot);

    data casuser._fill_general_all;
        length Split $5;
        set work._fill_general_train(in=_trn) work._fill_general_oot(in=_oot);
        if _trn then Split="TRAIN";
        else if _oot then Split="OOT";
    run;

    data casuser._fill_monthly_all;
        length Split $5;
        set work._fill_monthly_train(in=_trn) work._fill_monthly_oot(in=_oot);
        if _trn then Split="TRAIN";
        else if _oot then Split="OOT";
    run;

    proc sql noprint;
        select count(*) into :_has_trn_general trimmed from
            work._fill_general_train;
        select count(*) into :_has_oot_general trimmed from
            work._fill_general_oot;
    quit;

    %if &_has_trn_general. > 0 %then %let _start_sheet=TRAIN_General;
    %else %let _start_sheet=TRAIN_Monthly;

    ods graphics on;
    ods listing gpath="&images_path.";
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="&_start_sheet." sheet_interval="none"
        embedded_titles="yes");

    /* ==================================================================
    TRAIN
    ================================================================== */
    ods html5 file="&report_path./&file_prefix._train.html"
        options(bitmap_mode="inline");

    %if &_has_trn_general. > 0 %then %do;
        title "TRAIN: Fillrate vs Gini";
        proc print data=work._fill_general_train noobs label;
            label Variable="Variable"
                Var_Type="Tipo"
                N_Total="N Total"
                N_Filled="N Filled"
                Fillrate="Fillrate (%)"
                N_Gini="N Gini"
                Smdcr_Raw="_SMDCR_"
                Gini="Gini";
        run;
        title;
        ods excel options(sheet_name="TRAIN_Monthly" sheet_interval="now"
            embedded_titles="yes");
    %end;

    title "TRAIN: Fillrate mensual";
    proc print data=work._fill_monthly_train noobs label;
        label Variable="Variable"
            Var_Type="Tipo"
            N_Total="N Total"
            N_Filled="N Filled"
            Fillrate="Fillrate (%)";
    run;
    %_fill_plot_monthly(data=work._fill_monthly_train, byvar=&byvar.,
        split_label=TRAIN, image_stub=&file_prefix._trn_fill);
    title;

    ods html5 close;
    ods graphics / reset=all;

    /* ==================================================================
    OOT
    ================================================================== */
    ods html5 file="&report_path./&file_prefix._oot.html"
        options(bitmap_mode="inline");

    %if &_has_oot_general. > 0 %then %do;
        ods excel options(sheet_name="OOT_General" sheet_interval="now"
            embedded_titles="yes");
        title "OOT: Fillrate vs Gini";
        proc print data=work._fill_general_oot noobs label;
            label Variable="Variable"
                Var_Type="Tipo"
                N_Total="N Total"
                N_Filled="N Filled"
                Fillrate="Fillrate (%)"
                N_Gini="N Gini"
                Smdcr_Raw="_SMDCR_"
                Gini="Gini";
        run;
        title;
    %end;

    ods excel options(sheet_name="OOT_Monthly" sheet_interval="now"
        embedded_titles="yes");
    title "OOT: Fillrate mensual";
    proc print data=work._fill_monthly_oot noobs label;
        label Variable="Variable"
            Var_Type="Tipo"
            N_Total="N Total"
            N_Filled="N Filled"
            Fillrate="Fillrate (%)";
    run;
    %_fill_plot_monthly(data=work._fill_monthly_oot, byvar=&byvar.,
        split_label=OOT, image_stub=&file_prefix._oot_fill);
    title;

    ods html5 close;
    ods excel close;
    ods graphics / reset=all;
    ods graphics off;

    %put NOTE: [fillrate_report] HTML TRAIN =>
        &report_path./&file_prefix._train.html;
    %put NOTE: [fillrate_report] HTML OOT =>
        &report_path./&file_prefix._oot.html;
    %put NOTE: [fillrate_report] Excel =>
        &report_path./&file_prefix..xlsx;

%mend _fillrate_report;
