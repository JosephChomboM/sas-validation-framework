/* =========================================================================
gini_report.sas - Reportes HTML + Excel + JPEG para Gini
========================================================================= */
%macro _gini_report_style(col=, fmt=);
    compute &col.;
        if not missing(strip(put(&col., &fmt..))) then
            call define(_col_, "style",
                cats("style=[backgroundcolor=", strip(put(&col., &fmt..)),
                "]"));
    endcomp;
%mend _gini_report_style;

%macro _gini_report(report_path=, images_path=, file_prefix=, byvar=,
    model_low=, model_high=, var_low=, var_high=, delta_warn=, top_n=10,
    table_lib=casuser, table_prefix=);

    %local _delta_low _tbl_mdlg _tbl_mdlm _tbl_varg _tbl_vcmp _tbl_vsum
        _tbl_vdet;
    %let _delta_low=%sysevalf(-1 * &delta_warn.);

    %if %length(%superq(table_prefix)) > 0 %then %do;
        %let _tbl_mdlg=&table_lib..&table_prefix._mdlg;
        %let _tbl_mdlm=&table_lib..&table_prefix._mdlm;
        %let _tbl_varg=&table_lib..&table_prefix._varg;
        %let _tbl_vcmp=&table_lib..&table_prefix._vcmp;
        %let _tbl_vsum=&table_lib..&table_prefix._vsum;
        %let _tbl_vdet=&table_lib..&table_prefix._vdet;
    %end;
    %else %do;
        %let _tbl_mdlg=&table_lib.._gini_model_general;
        %let _tbl_mdlm=&table_lib.._gini_model_monthly;
        %let _tbl_varg=&table_lib.._gini_vars_general;
        %let _tbl_vcmp=&table_lib.._gini_vars_compare;
        %let _tbl_vsum=&table_lib.._gini_vars_summary;
        %let _tbl_vdet=&table_lib.._gini_vars_detail;
    %end;

    proc format;
        value GiniModelFmt
            low-<&model_low.='lightred'
            &model_low.-<&model_high.='lightyellow'
            &model_high.-high='lightgreen';
        value GiniVarFmt
            low-<&var_low.='lightred'
            &var_low.-<&var_high.='lightyellow'
            &var_high.-high='lightgreen';
        value DeltaFmt
            low-<&_delta_low.='lightred'
            &_delta_low.-<&delta_warn.='lightyellow'
            &delta_warn.-high='lightgreen';
        value $TrendFmt
            'EMPEORANDO'='lightred'
            'DEGRADACION'='lightred'
            'ACEPTABLE'='lightyellow'
            'ESTABLE'='lightyellow'
            'MEJORA'='lightgreen'
            'MEJORANDO'='lightgreen'
            'SATISFACTORIO'='lightgreen'
            'BAJO'='lightred'
            'SIN DATOS'='lightgray'
            'MIN DATOS'='lightgray';
    run;

    ods graphics on;
    ods listing gpath="&images_path.";
    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="MODEL_GENERAL" sheet_interval="none"
        embedded_titles="yes" frozen_headers="yes" autofilter="all");

    title "GINI del Modelo - Resumen Global";
    proc report data=&_tbl_mdlg. nowd missing;
        columns Split N_Total N_Default N_No_Default Tasa_Default N_Gini Gini
            IC_95_Lower IC_95_Upper Degradacion Evaluacion;
        define Split / display "Dataset";
        define N_Total / display "N Total";
        define N_Default / display "N Default";
        define N_No_Default / display "N No Default";
        define Tasa_Default / display "Tasa Default" format=percent8.2;
        define N_Gini / display "N Gini";
        define Gini / display "GINI" format=8.4;
        define IC_95_Lower / display "IC 95% Inf" format=8.4;
        define IC_95_Upper / display "IC 95% Sup" format=8.4;
        define Degradacion / display "Degradacion" format=percent8.2;
        define Evaluacion / display "Evaluacion";
        %_gini_report_style(col=Gini, fmt=GiniModelFmt)
        %_gini_report_style(col=Evaluacion, fmt=$TrendFmt)
    run;
    title;

    ods excel options(sheet_name="MODEL_MONTHLY" sheet_interval="now");
    title "GINI del Modelo por Periodo";
    proc report data=&_tbl_mdlm. nowd missing;
        columns Periodo Split N_Total N_Default
            Tasa_Default N_Gini Gini Delta_Gini Tendencia Evaluacion;
        define Periodo / display "Periodo" format=6.;
        define Split / display "Dataset";
        define N_Total / display "N Total";
        define N_Default / display "N Default";
        define Tasa_Default / display "Tasa Default" format=percent8.2;
        define N_Gini / display "N Gini";
        define Gini / display "GINI" format=8.4;
        define Delta_Gini / display "Delta Gini" format=8.4;
        define Tendencia / display "Tendencia";
        define Evaluacion / display "Evaluacion";
        %_gini_report_style(col=Gini, fmt=GiniModelFmt)
        %_gini_report_style(col=Delta_Gini, fmt=DeltaFmt)
        %_gini_report_style(col=Tendencia, fmt=$TrendFmt)
        %_gini_report_style(col=Evaluacion, fmt=$TrendFmt)
    run;
    title;

    ods excel options(sheet_name="VARS_GENERAL" sheet_interval="now");
    title "GINI por Variable";
    proc report data=&_tbl_varg. nowd missing;
        columns Variable Split N_Total N_Valid Pct_Valid
            N_Default N_Gini Gini Evaluacion;
        define Variable / display "Variable";
        define Split / display "Dataset";
        define N_Total / display "N Total";
        define N_Valid / display "N Validos";
        define Pct_Valid / display "% Validos" format=percent8.2;
        define N_Default / display "N Default";
        define N_Gini / display "N Gini";
        define Gini / display "GINI" format=8.4;
        define Evaluacion / display "Evaluacion";
        %_gini_report_style(col=Gini, fmt=GiniVarFmt)
        %_gini_report_style(col=Evaluacion, fmt=$TrendFmt)
    run;
    title;

    ods excel options(sheet_name="VARS_COMPARE" sheet_interval="now");
    title "GINI Variables - Comparativo TRAIN vs OOT";
    proc report data=&_tbl_vcmp. nowd missing;
        columns Variable Gini_Train Gini_OOT Delta_Gini
            Estabilidad;
        define Variable / display "Variable";
        define Gini_Train / display "GINI Train" format=8.4;
        define Gini_OOT / display "GINI OOT" format=8.4;
        define Delta_Gini / display "Delta GINI" format=8.4;
        define Estabilidad / display "Estabilidad";
        %_gini_report_style(col=Gini_Train, fmt=GiniVarFmt)
        %_gini_report_style(col=Gini_OOT, fmt=GiniVarFmt)
        %_gini_report_style(col=Delta_Gini, fmt=DeltaFmt)
        %_gini_report_style(col=Estabilidad, fmt=$TrendFmt)
    run;
    title;

    ods excel options(sheet_name="VARS_SUMMARY" sheet_interval="now");
    title "Resumen GINI Variables";
    proc report data=&_tbl_vsum. nowd missing;
        columns Variable Split N_Periodos First_Period
            Last_Period Gini_First Gini_Last Gini_Promedio Gini_Min Gini_Max
            Gini_Std Delta_Gini Tendencia Evaluacion;
        define Variable / display "Variable";
        define Split / display "Dataset";
        define N_Periodos / display "N Periodos";
        define First_Period / display "Primer Periodo" format=6.;
        define Last_Period / display "Ultimo Periodo" format=6.;
        define Gini_First / display "GINI Inicial" format=8.4;
        define Gini_Last / display "GINI Final" format=8.4;
        define Gini_Promedio / display "GINI Promedio" format=8.4;
        define Gini_Min / display "GINI Min" format=8.4;
        define Gini_Max / display "GINI Max" format=8.4;
        define Gini_Std / display "GINI Std" format=8.4;
        define Delta_Gini / display "Delta GINI" format=8.4;
        define Tendencia / display "Tendencia";
        define Evaluacion / display "Evaluacion";
        %_gini_report_style(col=Gini_First, fmt=GiniVarFmt)
        %_gini_report_style(col=Gini_Last, fmt=GiniVarFmt)
        %_gini_report_style(col=Gini_Promedio, fmt=GiniVarFmt)
        %_gini_report_style(col=Delta_Gini, fmt=DeltaFmt)
        %_gini_report_style(col=Tendencia, fmt=$TrendFmt)
        %_gini_report_style(col=Evaluacion, fmt=$TrendFmt)
    run;
    title;

    ods excel options(sheet_name="VARS_DETAIL" sheet_interval="now");
    title "Cubo GINI Variables - Detalle por Periodo";
    proc report data=&_tbl_vdet. nowd missing;
        columns Variable Split Periodo
            N_Total N_Valid N_Default N_Gini Gini Evaluacion;
        define Variable / display "Variable";
        define Split / display "Dataset";
        define Periodo / display "Periodo" format=6.;
        define N_Total / display "N Total";
        define N_Valid / display "N Validos";
        define N_Default / display "N Default";
        define N_Gini / display "N Gini";
        define Gini / display "GINI" format=8.4;
        define Evaluacion / display "Evaluacion";
        %_gini_report_style(col=Gini, fmt=GiniVarFmt)
        %_gini_report_style(col=Evaluacion, fmt=$TrendFmt)
    run;
    title;

    ods excel options(sheet_name="PLOTS" sheet_interval="now");

    ods graphics / imagename="&file_prefix._mdl_trn" imagefmt=jpeg;
    %_gini_plot_model_trend(data=&_tbl_mdlm., split=TRAIN,
        model_low=&model_low., model_high=&model_high.);
    ods graphics / reset=all;

    ods graphics / imagename="&file_prefix._mdl_oot" imagefmt=jpeg;
    %_gini_plot_model_trend(data=&_tbl_mdlm., split=OOT,
        model_low=&model_low., model_high=&model_high.);
    ods graphics / reset=all;

    ods graphics / imagename="&file_prefix._mdl_cmp" imagefmt=jpeg;
    %_gini_plot_model_compare(data=&_tbl_mdlm.,
        model_low=&model_low., model_high=&model_high.);
    ods graphics / reset=all;

    ods graphics / imagename="&file_prefix._rank_trn" imagefmt=jpeg;
    %_gini_plot_var_ranking(data=&_tbl_vsum., split=TRAIN,
        top_n=&top_n., var_low=&var_low., var_high=&var_high.);
    ods graphics / reset=all;

    ods graphics / imagename="&file_prefix._rank_oot" imagefmt=jpeg;
    %_gini_plot_var_ranking(data=&_tbl_vsum., split=OOT,
        top_n=&top_n., var_low=&var_low., var_high=&var_high.);
    ods graphics / reset=all;

    ods graphics / imagename="&file_prefix._vars_trn" imagefmt=jpeg;
    %_gini_plot_var_trends(detail=&_tbl_vdet.,
        summary=&_tbl_vsum., split=TRAIN, top_n=&top_n.,
        var_low=&var_low., var_high=&var_high.);
    ods graphics / reset=all;

    ods graphics / imagename="&file_prefix._vars_oot" imagefmt=jpeg;
    %_gini_plot_var_trends(detail=&_tbl_vdet.,
        summary=&_tbl_vsum., split=OOT, top_n=&top_n.,
        var_low=&var_low., var_high=&var_high.);
    ods graphics / reset=all;

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

%mend _gini_report;
